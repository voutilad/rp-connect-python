package processor

import (
	"context"
	_ "embed"
	"errors"
	"github.com/voutilad/rp-connect-python/internal/impl/python"
	"runtime"
	"strings"
	"sync/atomic"
	"unsafe"

	"github.com/redpanda-data/benthos/v4/public/service"

	py "github.com/voutilad/gogopython"
)

const (
	// GlobalContent provides bytes from a Message.
	GlobalContent = "__content_callback"
	// GlobalMetadata provides the "metadata" function.
	GlobalMetadata = "__metadata_callback"
	// GlobalMessageAddr points to a service.Message
	GlobalMessageAddr = "__message_addr"
)

type mode string

const (
	MultiMode   mode = "multi"
	SingleMode  mode = "single"
	LegacyMode  mode = "legacy"
	InvalidMode mode = "invalid"
)

func stringAsMode(s string) mode {
	switch strings.ToLower(s) {
	case string(MultiMode):
		return MultiMode
	case string(SingleMode):
		return SingleMode
	case string(LegacyMode):
		return LegacyMode
	default:
		return InvalidMode
	}
}

type pythonProcessor struct {
	logger       *service.Logger
	runtime      python.Runtime
	interpreters map[int64]interpreter
	alive        atomic.Int32
}

type interpreter struct {
	// code is the compiled form of the Processor's Python script.
	code py.PyCodeObjectPtr

	// globalsHelper is the compiled form of globals.py.
	globalsHelper py.PyCodeObjectPtr

	// jsonHelper is the compiled form of serializer.py.
	jsonHelper py.PyCodeObjectPtr

	// globals is the Python globals we use for injecting state.
	globals py.PyObjectPtr

	// locals is the Python local state. Should be cleared after each message.
	locals py.PyObjectPtr

	// callbacks we've registered with the interpreter.
	callbacks []*callback
}

// Python helper for initializing a `content` function, returning bytes from
// the globals mapping.
//
//go:embed globals.py
var globalHelperSrc string

// Python helper for serializing the "result" of a processor.
//
//go:embed serializer.py
var jsonHelperSrc string

// Initialize the Python processor Redpanda Connect module.
//
// Python is not initialized here as it's too early to know details (e.g.
// path to the executable).
func init() {
	configSpec := service.
		NewConfigSpec().
		Summary("Process data with Python.").
		Field(service.NewStringField("script").
			Description("Python code to execute.")).
		Field(service.NewStringField("exe").
			Description("Path to a Python executable.").
			Default("python3")).
		Field(service.NewStringField("mode").
			Description("Toggle different Python runtime modes: 'multi', 'single', and 'legacy' (the default)").
			Default(string(LegacyMode)))
	// TODO: linting rules for configuration fields

	err := service.RegisterProcessor("python", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			// Extract our configuration.
			exe, err := conf.FieldString("exe")
			if err != nil {
				panic(err)
			}
			script, err := conf.FieldString("script")
			if err != nil {
				return nil, err
			}
			modeString, err := conf.FieldString("mode")
			if err != nil {
				return nil, err
			}

			return newPythonProcessor(exe, script, stringAsMode(modeString), mgr.Logger())
		})
	if err != nil {
		// There's no way to fail initialization. We must panic. :(
		panic(err)
	}
}

// newPythonProcessor creates new Python processor instance with the provided
// configuration.
//
// This will create and initialize a new sub-interpreter from the main Python
// Go routine and precompile some Python code objects.
func newPythonProcessor(exe, script string, mode mode, logger *service.Logger) (service.Processor, error) {
	var err error
	ctx := context.Background()

	// Spin up our runtime.
	var processor *pythonProcessor
	switch mode {
	case MultiMode:
		processor, err = newMultiRuntimeProcessor(exe, logger)
	case LegacyMode:
		processor, err = newLegacyRuntimeProcessor(exe, logger)
	case SingleMode:
		processor, err = newSingleRuntimeProcessor(exe, logger)
	default:
		return nil, errors.New("invalid mode")
	}
	if err != nil {
		return nil, err
	}

	// Start the runtime now to ferret out errors.
	err = processor.runtime.Start(ctx)
	if err != nil {
		return nil, err
	}

	// Initialize our sub-interpreter state.
	err = processor.runtime.Map(ctx, func(token *python.InterpreterTicket) error {
		// Pre-compile our script and helpers.
		code := py.Py_CompileString(script, "rp_connect_python.py", py.PyFileInput)
		if code == py.NullPyCodeObjectPtr {
			py.PyErr_Print()
			return errors.New("failed to compile python script")
		}

		// Pre-compile our script and helpers.
		globalsHelper := py.Py_CompileString(globalHelperSrc, "__globals_helper__.py", py.PyFileInput)
		if globalsHelper == py.NullPyCodeObjectPtr {
			py.PyErr_Print()
			return errors.New("failed to compile python globals helper script")
		}

		// Pre-compile our serialization helper.
		jsonHelper := py.Py_CompileString(jsonHelperSrc, "__json_helper__.py", py.PyFileInput)
		if jsonHelper == py.NullPyCodeObjectPtr {
			py.PyErr_Print()
			return errors.New("failed to compile python JSON helper script")
		}

		// Create our callback functions.
		metadata, err := newCallback(GlobalMetadata, metadataCallback)
		if err != nil {
			return err
		}
		content, err := newCallback(GlobalContent, contentCallback)
		if err != nil {
			return err
		}

		// Pre-populate globals.
		globals := py.PyDict_New()
		locals := py.PyDict_New()
		if py.PyDict_SetItemString(globals, GlobalMetadata, metadata.Object) != 0 {
			return errors.New("failed to set callback function in global mapping")
		}
		if py.PyDict_SetItemString(globals, GlobalContent, content.Object) != 0 {
			return errors.New("failed to set callback function in global mapping")
		}

		processor.interpreters[token.Id()] = interpreter{
			code:          code,
			globalsHelper: globalsHelper,
			jsonHelper:    jsonHelper,
			globals:       globals,
			locals:        locals,
			callbacks:     []*callback{metadata, content},
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return processor, nil
}

func newMultiRuntimeProcessor(exe string, logger *service.Logger) (*pythonProcessor, error) {
	cnt := runtime.NumCPU()
	r, err := python.NewMultiInterpreterRuntime(exe, cnt, false, logger)
	if err != nil {
		return nil, err
	}

	p := pythonProcessor{
		logger:       logger,
		runtime:      r,
		interpreters: make(map[int64]interpreter),
	}
	p.alive.Store(int32(cnt))
	return &p, nil
}

func newLegacyRuntimeProcessor(exe string, logger *service.Logger) (*pythonProcessor, error) {
	cnt := runtime.NumCPU()
	r, err := python.NewMultiInterpreterRuntime(exe, cnt, true, logger)
	if err != nil {
		return nil, err
	}

	p := pythonProcessor{
		logger:       logger,
		runtime:      r,
		interpreters: make(map[int64]interpreter),
	}
	p.alive.Store(int32(cnt))
	return &p, nil
}

func newSingleRuntimeProcessor(exe string, logger *service.Logger) (*pythonProcessor, error) {
	r, err := python.NewSingleInterpreterRuntime(exe, logger)
	if err != nil {
		return nil, err
	}

	p := pythonProcessor{
		logger:       logger,
		runtime:      r,
		interpreters: make(map[int64]interpreter),
	}
	p.alive.Store(1)
	return &p, nil
}

// Process a given Redpanda Connect service.Message using Python.
func (p *pythonProcessor) Process(ctx context.Context, m *service.Message) (service.MessageBatch, error) {
	// We currently don't drop on Python errors, but set an error on the message.
	batch := []*service.Message{m}

	// Acquire an interpreter and look up our local state.
	token, err := p.runtime.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = p.runtime.Release(token) }()

	// Look up our previously initialized interpreter state.
	i := p.interpreters[token.Id()]

	err = p.runtime.Apply(token, ctx, func() error {
		// Clear out any local state from previous messages.
		py.PyDict_Clear(i.locals)

		// Set up our pointer to our service.Message.
		addr := py.PyLong_FromUnsignedLong(uint64(uintptr(unsafe.Pointer(m))))
		if py.PyDict_SetItemString(i.globals, GlobalMessageAddr, addr) != 0 {
			return errors.New("failed to set address of message")
		}
		py.Py_DecRef(addr)

		// Pre-evaluate our global helpers.
		result := py.PyEval_EvalCode(i.globalsHelper, i.globals, i.locals)
		if result == py.NullPyObjectPtr {
			// If we get here, something horrible has occurred and we cannot recover.
			return errors.New("failed to evaluate global helper script")
		}
		py.Py_DecRef(result)

		// Evaluate the Python script that was pre-compiled into a code object.
		// It should have access to global helper functions/classes and should
		// set a local called "root".
		result = py.PyEval_EvalCode(i.code, i.globals, i.locals)
		if result == py.NullPyObjectPtr {
			py.PyErr_Print()
			return errors.New("problem executing Python script")
		}
		py.Py_DecRef(result)

		// The user script should have modified a local called "root".
		// Note: we don't call Py_DecRef as this is a borrowed reference.
		root := py.PyDict_GetItemString(i.locals, "root")
		if root == py.NullPyObjectPtr {
			return errors.New("'root' not found in Python script")
		}

		// Check our type and use an optimized conversion approach if possible.
		switch py.BaseType(root) {
		case py.None:
			// Drop the message.
			batch = []*service.Message{}
		case py.Set:
			// We can't serialize Sets to JSON. Warn and drop.
			return errors.New("cannot serialize a Python set")
		case py.Long:
			long := py.PyLong_AsLong(root)
			m.SetStructured(long)
		case py.String:
			str, err := py.UnicodeToString(root)
			if err != nil {
				return errors.New("unable to decode Python string")
			} else {
				// We use SetBytes instead of SetStructured to avoid
				// having our string wrapped in double-quotes.
				m.SetBytes([]byte(str))
			}
		case py.Bytes:
			// We need to copy-out the bytes into the message. We get a
			// pointer to the underlying data managed by Python.
			p := py.PyBytes_AsString(root)
			sz := py.PyBytes_Size(root)
			m.SetBytes(unsafe.Slice(p, sz))
		case py.Unknown:
			// We'll try serializing this to JSON. It could be a Root type.
			fallthrough
		case py.Tuple:
			fallthrough
		case py.List:
			fallthrough
		case py.Dict:
			// Convert to JSON for now with Python's help ;) because YOLO
			result = py.PyEval_EvalCode(i.jsonHelper, i.globals, i.locals)
			if result == py.NullPyObjectPtr {
				return errors.New("failed to JSON-ify root")
			}
			py.Py_DecRef(result)

			// The "result" should now be JSON as utf8 bytes. Get a weak
			// reference to the data.
			resultBytes := py.PyDict_GetItemString(i.locals, "result")
			if resultBytes == py.NullPyObjectPtr {
				return errors.New("failed to JSON-ify root: missing result")
			}

			// Before copying out, we need to know the length.
			sz := py.PyBytes_Size(resultBytes)
			rawBytes := py.PyBytes_AsString(resultBytes)

			// Copy the data out from Python land into Redpanda Connect.
			buffer := make([]byte, sz)
			copy(buffer, unsafe.Slice(rawBytes, sz))
			m.SetBytes(buffer)
		}
		return nil
	})

	if err != nil {
		m.SetError(err)
	}
	return batch, err
}

// Close a processor.
//
// If we're the last Python Processor, ask the main Go routine to stop the runtime.
func (p *pythonProcessor) Close(ctx context.Context) error {
	if p.alive.Add(-1) == 0 {
		p.logger.Debug("Stopping all sub-interpreters for processor")
		return p.runtime.Stop(ctx)
	}

	return nil
}
