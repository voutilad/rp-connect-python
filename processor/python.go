package processor

import (
	"context"
	"errors"
	"github.com/voutilad/rp-connect-python/internal/impl/python"
	"runtime"
	"sync/atomic"
	"unsafe"

	"github.com/redpanda-data/benthos/v4/public/service"

	py "github.com/voutilad/gogopython"
)

type pythonProcessor struct {
	logger       *service.Logger
	runtime      python.Runtime
	interpreters map[int64]interpreter
	alive        atomic.Int32
}

type interpreter struct {
	code          py.PyCodeObjectPtr
	globalsHelper py.PyCodeObjectPtr
	jsonHelper    py.PyCodeObjectPtr
}

// Python helper for initializing a `content` function, returning bytes from
// the globals mapping.
const globalHelperSrc = `
global content
def content():
	# Returns the content of the message being processed.
	global __content__
	return __content__
`

// Python helper for serializing the "result" of a processor.
const jsonHelperSrc = `
import json
try:
	if type(root) is str:
		result = root.encode()
	else:
		result = json.dumps(root).encode()
except:
	result = None
`

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
		Field(service.NewBoolField("legacy_mode").
			Description("Run in a less smp-friendly manner to support NumPy.").
			Default(false))

	err := service.RegisterProcessor("python", configSpec, constructor)
	if err != nil {
		// There's no way to fail initialization. We must panic. :(
		panic(err)
	}
}

// Construct a new Python processor instance.
//
// This will create and initialize a new sub-interpreter from the main Python
// Go routine and precompile some Python code objects.
func constructor(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	logger := mgr.Logger()
	ctx := context.Background()

	// Extract our configuration.
	exe, err := conf.FieldString("exe")
	if err != nil {
		panic(err)
	}
	script, err := conf.FieldString("script")
	if err != nil {
		return nil, err
	}
	legacyMode, err := conf.FieldBool("legacy_mode")
	if err != nil {
		return nil, err
	}
	cnt := runtime.NumCPU()

	// Spin up our runtime.
	r, err := python.NewMultiInterpreterRuntime(exe, cnt, legacyMode, mgr.Logger())
	//r, err := python.NewSingleInterpreterRuntime(exe, mgr.Logger())
	if err != nil {
		return nil, err
	}

	// Start the runtime now to ferret out errors.
	err = r.Start(ctx)
	if err != nil {
		return nil, err
	}

	// Initialize our sub-interpreters.
	interpreters := make(map[int64]interpreter)
	err = r.Map(ctx, func(token *python.InterpreterToken) error {
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

		// Pre-compile our script and helpers.
		jsonHelper := py.Py_CompileString(jsonHelperSrc, "__json_helper__.py", py.PyFileInput)
		if jsonHelper == py.NullPyCodeObjectPtr {
			py.PyErr_Print()
			return errors.New("failed to compile python JSON helper script")
		}

		interpreters[token.Id()] = interpreter{
			code:          code,
			globalsHelper: globalsHelper,
			jsonHelper:    jsonHelper,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	alive := atomic.Int32{}
	alive.Store(int32(cnt))
	return &pythonProcessor{logger: logger, runtime: r, interpreters: interpreters, alive: alive}, nil
}

// Process a given Redpanda Connect service.Message using Python.
func (p *pythonProcessor) Process(ctx context.Context, m *service.Message) (service.MessageBatch, error) {
	var batch []*service.Message

	// Acquire an interpreter and look up our local state.
	token, err := p.runtime.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = p.runtime.Release(token) }()
	i := p.interpreters[token.Id()]

	err = p.runtime.Apply(token, ctx, func() error {
		// We need to set up some bindings so the script can actually _do_ something with our message.
		// For now, we'll use a bit of a hack to create a `content()` function.
		globals := py.PyDict_New()
		locals := py.PyDict_New()
		empty := py.PyDict_New()

		defer py.Py_DecRef(globals)
		defer py.Py_DecRef(locals)
		defer py.Py_DecRef(empty)

		// Prepare our globals. This wires up the Redpanda Connect hook like content().
		if py.PyEval_EvalCode(i.globalsHelper, globals, locals) == py.NullPyObjectPtr {
			return errors.New("interpreter not initialized")
		}
		py.PyDict_SetItemString(locals, "root", empty)

		// We copy-in the data from Redpanda Connect to the Python interpreter as a
		// bytes Python object.
		data, err := m.AsBytes()
		if err != nil {
			return err
		}
		bytes := py.PyBytes_FromStringAndSize(unsafe.SliceData(data), int64(len(data)))
		if bytes == py.NullPyObjectPtr {
			return errors.New("failed to create Python bytes")
		}
		defer py.Py_DecRef(bytes)
		if py.PyDict_SetItemString(globals, "__content__", bytes) != 0 {
			return errors.New("failed to copy-in message bytes")
		}

		// Evaluate the Python script that was pre-compiled into a code object.
		result := py.PyEval_EvalCode(i.code, globals, locals)
		if result == py.NullPyObjectPtr {
			py.PyErr_Print()
			return errors.New("problem executing Python script")
		}
		defer py.Py_DecRef(result)

		// Gives us a borrowed/weak reference to the item.
		root := py.PyDict_GetItemString(locals, "root")
		switch py.BaseType(root) {
		case py.None:
			// Drop the message.
			p.logger.Trace("Received Python None. Dropping message.")
			batch = []*service.Message{}
		case py.Unknown:
			// The script is bad. Fail hard and fast to let the operator know.
			py.PyErr_Print()
			return errors.New("'root' not found in Python script")
		case py.Set:
			// We can't serialize Sets to JSON. Warn and drop.
			p.logger.Warn("Cannot serialize Python set.")
			batch = []*service.Message{}
		case py.Long:
			long := py.PyLong_AsLong(root)
			m.SetStructured(long)
			batch = []*service.Message{m}
		case py.String:
			str, err := py.UnicodeToString(root)
			if err != nil {
				p.logger.Warn("Unable to decode Python string.")
				batch = []*service.Message{}
			} else {
				// We use SetBytes instead of SetStructured to avoid
				// having our string wrapped in double-quotes.
				m.SetBytes([]byte(str))
				batch = []*service.Message{m}
			}
		case py.Tuple:
			fallthrough
		case py.List:
			fallthrough
		case py.Dict:
			// Convert to JSON for now with Python's help ;) because YOLO
			convertResult := py.PyEval_EvalCode(i.jsonHelper, globals, locals)
			if convertResult == py.NullPyObjectPtr {
				return errors.New("failed to JSON-ify root")
			}
			defer py.Py_DecRef(convertResult)

			// The "result" should now be JSON as utf8 bytes. Get a weak
			// reference to the data.
			resultBytes := py.PyDict_GetItemString(locals, "result")
			if resultBytes == py.NullPyObjectPtr {
				return errors.New("result disappeared, oh no")
			}

			// Use [unsafe] to extract the message data.
			sz := py.PyBytes_Size(resultBytes)
			rawBytes := py.PyBytes_AsString(resultBytes)

			// Copy the data out from Python land into Redpanda Connect.
			buffer := make([]byte, sz)
			copy(buffer, unsafe.Slice(rawBytes, sz))
			m.SetBytes(buffer)
			batch = []*service.Message{m}
		}

		return nil
	})

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
