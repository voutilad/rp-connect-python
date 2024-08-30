package processor

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/voutilad/rp-connect-python/internal/impl/python"
	"runtime"
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

type PythonProcessor struct {
	logger         *service.Logger
	runtime        python.Runtime
	interpreters   map[int64]*interpreter
	alive          atomic.Int32
	serializerMode python.SerializerMode
}

type interpreter struct {
	// code is the compiled form of the Processor's Python script.
	code py.PyCodeObjectPtr

	// helperModule provides bloblang-style hooks like content()
	helperModule py.PyObjectPtr
	// helperCode is the compiled Python Code from helperModule
	helperCode py.PyCodeObjectPtr

	// Our serializer helper.
	serializer *python.Serializer

	// globals is the Python globals we use for injecting state.
	globals py.PyObjectPtr

	// locals is the Python local state. Should be cleared after each message.
	locals py.PyObjectPtr

	// root is our Bloblang-like Root instance.
	root py.PyObjectPtr
	// rootClear is the clear() method on our Root instance.
	rootClear  py.PyObjectPtr
	rootClass  py.PyObjectPtr
	rootToDict py.PyObjectPtr

	// meta is our metadata dictionary.
	meta py.PyObjectPtr

	// callbacks we've registered with the interpreter.
	callbacks []*python.Callback
}

// Python helper for initializing a `content` function, returning bytes from
// the globals mapping.
//
//go:embed globals.py
var globalHelperSrc string

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
			Description("Toggle different Python runtime modes.").
			Examples(string(python.Global), string(python.Isolated), string(python.IsolatedLegacy)).
			Default(string(python.Global))).
		Field(service.NewStringField("serializer").
			Description("Serialization mode to use on results.").
			Examples(string(python.None), string(python.Pickle), string(python.Bloblang)).
			Default(string(python.Bloblang)))
	// TODO: linting rules for configuration fields

	err := service.RegisterBatchProcessor("python", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
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
			serializer, err := conf.FieldString("serializer")
			if err != nil {
				return nil, err
			}

			return NewPythonProcessor(exe, script, runtime.NumCPU(), python.StringAsMode(modeString),
				python.StringAsSerializerMode(serializer), mgr.Logger())
		})
	if err != nil {
		// There's no way to fail initialization. We must panic. :(
		panic(err)
	}
}

// NewPythonProcessor creates new Python processor instance with the provided
// configuration.
//
// This will create and initialize a new sub-interpreter from the main Python
// Go routine and precompile some Python code objects.
func NewPythonProcessor(exe, script string, cnt int, mode python.Mode, serializer python.SerializerMode,
	logger *service.Logger) (service.BatchProcessor, error) {

	var err error
	ctx := context.Background()

	// XXX for now, enforce that we only support non-serializing modes when
	// using a global interpreter mode.
	if serializer == python.None && mode != python.Global {
		return nil,
			errors.New("isolated interpreters require bloblang or pickle serialization")
	}

	// Spin up our runtime.
	var processor *PythonProcessor
	switch mode {
	case python.Isolated:
		processor, err = newMultiRuntimeProcessor(exe, cnt, logger)
	case python.IsolatedLegacy:
		processor, err = newLegacyRuntimeProcessor(exe, cnt, logger)
	case python.Global:
		processor, err = newSingleRuntimeProcessor(exe, logger)
	default:
		return nil, errors.New("invalid mode")
	}
	if err != nil {
		return nil, err
	}

	// TODO: should probably tie this logic into the runtime mode as they go hand-in-hand.
	processor.serializerMode = serializer

	// Start the runtime now to ferret out errors.
	err = processor.runtime.Start(ctx)
	if err != nil {
		return nil, err
	}

	// Initialize our sub-interpreter state.
	err = processor.runtime.Map(ctx, func(token *python.InterpreterTicket) error {
		// Pre-compile our script and helpers.
		code := py.Py_CompileString(script, "__rp_connect_python__.py", py.PyFileInput)
		if code == py.NullPyCodeObjectPtr {
			py.PyErr_Print()
			return errors.New("failed to compile python script")
		}

		// Pre-compile our script and helpers.
		helperCode := py.Py_CompileString(globalHelperSrc, "__bloblang__.py", py.PyFileInput)
		if helperCode == py.NullPyCodeObjectPtr {
			py.PyErr_Print()
			return errors.New("failed to compile python helper script")
		}
		helperModule := py.PyImport_ExecCodeModule("__bloblang__", helperCode)
		if helperModule == py.NullPyObjectPtr {
			py.PyErr_Print()
			return errors.New("failed to import python helper module")
		}

		// Create our callback functions.
		metadata, err := python.NewCallback(GlobalMetadata, metadataCallback)
		if err != nil {
			return err
		}
		py.PyModule_AddObjectRef(helperModule, GlobalMetadata, metadata.Object)
		content, err := python.NewCallback(GlobalContent, contentCallback)
		if err != nil {
			return err
		}
		py.PyModule_AddObjectRef(helperModule, GlobalContent, content.Object)

		// Prepare our Root instance and get a reference to it's clear method.
		rootClass := py.PyObject_GetAttrString(helperModule, "Root")
		if rootClass == py.NullPyObjectPtr {
			py.PyErr_Print()
			return errors.New("failed to find Root class in helper module")
		}
		root := py.PyObject_CallNoArgs(rootClass)
		if root == py.NullPyObjectPtr {
			py.PyErr_Print()
			return errors.New("failed to create new Root instance")
		}
		rootClear := py.PyObject_GetAttrString(root, "clear")
		if rootClear == py.NullPyObjectPtr {
			py.PyErr_Print()
			return errors.New("failed to find clear method on Root instance")
		}
		rootToDict := py.PyObject_GetAttrString(root, "to_dict")
		if rootToDict == py.NullPyObjectPtr {
			py.PyErr_Print()
			return errors.New("failed to find to_dict method on Root instance")
		}

		// Set up our main module and derive our globals from it.
		main := py.PyImport_AddModule("__main__")
		if main == py.NullPyObjectPtr {
			return errors.New("failed to add __main__ module")
		}
		globals := py.PyModule_GetDict(main)
		if globals == py.NullPyObjectPtr {
			return errors.New("failed to create globals")
		}

		// Pre-populate globals.
		contentFn := py.PyObject_GetAttrString(helperModule, "content")
		if contentFn == py.NullPyObjectPtr {
			py.PyErr_Print()
			return errors.New("failed to find content function in helper module")
		}
		py.PyDict_SetItemString(globals, "content", contentFn)
		metadataFn := py.PyObject_GetAttrString(helperModule, "metadata")
		if metadataFn == py.NullPyObjectPtr {
			py.PyErr_Print()
			return errors.New("failed to find metadata function in helper module")
		}
		py.PyDict_SetItemString(globals, "metadata", metadataFn)
		unpickleFn := py.PyObject_GetAttrString(helperModule, "unpickle")
		if unpickleFn == py.NullPyObjectPtr {
			py.PyErr_Print()
			return errors.New("failed to find unpickle function in helper module")
		}
		py.PyDict_SetItemString(globals, "unpickle", unpickleFn)

		// Wire in root and "meta" objects.
		locals := py.PyDict_New()
		meta := py.PyDict_New()

		// Create our serializer.
		serializer, err := python.NewSerializer()
		if err != nil {
			return err
		}

		processor.interpreters[token.Id()] = &interpreter{
			code:         code,
			helperCode:   helperCode,
			helperModule: helperModule,
			root:         root,
			rootClass:    rootClass,
			rootClear:    rootClear,
			rootToDict:   rootToDict,
			meta:         meta,
			globals:      globals,
			locals:       locals,
			serializer:   serializer,
			callbacks:    []*python.Callback{metadata, content},
		}
		return nil
	})

	if err != nil {
		// Something is borked. Try to clean up.
		_ = processor.runtime.Stop(ctx)
		return nil, err
	}

	return processor, nil
}

func newMultiRuntimeProcessor(exe string, cnt int, logger *service.Logger) (*PythonProcessor, error) {
	r, err := python.NewMultiInterpreterRuntime(exe, cnt, false, logger)
	if err != nil {
		return nil, err
	}

	p := PythonProcessor{
		logger:       logger,
		runtime:      r,
		interpreters: make(map[int64]*interpreter),
	}
	p.alive.Store(int32(cnt))
	return &p, nil
}

func newLegacyRuntimeProcessor(exe string, cnt int, logger *service.Logger) (*PythonProcessor, error) {
	r, err := python.NewMultiInterpreterRuntime(exe, cnt, true, logger)
	if err != nil {
		return nil, err
	}

	p := PythonProcessor{
		logger:       logger,
		runtime:      r,
		interpreters: make(map[int64]*interpreter),
	}
	p.alive.Store(int32(cnt))
	return &p, nil
}

func newSingleRuntimeProcessor(exe string, logger *service.Logger) (*PythonProcessor, error) {
	r, err := python.NewSingleInterpreterRuntime(exe, logger)
	if err != nil {
		return nil, err
	}

	p := PythonProcessor{
		logger:       logger,
		runtime:      r,
		interpreters: make(map[int64]*interpreter),
	}
	p.alive.Store(1)
	return &p, nil
}

// ProcessBatch executes the given Python script against each message in the batch.
func (p *PythonProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	// Acquire an interpreter and look up our local state.
	ticket, err := p.runtime.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = p.runtime.Release(ticket) }()

	// Look up our previously initialized interpreter state.
	i := p.interpreters[ticket.Id()]

	newBatch := service.MessageBatch{}

	err = p.runtime.Apply(ticket, ctx, func() error {
		for _, m := range batch {
			// Abort if we're cancelling execution.
			if ctx.Err() != nil {
				return ctx.Err()
			}

			// Clear out any local state from previous messages.
			py.PyDict_Clear(i.meta)
			py.PyObject_CallNoArgs(i.rootClear)

			// We always have a root and meta instance available, regardless
			// the interpreter and serializer modes.
			py.PyDict_SetItemString(i.locals, "root", i.root)
			py.PyDict_SetItemString(i.locals, "meta", i.meta)

			// Set up our pointer to our service.Message in case the script is
			// accessing raw bytes.
			addr := py.PyLong_FromUnsignedLong(uint64(uintptr(unsafe.Pointer(m))))
			if py.PyModule_AddObjectRef(i.helperModule, GlobalMessageAddr, addr) != 0 {
				return errors.New("failed to set address of message")
			}
			py.Py_DecRef(addr)

			// If the incoming message was from a previous Python component,
			// see if it passed us a Python object. If so, we use it for
			// creating "this".
			mode, ok := m.MetaGetMut(python.SerializerMetaKey)
			if ok && mode.(python.SerializerMode) == python.None {
				this, err := m.AsStructured()
				if err != nil {
					panic(err)
				}
				py.PyDict_SetItemString(i.locals, "this", this.(py.PyObjectPtr))
			}

			// Evaluate the Python script that was pre-compiled into a code object.
			// It should have access to global helper functions/classes and should
			// set a local called "root".
			result := py.PyEval_EvalCode(i.code, i.globals, i.locals)
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

			// Shallow-copy before we mutate the message and metadata.
			newMessage := m.Copy()

			// The user might have modified the "meta" mapping to set new metadata
			// on the message.
			meta := py.PyDict_GetItemString(i.locals, "meta")
			if meta != py.NullPyObjectPtr {
				// XXX If meta is re-assigned and _not_ a dictionary, we error but
				// keep running. Not sure best approach yet.
				err = handleMeta(meta, newMessage, i)
				if err != nil {
					newMessage.SetError(err)
				}
			}

			// Handle the actual message data based on our serializer mode.
			switch p.serializerMode {
			case python.None:
				// We don't serialize and instead pass a Python object pointer.
				if py.BaseType(root) == py.None {
					// Drop the message.
					// TODO: Is this correct? To drop do we just not output a new message?
					continue
				} else {
					// XXX validate we're using global interpreter mode?
					newMessage.SetStructured(root)
				}
			case python.Bloblang:
				drop, err := handleRootAsJson(root, newMessage, i)
				if drop {
					// TODO: Is this correct? To drop do we just not output a new message?
					continue
				}
				if err != nil {
					newMessage.SetError(err)
				}

			case python.Pickle:
				drop, err := handleRootAsPickle(root, newMessage, i)
				if drop {
					// TODO: Is this correct? To drop do we just not output a new message?
					continue
				}
				if err != nil {
					newMessage.SetError(err)
				}
			}

			newMessage.MetaSetMut(python.SerializerMetaKey, p.serializerMode)
			newBatch = append(newBatch, newMessage)
		}
		return nil
	})

	if len(newBatch) == 0 || err != nil {
		return nil, err
	}

	return []service.MessageBatch{newBatch}, err
}

func handleRootAsPickle(root py.PyObjectPtr, m *service.Message, i *interpreter) (bool, error) {
	if py.BaseType(root) == py.None {
		// We don't pickle None's.
		return true, nil
	}
	pickled, err := i.serializer.Pickle(root)
	if err != nil {
		panic(err)
	}
	m.SetBytes(pickled)
	return false, nil
}

// handleRoot post-processes the `root` object the Python script may have
// mutated at runtime.
func handleRootAsJson(root py.PyObjectPtr, m *service.Message, i *interpreter) (bool, error) {
	// Check our type and use an optimized conversion approach if possible.
	switch py.BaseType(root) {
	case py.None:
		// Drop the message.
		return true, nil

	case py.Set:
		// We can't serialize Sets to JSON.
		return false, errors.New("cannot serialize a Python set")

	case py.Long:
		long := py.PyLong_AsLong(root)
		m.SetStructured(long)

	case py.String:
		str, err := py.UnicodeToString(root)
		if err != nil {
			return false, errors.New("unable to decode Python string")
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
		buffer := make([]byte, sz)
		copy(buffer, unsafe.Slice(p, sz))
		m.SetBytes(buffer)

	case py.Tuple, py.List, py.Dict, py.Unknown:
		obj := root
		// Convert to JSON bytes for now with Python's help ;) because YOLO
		if py.PyObject_IsInstance(root, i.rootClass) == 1 {
			// We need to convert to a dict first.
			obj = py.PyObject_CallNoArgs(i.rootToDict)
			defer py.Py_DecRef(obj)
			if obj == py.NullPyObjectPtr {
				py.PyErr_Print()
				panic("failed to convert root object to a dict")
			}
		}
		buffer, err := i.serializer.JsonBytes(obj)
		if err != nil {
			panic(err)
		}
		m.SetBytes(buffer)
	}

	return false, nil
}

// handleMeta extracts any metadata updates made by the Python script.
//
// It's far from efficient for container values (lists, tuples, dicts) as it
// relies on the Python side serializing to JSON and the Go side
// deserializing from JSON.
func handleMeta(meta py.PyObjectPtr, m *service.Message, i *interpreter) error {
	if py.BaseType(meta) != py.Dict {
		return errors.New("meta python type is not a dictionary")
	}

	keys := py.PyDict_Keys(meta)
	if keys == py.NullPyObjectPtr {
		return errors.New("failed to get keys from metadata dictionary")
	}
	defer py.Py_DecRef(keys)
	if py.BaseType(keys) != py.List {
		// This should not happen. If it does, something is horribly wrong.
		panic("keys wasn't a Python list?!")
	}

	for idx := int64(0); idx < py.PyList_Size(keys); idx++ {
		key := py.PyList_GetItem(keys, idx)
		if key == py.NullPyObjectPtr {
			// Shouldn't happen...
			panic("metadata dictionary key was null")
		}
		keyString, err := py.UnicodeToString(key)
		if err != nil {
			panic("could not decode dictionary key")
		}
		val := py.PyDict_GetItem(meta, key)
		if val == py.NullPyObjectPtr {
			// We shouldn't get null pointers. Something is wrong.
			panic(fmt.Sprintf("metadata dictionary value was null for key %s", keyString))
		}

		switch py.BaseType(val) {
		case py.None:
			// Remove our dictionary item.
			m.MetaDelete(keyString)

		case py.String:
			valString, err := py.UnicodeToString(val)
			if err != nil {
				panic("could not decode Python string used as metadata value")
			}
			m.MetaSetMut(keyString, valString)

		case py.Bytes:
			p := py.PyBytes_AsString(val)
			sz := py.PyBytes_Size(val)
			m.MetaSetMut(keyString, unsafe.Slice(p, sz))

		case py.Long:
			long := py.PyLong_AsLong(val)
			m.MetaSetMut(keyString, long)

		case py.Float:
			float := py.PyFloat_AsDouble(val)
			m.MetaSetMut(keyString, float)

		case py.Tuple, py.List, py.Dict:
			// Convert to a JSON string
			str, err := i.serializer.JsonString(val)
			if err != nil {
				panic(err)
			}

			// XXX unmarshal the JSON back into Go objects using the json
			// module. This handles nested structures nicely and saves on
			// writing a bunch of recursive extraction code.
			var _map map[string]any
			err = json.Unmarshal([]byte(str), &_map)
			if err != nil {
				panic(fmt.Sprintf("%s: %s", "failed to unmarshal json", err))
			}
			m.MetaSetMut(keyString, _map)

		default:
			return errors.New("unhandled metadata dictionary value")
		}
	}
	return nil
}

// Close a processor.
//
// If we're the last Python Processor, ask the main Go routine to stop the runtime.
func (p *PythonProcessor) Close(ctx context.Context) error {
	if p.alive.Add(-1) == 0 {
		p.logger.Debug("Stopping all sub-interpreters for processor")
		return p.runtime.Stop(ctx)
	}

	return nil
}
