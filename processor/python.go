package processor

import (
	"context"
	"errors"
	"github.com/voutilad/rp-connect-python/internal/impl/python"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/redpanda-data/benthos/v4/public/service"

	py "github.com/voutilad/gogopython"
)

var processorCnt atomic.Int32

type pythonProcessor struct {
	logger           *service.Logger
	exe              string
	closed           atomic.Bool
	interpreterState py.PyInterpreterStatePtr
	threadState      py.PyThreadStatePtr
	mtx              sync.Mutex
	code             py.PyCodeObjectPtr
	globalsHelper    py.PyCodeObjectPtr
	jsonHelper       py.PyCodeObjectPtr
}

var pythonExe = ""
var pythonRuntime *python.Runtime
var pythonMtx sync.Mutex

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
	// Initialize globals.
	processorCnt.Store(0)

	configSpec := service.
		NewConfigSpec().
		Summary("Process data with Python.").
		Field(service.NewStringField("script").
			Description("Python code to execute.")).
		Field(service.NewStringField("exe").
			Description("Path to a Python executable.").
			Default("python3"))

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
	// Extract our configuration.
	exe, err := conf.FieldString("exe")
	if err != nil {
		panic(err)
	}
	script, err := conf.FieldString("script")
	if err != nil {
		return nil, err
	}

	// Look up or create a Runtime.
	pythonMtx.Lock()
	if pythonExe == "" {
		r, err := python.New(exe)
		if err != nil {
			return nil, err
		}
		pythonRuntime = r
		pythonExe = exe
	} else if pythonExe != exe {
		pythonMtx.Unlock()
		return nil, errors.New("multiple python processors must use the same exe")
	}
	pythonMtx.Unlock()

	// We'll give ourselves 10 seconds to initialize.
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*10))
	defer cancel()

	// Start the runtime.
	err = pythonRuntime.Start(ctx, mgr.Logger())
	if err != nil {
		return nil, err
	}

	// Start a sub-interpreter.
	interpreterState, err := pythonRuntime.Spawn(ctx)
	if err != nil {
		return nil, err
	}

	// We're about to use the sub-interpreter, so lock our Go routine to a thread.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// Create a new ThreadState.
	threadState := py.PyThreadState_New(interpreterState)
	py.PyEval_RestoreThread(threadState)

	// Pre-compile our script and helpers.
	code := py.Py_CompileString(script, "rp_connect_python.py", py.PyFileInput)
	if code == py.NullPyCodeObjectPtr {
		py.PyErr_Print()
		return nil, errors.New("failed to compile python script")
	}

	// Pre-compile our script and helpers.
	globalsHelper := py.Py_CompileString(globalHelperSrc, "__globals_helper__.py", py.PyFileInput)
	if globalsHelper == py.NullPyCodeObjectPtr {
		py.PyErr_Print()
		return nil, errors.New("failed to compile python globals helper script")
	}

	// Pre-compile our script and helpers.
	jsonHelper := py.Py_CompileString(jsonHelperSrc, "__json_helper__.py", py.PyFileInput)
	if jsonHelper == py.NullPyCodeObjectPtr {
		py.PyErr_Print()
		return nil, errors.New("failed to compile python JSON helper script")
	}

	// XXX This is not good and will be global across all runtimes.
	processorCnt.Add(1)

	py.PyEval_SaveThread()

	return &pythonProcessor{
		logger:           mgr.Logger(),
		exe:              exe,
		interpreterState: interpreterState,
		threadState:      threadState,
		code:             code,
		globalsHelper:    globalsHelper,
		jsonHelper:       globalsHelper,
	}, nil
}

// Process a given Redpanda Connect service.Message using Python.
func (p *pythonProcessor) Process(_ context.Context, m *service.Message) (service.MessageBatch, error) {
	var err error = nil
	var batch []*service.Message

	p.mtx.Lock()
	defer p.mtx.Unlock()

	// We need to lock our OS thread so Go won't screw us.
	runtime.LockOSThread()

	py.PyEval_RestoreThread(p.threadState)

	// We need to set up some bindings so the script can actually _do_ something with our message.
	// For now, we'll use a bit of a hack to create a `content()` function.
	globals := py.PyDict_New() // xxx can we save this between runs? There must be a way.
	locals := py.PyDict_New()

	empty := py.PyDict_New()
	py.PyDict_SetItemString(locals, "root", empty)
	py.Py_DecRef(empty)

	if py.PyEval_EvalCode(p.globalsHelper, globals, locals) == py.NullPyObjectPtr {
		p.logger.Warn("something failed preparing content()!!!")
	} else {
		var data []byte
		data, err = m.AsBytes()
		if err == nil {
			// Will copy the underlying data into a new PyObject.
			bytes := py.PyBytes_FromStringAndSize(unsafe.SliceData(data), len(data))

			if bytes == py.NullPyObjectPtr {
				err = errors.New("failed to create Python bytes")
			} else {
				_ = py.PyDict_SetItemString(globals, "__content__", bytes)
				py.Py_DecRef(bytes)
				// xxx check return value
				result := py.PyEval_EvalCode(p.code, globals, locals)
				if result == py.NullPyObjectPtr {
					py.PyErr_Print()
					err = errors.New("problem executing Python script")
				}

				// XXX Gives us a borrowed reference to the item.
				root := py.PyDict_GetItemString(locals, "root")
				switch py.Py_BaseType(root) {
				case py.None:
					// Drop the message.
					p.logger.Trace("dropping message")
					batch = []*service.Message{}
				case py.Unknown:
					// The script is bad. Fail hard and fast to let the operator know.
					py.PyErr_Print()
					err = errors.New("'root' not found in Python script")
				case py.Long:
					// todo: we can handle this :)
					fallthrough
				case py.String:
					// todo: we can handle this :)
					fallthrough
				case py.Tuple:
					fallthrough
				case py.List:
					fallthrough
				case py.Dict:
					// Convert to JSON for now with Python's help ;) because YOLO
					convertResult := py.PyEval_EvalCode(p.jsonHelper, globals, locals)
					if convertResult == py.NullPyObjectPtr {
						err = errors.New("failed to JSON-ify root")
					} else {
						// Not needed any longer.
						py.Py_DecRef(convertResult)

						// The "result" should now be JSON as utf8 bytes.
						resultBytes := py.PyDict_GetItemString(locals, "result")
						if resultBytes == py.NullPyObjectPtr {
							err = errors.New("result disappeared, oh no")
						} else {
							// Use [unsafe] to extract the message data.
							sz := py.PyBytes_Size(resultBytes)
							rawBytes := py.PyBytes_AsString(resultBytes)

							// Copy the data out from Python land.
							buffer := make([]byte, sz)
							copy(buffer, unsafe.Slice(rawBytes, sz))
							m.SetBytes(buffer)

							batch = []*service.Message{m}
						}
					}
				}
				py.Py_DecRef(result)
			}
		}
	}

	py.Py_DecRef(globals)
	py.Py_DecRef(locals)

	// Ok for Go to do its thing again.
	py.PyEval_SaveThread()
	runtime.UnlockOSThread()

	return batch, err
}

// Close a processor.
//
// If we're the last Python Processor, ask the main Go routine to stop the runtime.
func (p *pythonProcessor) Close(ctx context.Context) error {
	// It seems Close may be called multiple times for a single processor.
	// Guard against it.
	if !p.closed.CompareAndSwap(false, true) {
		return nil
	}

	p.mtx.Lock()
	runtime.LockOSThread()
	// Restore our thread state so we can clean things.
	py.PyEval_RestoreThread(p.threadState)

	// Clean-up code objects.
	py.Py_DecRef(py.PyObjectPtr(p.code))
	py.Py_DecRef(py.PyObjectPtr(p.globalsHelper))
	py.Py_DecRef(py.PyObjectPtr(p.jsonHelper))

	// Clean up our thread state.
	py.PyThreadState_Clear(p.threadState)
	py.PyThreadState_DeleteCurrent()
	runtime.UnlockOSThread()
	p.mtx.Unlock()

	if processorCnt.Add(-1) == 0 {
		// Look up or create a Runtime.
		pythonMtx.Lock()
		r := pythonRuntime
		pythonMtx.Unlock()
		if r == nil {
			return errors.New("runtime disappeared")
		}
		return r.Stop(ctx)
	}

	return nil
}
