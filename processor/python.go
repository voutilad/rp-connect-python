package processor

import (
	"context"
	"errors"
	"github.com/voutilad/rp-connect-python/internal/impl/python"
	"runtime"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/redpanda-data/benthos/v4/public/service"

	py "github.com/voutilad/gogopython"
)

var processorCnt atomic.Int32

type pythonProcessor struct {
	logger *service.Logger
	state  py.PyInterpreterStatePtr
	script string
	closed atomic.Bool
}

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
func constructor(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	// Extract our configuration.
	exe, err := conf.FieldString("exe")
	if err != nil {
		panic(err)
	}
	home, paths, err := py.FindPythonHomeAndPaths(exe)
	if err != nil {
		panic(err)
	}
	script, err := conf.FieldString("script")
	if err != nil {
		return nil, err
	}

	// Start the runtime.
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*3))
	defer cancel()
	err = python.Start(python.Config{Exe: exe, Home: home, Paths: paths}, ctx, mgr.Logger())
	if err != nil {
		return nil, err
	}

	// Start a sub-interpreter.
	state, err := python.NewSubInterpreter(ctx)
	if err != nil {
		return nil, err
	}

	processorCnt.Add(1)
	return &pythonProcessor{logger: mgr.Logger(), script: script, state: state}, nil
}

// Python helper for initializing a content function in the global state.
const defContent = `
global content
def content():
	# Returns the content of the message being processed.
	global __content__
	return __content__
`

// Python helper for serializing the "result" of a processor.
const jsonHelper = `
import json
try: 
	result = json.dumps(root).encode()
except:
	result = None
`

// Process a given Redpanda Connect service.Message using Python.
func (p *pythonProcessor) Process(ctx context.Context, m *service.Message) (service.MessageBatch, error) {
	var err error = nil
	var batch []*service.Message
	p.logger.Info("processing message")

	// We need to lock our OS thread so Go won't screw us.
	runtime.LockOSThread()

	// We may be on a *new* OS thread since the last time, so play it safe and make new state.
	// TODO: only flush if we detect we've drifted OS threads?
	ts := py.PyThreadState_New(p.state)
	py.PyEval_RestoreThread(ts)

	// We need to set up some bindings so the script can actually _do_ something with our message.
	// For now, we'll use a bit of a hack to create a `content()` function.
	globals := py.PyDict_New() // xxx can we save this between runs? There must be a way.
	locals := py.PyDict_New()
	py.PyDict_SetItemString(locals, "root", py.PyDict_New())

	if py.PyRun_String(defContent, py.PyFileInput, globals, locals) == py.NullPyObjectPtr {
		p.logger.Warn("something failed preparing content()!!!")
	} else {
		var data []byte
		data, err = m.AsBytes()
		if err == nil {
			bytes := py.PyBytes_FromStringAndSize(unsafe.SliceData(data), len(data))
			if bytes == py.NullPyObjectPtr {
				err = errors.New("failed to create Python bytes")
			} else {
				_ = py.PyDict_SetItemString(globals, "__content__", bytes)
				// xxx check return value
				result := py.PyRun_String(p.script, py.PyFileInput, globals, locals)
				if result == py.NullPyObjectPtr {
					py.PyErr_Print()
					err = errors.New("problem executing Python script")
				}
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
					result = py.PyRun_String(jsonHelper, py.PyFileInput, globals, locals)
					if result == py.NullPyObjectPtr {
						err = errors.New("failed to JSONify root")
					} else {
						// The "result" should now be JSON as utf8 bytes.
						bytes := py.PyDict_GetItemString(locals, "result")
						if bytes == py.NullPyObjectPtr {
							err = errors.New("result disappeared, oh no")
						} else {
							// Use [unsafe] to extract the message data.
							sz := py.PyBytes_Size(bytes)
							rawBytes := py.PyBytes_AsString(bytes)
							m.SetBytes(unsafe.Slice(rawBytes, sz))
							batch = []*service.Message{m}
						}
					}
				}
			}
		}
	}

	// Clean up our thread state. Impossible to re-use safely with Go.
	py.PyThreadState_Clear(ts)
	py.PyThreadState_DeleteCurrent()

	// Ok for Go to do its thing again.
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

	if processorCnt.Add(-1) == 0 {
		return python.Stop(ctx)
	}

	return nil
}
