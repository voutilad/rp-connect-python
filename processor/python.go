package processor

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/redpanda-data/benthos/v4/public/service"

	py "github.com/voutilad/gogopython"
)

var processorCnt atomic.Int32
var started atomic.Bool

type message int

const (
	PYTHON_START  message = iota
	PYTHON_STATUS         // ping pong!
	PYTHON_STOP           // stop the world
	PYTHON_SPAWN          // spawn a subinterpreter
)

type reply struct {
	err   error
	state py.PyInterpreterStatePtr
}

var fromMain chan reply
var toMain chan message
var chanMtx sync.Mutex

type subInterpreter struct {
	originalTs py.PyThreadStatePtr
	state      py.PyInterpreterStatePtr
	id         int64
}

type pythonProcessor struct {
	logger *service.Logger
	state  py.PyInterpreterStatePtr
	script string
	closed bool
}

func init() {
	processorCnt.Store(0)

	toMain = make(chan message)
	fromMain = make(chan reply)

	configSpec := service.
		NewConfigSpec().
		Summary("Process data with Python").
		Field(service.NewStringField("script")).
		Field(service.NewStringField("home").Default("./venv")).
		Field(service.NewStringListField("paths").Default([]string{}))

	ctor := func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
		home, err := conf.FieldString("home")
		if err != nil {
			return nil, err
		}
		paths, err := conf.FieldStringList("paths")
		if err != nil {
			return nil, err
		}

		if started.CompareAndSwap(false, true) {
			go func() {
				mainPython(home, paths, mgr.Logger())
			}()
			chanMtx.Lock()
			toMain <- PYTHON_START
			r := <-fromMain
			chanMtx.Unlock()
			if r.err != nil {
				return nil, err
			}
		}

		return newPythonProcessor(mgr.Logger(), conf)
	}

	err := service.RegisterProcessor("python", configSpec, ctor)
	if err != nil {
		panic(err)
	}
}

func mainPython(home string, paths []string, logger *service.Logger) {
	started := false
	keepGoing := true

	mainThreadState := py.NullThreadState
	subInterpeters := make([]subInterpreter, 0)

	runtime.LockOSThread()

	for keepGoing {
		msg := <-toMain
		switch msg {
		case PYTHON_START:
			if !started {
				logger.Info("starting python interpreter")
				var err error
				mainThreadState, err = initPythonOnce(home, paths)
				if err != nil {
					keepGoing = false
					logger.Errorf("failed to start python interpreter: %s", err)
				}
				started = true
				fromMain <- reply{err: err}
			} else {
				logger.Warn("interpreter already started")
				fromMain <- reply{err: errors.New("interpreter already started")}
			}

		case PYTHON_STOP:
			if started {
				keepGoing = false
				for _, s := range subInterpeters {
					stopSubInterpreter(s, mainThreadState, logger)
				}
				if py.Py_FinalizeEx() != 0 {
					// The chance we get here *without* an explosion is slim, but why not.
					fromMain <- reply{err: errors.New("failed to shutdown python")}
				} else {
					fromMain <- reply{}
				}
			} else {
				logger.Warn("interpreter not running")
				fromMain <- reply{err: errors.New("interpreter not running")}
			}

		case PYTHON_SPAWN:
			logger.Info("spawning a sub-interpreter")
			subInterpreter, err := initSubInterpreter(logger)
			if err != nil {
				keepGoing = false
				logger.Warn("failed to create subinterpreter")
				fromMain <- reply{err: err}
			}
			subInterpeters = append(subInterpeters, *subInterpreter)
			fromMain <- reply{state: subInterpreter.state}

		case PYTHON_STATUS:
			logger.Info("i'm alive!")
			fromMain <- reply{}
		}
	}

	runtime.UnlockOSThread()
}

// Initialize the main interpreter. Sub-interpreters are created by Processor instances.
func initPythonOnce(home string, paths []string) (py.PyThreadStatePtr, error) {
	// Find and load the Python dynamic library.
	err := py.Load_library()
	if err != nil {
		return py.NullThreadState, err
	}

	// Pre-configure the Python Interpreter
	preConfig := py.PyPreConfig{}
	py.PyPreConfig_InitIsolatedConfig(&preConfig)
	status := py.Py_PreInitialize(&preConfig)
	if status.Type != 0 {
		errmsg := py.PyBytesToString(status.ErrMsg)
		return py.NullThreadState, errors.New(errmsg)
	}

	/*
	 * Configure our Paths. We need to approximate an isolated config from regular
	 * because Python will ignore our modifying some values if we initialize an
	 * isolated config. Annoying.
	 */
	config := py.PyConfig_3_12{}
	py.PyConfig_InitPythonConfig(&config)
	config.ParseArgv = 0
	config.SafePath = 1
	config.UserSiteDirectory = 0
	config.InstallSignalHandlers = 0

	status = py.PyConfig_SetBytesString(&config, &config.Home, home)
	if status.Type != 0 {
		errmsg := py.PyBytesToString(status.ErrMsg)
		return py.NullThreadState, errors.New(errmsg)
	}
	path := strings.Join(paths, ":") // xxx ';' on windows
	status = py.PyConfig_SetBytesString(&config, &config.PythonPathEnv, path)
	if status.Type != 0 {
		errmsg := py.PyBytesToString(status.ErrMsg)
		return py.NullThreadState, errors.New(errmsg)
	}
	status = py.Py_InitializeFromConfig(&config)
	if status.Type != 0 {
		errmsg := py.PyBytesToString(status.ErrMsg)
		return py.NullThreadState, errors.New(errmsg)
	}

	// Save details on our Main thread state and drop GIL.
	mainThreadState := py.PyEval_SaveThread()

	return mainThreadState, nil
}

func initSubInterpreter(logger *service.Logger) (*subInterpreter, error) {
	// Some of these args are required if we want to use Numpy, etc.
	var subStatePtr py.PyThreadStatePtr
	interpreterConfig := py.PyInterpreterConfig{}
	interpreterConfig.Gil = py.DefaultGil // OwnGil works in 3.12, but is hard to use.
	interpreterConfig.CheckMultiInterpExtensions = 0
	interpreterConfig.UseMainObMalloc = 1

	// Cross your fingers...
	status := py.Py_NewInterpreterFromConfig(&subStatePtr, &interpreterConfig)
	if status.Type != 0 {
		errmsg := py.PyBytesToString(status.ErrMsg)
		logger.Errorf("failed to create sub-interpreter: %s", errmsg)
		return nil, errors.New(errmsg)
	}

	// Collect our information and drop the GIL.
	state := py.PyInterpreterState_Get()
	id := py.PyInterpreterState_GetID(state)
	ts := py.PyEval_SaveThread()

	return &subInterpreter{
		originalTs: ts,
		state:      state,
		id:         id,
	}, nil
}

func newPythonProcessor(logger *service.Logger, conf *service.ParsedConfig) (*pythonProcessor, error) {
	script, err := conf.FieldString("script")
	if err != nil {
		return nil, err
	}

	chanMtx.Lock()
	toMain <- PYTHON_SPAWN
	r := <-fromMain
	chanMtx.Unlock()

	if r.err != nil {
		return nil, err
	}

	i := processorCnt.Add(1)

	logger.Infof("processor cnt: %d\n", i)
	return &pythonProcessor{
		logger: logger,
		script: script,
		state:  r.state,
		closed: false,
	}, nil
}

func (p *pythonProcessor) Process(ctx context.Context, m *service.Message) (service.MessageBatch, error) {

	p.logger.Info("processing message")
	runtime.LockOSThread()

	// We can't trust we're on the same OS thread as before, so we're forced to do this dance.
	ts := py.PyThreadState_New(p.state)
	py.PyEval_RestoreThread(ts)

	// Make python go now.
	py.PyRun_SimpleString(p.script)

	// Clean up our thread state. No idea how to reuse it safely :(
	py.PyThreadState_Clear(ts)
	py.PyThreadState_DeleteCurrent()

	runtime.UnlockOSThread()

	return []*service.Message{m}, nil
}

func stopSubInterpreter(s subInterpreter, mainState py.PyThreadStatePtr, logger *service.Logger) {
	logger.Infof("stopping sub-interpreter %d\n", s.id)

	py.PyEval_RestoreThread(s.originalTs)
	py.PyThreadState_Clear(s.originalTs)

	py.PyInterpreterState_Clear(s.state)
	py.PyInterpreterState_Delete(s.state)

	py.PyEval_RestoreThread(mainState)

	logger.Infof("stopped sub-interpreter %d\n", s.id)
}

func (p *pythonProcessor) Close(ctx context.Context) error {

	if p.closed {
		return nil
	}
	p.closed = true

	if processorCnt.Add(-1) == 0 {
		p.logger.Info("last one...telling main interpreter to clean up")
		chanMtx.Lock()
		toMain <- PYTHON_STOP
		r := <-fromMain
		chanMtx.Unlock()

		if r.err != nil {
			return r.err
		}
	}

	return nil
}
