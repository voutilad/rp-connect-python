package processor

import (
	"bufio"
	"context"
	"errors"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/redpanda-data/benthos/v4/public/service"

	py "github.com/voutilad/gogopython"
)

var processorCnt atomic.Int32
var started atomic.Bool
var ready atomic.Bool

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
	closed atomic.Bool
}

func init() {
	processorCnt.Store(0)

	toMain = make(chan message)
	fromMain = make(chan reply)

	configSpec := service.
		NewConfigSpec().
		Summary("Process data with Python").
		Field(service.NewStringField("script")).
		Field(service.NewStringField("exe").Default("python3"))

	ctor := func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
		logger := mgr.Logger()
		if started.CompareAndSwap(false, true) {
			go func() {
				exe, err := conf.FieldString("exe")
				if err != nil {
					panic(err)
				}
				logger.Infof("finding path details for %s\n", exe)
				helper := "'import sys; print(sys.prefix); [print(p) for p in sys.path if len(p) > 0]'"
				cmd := exec.Command(exe, "-c", helper)
				stdout, err := cmd.StdoutPipe()
				if err != nil {
					panic(err)
				}
				if err = cmd.Start(); err != nil {
					panic(err)
				}
				scanner := bufio.NewScanner(bufio.NewReader(stdout))

				// First line is our home, subsequent are the path
				var home string
				paths := make([]string, 0)
				first := true
				for scanner.Scan() {
					text := scanner.Text()
					if first {
						home = text
						first = false
					} else {
						paths = append(paths, text)
					}
				}
				if err = cmd.Wait(); err != nil {
					panic(err)
				}

				// We now become the Python charmer.
				mainPython(exe, home, paths, logger)
			}()

			// Send our Start message
			chanMtx.Lock()
			toMain <- PYTHON_START
			r := <-fromMain
			chanMtx.Unlock()
			if r.err != nil {
				return nil, r.err
			}
		}

		// hacky, but use the ping pong to make sure we're ready
		chanMtx.Lock()
		toMain <- PYTHON_STATUS
		r := <-fromMain
		chanMtx.Unlock()
		if r.err != nil {
			// this should not happen!
			panic(r.err)
		}

		return newPythonProcessor(mgr.Logger(), conf)
	}

	err := service.RegisterProcessor("python", configSpec, ctor)
	if err != nil {
		panic(err)
	}
}

func mainPython(exe, home string, paths []string, logger *service.Logger) {
	pythonStarted := false
	keepGoing := true

	mainThreadState := py.NullThreadState
	subInterpreters := make([]subInterpreter, 0)

	logger.Infof("starting main python interpreter with home='%s'\n", home)

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	for keepGoing {
		msg := <-toMain
		switch msg {
		case PYTHON_START:
			if !pythonStarted {
				logger.Info("starting python interpreter")
				var err error
				mainThreadState, err = initPythonOnce(exe, home, paths)
				if err != nil {
					keepGoing = false
					logger.Errorf("failed to start python interpreter: %s", err)
				}
				pythonStarted = true
				fromMain <- reply{err: err}
			} else {
				logger.Warn("interpreter already started")
				fromMain <- reply{err: errors.New("interpreter already started")}
			}

		case PYTHON_STOP:
			if pythonStarted {
				keepGoing = false
				for _, s := range subInterpreters {
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
			subInterpreters = append(subInterpreters, *subInterpreter)
			fromMain <- reply{state: subInterpreter.state}

		case PYTHON_STATUS:
			logger.Info("i'm alive!")
			fromMain <- reply{}
		}
	}
}

// Initialize the main interpreter. Sub-interpreters are created by Processor instances.
func initPythonOnce(exe, home string, paths []string) (py.PyThreadStatePtr, error) {
	// Find and load the Python dynamic library.
	err := py.Load_library(exe)
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
	// For now, we just execute a "script" inline in the yaml.
	script, err := conf.FieldString("script")
	if err != nil {
		return nil, err
	}

	// The "main" go routine takes care of initializing the sub-interpreter.
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
	}, nil
}

func (p *pythonProcessor) Process(ctx context.Context, m *service.Message) (service.MessageBatch, error) {
	p.logger.Info("processing message")

	// We need to lock our OS thread so Go won't screw us.
	runtime.LockOSThread()

	// We may be on a *new* OS thread since the last time, so play it safe and make new state.
	ts := py.PyThreadState_New(p.state)
	py.PyEval_RestoreThread(ts)

	// Make Python go now.
	py.PyRun_SimpleString(p.script)

	// Clean up our thread state. Impossible to re-use safely with Go.
	py.PyThreadState_Clear(ts)
	py.PyThreadState_DeleteCurrent()

	// Ok for Go to do its thing again.
	runtime.UnlockOSThread()

	return []*service.Message{m}, nil
}

// Teardown a Sub-Interpreter and delete its state. This will probably trigger a lot of Python
// cleanup under the hood.
//
// This returns void because most of these calls are fatal :x
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
	// xxx
	if !p.closed.CompareAndSwap(false, true) {
		return nil
	}

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
