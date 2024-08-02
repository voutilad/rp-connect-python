package python

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

// Config for a Python runtime.
type Config struct {
	Exe   string   // Path to the python executable.
	Home  string   // Python "home" to use.
	Paths []string // Paths to set in the Python runtime.
}

// Atomic used to flagging if we're already launched the Python runtime.
var started atomic.Bool

// Request the Python runtime Go routine perform an action.
type request int

const (
	pythonStart  request = iota // Start the runtime.
	pythonStatus                // Are you alive?
	pythonStop                  // Stop and shutdown sub-interpreters.
	pythonSpawn                 // Span a new sub-interpreter.
)

// Reply from the Python runtime Go routine in response to a Request.
type reply struct {
	err   error
	state py.PyInterpreterStatePtr
}

var fromMain chan reply
var toMain chan request
var chanMtx sync.Mutex

// State related to a Python Sub-interpreter.
type subInterpreter struct {
	originalTs py.PyThreadStatePtr      // Original Python ThreadState.
	state      py.PyInterpreterStatePtr // Interpreter State.
	id         int64                    // Unique identifier.
}

// Start the Python runtime.
//
// This is idempotent. Multiple calls to Start will do nothing.
func Start(config Config, ctx context.Context, logger *service.Logger) error {
	if started.CompareAndSwap(false, true) {
		fromMain = make(chan reply)
		toMain = make(chan request)

		// Launch the main Go routine for managing the runtime.
		go mainPython(config, toMain, fromMain, logger)
	}

	// Tell it to start.
	chanMtx.Lock()
	defer chanMtx.Unlock()
	toMain <- pythonStart
	select {
	case r := <-fromMain:
		if r.err != nil {
			return r.err
		}
	case <-ctx.Done():
		return errors.New("interrupted")
	}
	return nil
}

func Stop(ctx context.Context) error {
	// Tell it to start.
	chanMtx.Lock()
	defer chanMtx.Unlock()
	toMain <- pythonStop
	select {
	case r := <-fromMain:
		if r.err != nil {
			return r.err
		}
	case <-ctx.Done():
		return errors.New("interrupted")
	}
	return nil
}

func NewSubInterpreter(ctx context.Context) (py.PyInterpreterStatePtr, error) {
	state := py.NullInterpreterState
	var err error = nil

	// Tell it to spawn a new sub-interpreter.
	chanMtx.Lock()
	defer chanMtx.Unlock()
	toMain <- pythonSpawn
	select {
	case r := <-fromMain:
		if r.err != nil {
			err = r.err
		} else {
			state = r.state
		}
	case <-ctx.Done():
		err = errors.New("interrupted")
	}
	return state, err
}

// Teardown a Sub-Interpreter and delete its state. This will probably trigger a lot of Python
// cleanup under the hood.
//
// Note: This returns void because most of these calls are fatal.
func stopSubInterpreter(s subInterpreter, mainState py.PyThreadStatePtr, logger *service.Logger) {
	logger.Debugf("stopping sub-interpreter %d\n", s.id)

	// We should be running from the main Go routine. Load the original
	// Python ThreadState so we can clean up.
	py.PyEval_RestoreThread(s.originalTs)
	py.PyThreadState_Clear(s.originalTs)

	// Clean up the ThreadState. Clear *must* be called before Delete.
	py.PyInterpreterState_Clear(s.state)
	py.PyInterpreterState_Delete(s.state)

	// Restore the original/main ThreadState to assist next invocation.
	py.PyEval_RestoreThread(mainState)

	logger.Debugf("stopped sub-interpreter %d\n", s.id)
}

// Primary "run loop" for main Python interpreter.
//
// Responsible for managing the Python runtime, spawning new sub-interpreters,
// and cleaning up the mess.
//
// All communication to the main interpreter should be done via the to and from channels.
func mainPython(cfg Config, to <-chan request, from chan<- reply, logger *service.Logger) {
	pythonStarted := false
	keepGoing := true

	mainThreadState := py.NullThreadState
	subInterpreters := make([]subInterpreter, 0)

	// We need to stay pinned to the same OS thread as Python's C API heavily
	// makes use of thread local storage. If we let Go reschedule us to a
	// different OS thread, we can corrupt our state and crash.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	for keepGoing {
		msg := <-to
		switch msg {
		case pythonStart:
			if !pythonStarted {
				logger.Info("starting python interpreter")
				var err error
				mainThreadState, err = initPythonOnce(cfg)
				if err != nil {
					keepGoing = false
					logger.Errorf("failed to start python interpreter: %s", err)
				}
				pythonStarted = true
				from <- reply{err: err}
			} else {
				logger.Warn("main interpreter already started")
				from <- reply{err: errors.New("main interpreter already started")}
			}

		case pythonStop:
			if pythonStarted {
				keepGoing = false
				for _, s := range subInterpreters {
					stopSubInterpreter(s, mainThreadState, logger)
				}
				if py.Py_FinalizeEx() != 0 {
					// The chance we get here *without* an explosion is slim, but why not.
					from <- reply{err: errors.New("failed to shutdown python")}
				} else {
					from <- reply{}
				}
			} else {
				logger.Warn("main interpreter not running")
				from <- reply{err: errors.New("main interpreter not running")}
			}

		case pythonSpawn:
			logger.Info("spawning a new sub-interpreter")
			subInterpreter, err := initSubInterpreter(logger)
			if err != nil {
				keepGoing = false
				logger.Warn("failed to create sub-interpreter")
				from <- reply{err: err}
			} else {
				subInterpreters = append(subInterpreters, *subInterpreter)
				from <- reply{state: subInterpreter.state}
			}
		case pythonStatus:
			logger.Debug("main interpreter Go routine is alive")
			from <- reply{}
		}
	}
}

// Initialize the main Python interpreter. (Sub-interpreters are created by
// Processor instances.)
//
// Returns the Python thread state on success.
// On failure, returns a null Python thread state and an error.
func initPythonOnce(cfg Config) (py.PyThreadStatePtr, error) {
	// Find and load the Python dynamic library.
	err := py.Load_library(cfg.Exe)
	if err != nil {
		return py.NullThreadState, err
	}

	// Pre-configure the Python Interpreter. Not 100% necessary, but gives us
	// more control and identifies errors early.
	preConfig := py.PyPreConfig{}
	py.PyPreConfig_InitIsolatedConfig(&preConfig)
	status := py.Py_PreInitialize(&preConfig)
	if status.Type != 0 {
		errMsg := py.PyBytesToString(status.ErrMsg)
		return py.NullThreadState, errors.New(errMsg)
	}

	// Configure our Paths. We need to approximate an isolated pyConfig from a
	// regular config because Python will ignore our modifying some values if
	// we initialize an isolated pyConfig. Annoying!
	pyConfig := py.PyConfig_3_12{}
	py.PyConfig_InitPythonConfig(&pyConfig)
	pyConfig.ParseArgv = 0 // We don't want Python looking at argv.
	pyConfig.SafePath = 0
	pyConfig.UserSiteDirectory = 0
	pyConfig.InstallSignalHandlers = 0 // We don't want Python handling signals.

	// We need to write funky wchar_t strings to our config, so we do a little
	// dance with some helper functions.
	status = py.PyConfig_SetBytesString(&pyConfig, &pyConfig.Home, cfg.Home)
	if status.Type != 0 {
		errMsg := py.PyBytesToString(status.ErrMsg)
		return py.NullThreadState, errors.New(errMsg)
	}
	path := strings.Join(cfg.Paths, ":") // xxx ';' on windows
	status = py.PyConfig_SetBytesString(&pyConfig, &pyConfig.PythonPathEnv, path)
	if status.Type != 0 {
		errMsg := py.PyBytesToString(status.ErrMsg)
		return py.NullThreadState, errors.New(errMsg)
	}
	status = py.Py_InitializeFromConfig(&pyConfig)
	if status.Type != 0 {
		errMsg := py.PyBytesToString(status.ErrMsg)
		return py.NullThreadState, errors.New(errMsg)
	}

	// If we made it here, the main interpreter is started.
	// Save details on our Main thread state and drop GIL.
	return py.PyEval_SaveThread(), nil
}

// Initialize a Sub-interpreter.
//
// Relies on global Python state and being run only from the OS thread that
// manages the runtime. Will potentially panic otherwise.
func initSubInterpreter(logger *service.Logger) (*subInterpreter, error) {
	// Some of these args are required if we want to use Numpy, etc.
	var subStatePtr py.PyThreadStatePtr
	interpreterConfig := py.PyInterpreterConfig{}
	interpreterConfig.Gil = py.DefaultGil            // OwnGil works in 3.12, but is hard to use.
	interpreterConfig.CheckMultiInterpExtensions = 0 // Numpy uses "legacy" extensions.
	interpreterConfig.UseMainObMalloc = 1            // This must be 1 if using "legacy" extensions.
	interpreterConfig.AllowThreads = 1               // Allow using threading library.
	interpreterConfig.AllowDaemonThreads = 0         // Don't allow daemon threads for now.

	// Cross your fingers. This has potential for a fatal (panic) exit.
	status := py.Py_NewInterpreterFromConfig(&subStatePtr, &interpreterConfig)
	if status.Type != 0 {
		errMsg := py.PyBytesToString(status.ErrMsg)
		logger.Errorf("failed to create new sub-interpreter: %s", errMsg)
		return nil, errors.New(errMsg)
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
