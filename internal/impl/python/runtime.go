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

// Request the Python runtime Go routine perform an action.
type request int

const (
	pythonStart       request = iota // Start the runtime.
	pythonStartLegacy                // Start the runtime, in legacy mode.
	pythonStatus                     // Are you alive?
	pythonStop                       // Stop and shutdown sub-interpreters.
	pythonSpawn                      // Span a new sub-interpreter.
)

// Reply from the Python runtime Go routine in response to a Request.
type reply struct {
	err         error
	interpreter subInterpreter
}

// State related to a Python Sub-interpreter.
type subInterpreter struct {
	state  py.PyInterpreterStatePtr // Interpreter State.
	thread py.PyThreadStatePtr      // Original Python ThreadState.
	id     int64                    // Unique identifier.
}

type Runtime struct {
	exe        string          // Python exe (binary).
	home       string          // Python home.
	paths      []string        // Python package paths.
	from       chan reply      // protected by mtx.
	to         chan request    // protected by mtx.
	mtx        sync.Mutex      // Mutex to protect channels ordering.
	ctx        context.Context // Context to help cancel the runtime.
	cancel     func()          // Cancellation function.
	started    atomic.Bool     // Flag to signal if the Go routine is running.
	legacyMode bool            // Run in legacy mode? (e.g. for NumPy)
}

// New Runtime instance from the given Python executable.
//
// Will fail if it cannot detect Python home and path settings or the Python
// executable cannot be found.
func New(exe string) (*Runtime, error) {
	home, paths, err := py.FindPythonHomeAndPaths(exe)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())

	return &Runtime{
		exe:    exe,
		home:   home,
		paths:  paths,
		from:   make(chan reply),
		to:     make(chan request),
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

// Start the Python runtime.
//
// This is idempotent. Multiple calls to Start will do nothing and return nil.
func (r *Runtime) Start(ctx context.Context, logger *service.Logger, legacyMode bool) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	var startMode request
	if legacyMode {
		startMode = pythonStartLegacy
	} else {
		startMode = pythonStart
	}

	if r.started.CompareAndSwap(false, true) {
		go r.mainPython(logger)

		// Tell it to start.
		r.to <- startMode
		select {
		case reply := <-r.from:
			if reply.err != nil {
				return reply.err
			}
		case <-ctx.Done():
			return errors.New("interrupted")
		}
	} else {
		// Already started. Just check status.
		r.to <- pythonStatus
		select {
		case reply := <-r.from:
			if reply.err != nil {
				return reply.err
			}
		case <-ctx.Done():
			return errors.New("interrupted")
		}
	}
	return nil
}

// Stop a running Python Runtime.
func (r *Runtime) Stop(ctx context.Context) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.to <- pythonStop
	select {
	case reply := <-r.from:
		if reply.err != nil {
			return reply.err
		}
	case <-ctx.Done():
		return errors.New("interrupted")
	}
	return nil
}

// Spawn a new sub-interpreter from the Runtime.
func (r *Runtime) Spawn(ctx context.Context) (py.PyInterpreterStatePtr, py.PyThreadStatePtr, error) {
	// Tell it to spawn a new sub-interpreter.
	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.to <- pythonSpawn
	select {
	case rep := <-r.from:
		if rep.err != nil {
			return py.NullInterpreterState, py.NullThreadState, rep.err
		} else {
			return rep.interpreter.state, rep.interpreter.thread, nil
		}
	case <-ctx.Done():
		return py.NullInterpreterState, py.NullThreadState, errors.New("interrupted")
	}
}

// Teardown a Sub-Interpreter and delete its state. This will probably trigger a lot of Python
// cleanup under the hood.
//
// Note: This returns void because most of these calls are fatal.
func stopSubInterpreter(s subInterpreter, logger *service.Logger) {
	logger.Tracef("Stopping sub-interpreter %d\n", s.id)

	// We should be running from the main Go routine. Load the original
	// Python ThreadState so we can clean up.
	py.PyEval_RestoreThread(s.thread)
	py.PyThreadState_Clear(s.thread)

	// Clean up the ThreadState. Clear *must* be called before Delete.
	py.PyInterpreterState_Clear(s.state)
	py.PyInterpreterState_Delete(s.state)

	logger.Tracef("Stopped sub-interpreter %d\n", s.id)
}

// Primary "run loop" for main Python interpreter.
//
// Responsible for managing the Python runtime, spawning new sub-interpreters,
// and cleaning up the mess.
//
// All communication to the main interpreter should be done via the to and from channels.
func (r *Runtime) mainPython(logger *service.Logger) {
	pythonStarted := false
	keepGoing := true

	mainThreadState := py.NullThreadState
	subInterpreters := make([]subInterpreter, 0)

	// Just a guard rail.
	initPythonOnce := sync.OnceValues(func() (py.PyThreadStatePtr, error) {
		return initPython(r.exe, r.home, r.paths)
	})

	// We need to stay pinned to the same OS thread as Python's C API heavily
	// makes use of thread local storage. If we let Go reschedule us to a
	// different OS thread, we can corrupt our state and crash.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	for keepGoing {
		msg := <-r.to
		switch msg {
		case pythonStartLegacy:
			fallthrough
		case pythonStart:
			if !pythonStarted {
				logger.Info("Starting python interpreter.")
				var err error
				mainThreadState, err = initPythonOnce()
				if err != nil {
					keepGoing = false
					logger.Errorf("Failed to start python interpreter: %s", err)
				}
				pythonStarted = true
				r.from <- reply{err: err}
			} else {
				logger.Warn("Main interpreter already started")
				r.from <- reply{err: errors.New("main interpreter already started")}
			}

		case pythonStop:
			if pythonStarted {
				keepGoing = false
				for _, s := range subInterpreters {
					stopSubInterpreter(s, logger)
				}
				py.PyEval_RestoreThread(mainThreadState)
				logger.Trace("Tearing down Python")
				if py.Py_FinalizeEx() != 0 {
					// The chance we get here *without* an explosion is slim, but why not.
					r.from <- reply{err: errors.New("failed to shutdown python")}
				} else {
					logger.Trace("Python stopped")
					r.from <- reply{}
				}
			} else {
				logger.Warn("Main interpreter not running")
				r.from <- reply{err: errors.New("main interpreter not running")}
			}

		case pythonSpawn:
			if pythonStarted {
				logger.Trace("Spawning a new sub-interpreter.")
				sub, err := initSubInterpreter(r.legacyMode, logger)
				if err != nil {
					keepGoing = false
					logger.Warn("Failed to create sub-interpreter.")
					r.from <- reply{err: err}
				} else {
					subInterpreters = append(subInterpreters, *sub)
					r.from <- reply{interpreter: *sub}
				}
			} else {
				logger.Warn("Main interpreter not running.")
				r.from <- reply{err: errors.New("main interpreter not running")}
			}

		case pythonStatus:
			logger.Debug("Main interpreter Go routine is alive.")
			r.from <- reply{}
		}
	}
}

// Load the Python runtime libraries and pre-initialize the environment.
func loadPython(exe string) error {
	// Find and load the Python dynamic library.
	err := py.Load_library(exe)
	if err != nil {
		return err
	}
	// Pre-configure the Python Interpreter. Not 100% necessary, but gives us
	// more control and identifies errors early.
	preConfig := py.PyPreConfig{}
	py.PyPreConfig_InitIsolatedConfig(&preConfig)
	status := py.Py_PreInitialize(&preConfig)
	if status.Type != 0 {
		msg, _ := py.WCharToString(status.ErrMsg)
		return errors.New(msg)
	}
	return nil
}

// Initialize the main Python interpreter. (Sub-interpreters are created by
// Processor instances.) Should only be called once!
//
// Returns the Python thread state on success.
// On failure, returns a null Python thread state and an error.
func initPython(exe, home string, paths []string) (py.PyThreadStatePtr, error) {
	// Load our dynamic libraries.
	err := loadPython(exe)
	if err != nil {
		return py.NullThreadState, err
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
	status := py.PyConfig_SetBytesString(&pyConfig, &pyConfig.Home, home)
	if status.Type != 0 {
		msg, _ := py.WCharToString(status.ErrMsg)
		return py.NullThreadState, errors.New(msg)
	}
	path := strings.Join(paths, ":") // xxx ';' on windows
	status = py.PyConfig_SetBytesString(&pyConfig, &pyConfig.PythonPathEnv, path)
	if status.Type != 0 {
		msg, _ := py.WCharToString(status.ErrMsg)
		return py.NullThreadState, errors.New(msg)
	}
	status = py.Py_InitializeFromConfig(&pyConfig)
	if status.Type != 0 {
		msg, _ := py.WCharToString(status.ErrMsg)
		return py.NullThreadState, errors.New(msg)
	}

	// If we made it here, the main interpreter is started.
	// Save details on our Main thread state and drop GIL.
	return py.PyEval_SaveThread(), nil
}

// Initialize a Sub-interpreter.
//
// Relies on global Python state and being run only from the OS thread that
// manages the runtime. Will potentially panic otherwise.
func initSubInterpreter(legacyMode bool, logger *service.Logger) (*subInterpreter, error) {
	// Some of these args are required if we want to use Numpy, etc.
	var ts py.PyThreadStatePtr
	interpreterConfig := py.PyInterpreterConfig{}

	if !legacyMode {
		interpreterConfig.Gil = py.OwnGil
		interpreterConfig.CheckMultiInterpExtensions = 1
		interpreterConfig.UseMainObMalloc = 0
		interpreterConfig.AllowThreads = 1       // Allow using threading library.
		interpreterConfig.AllowDaemonThreads = 0 // Don't allow daemon threads for now.
	} else {
		interpreterConfig.Gil = py.SharedGil
		interpreterConfig.CheckMultiInterpExtensions = 0 // Numpy uses "legacy" extensions.
		interpreterConfig.UseMainObMalloc = 1            // This must be 1 if using "legacy" extensions.
		interpreterConfig.AllowThreads = 1               // Allow using threading library.
		interpreterConfig.AllowDaemonThreads = 0         // Don't allow daemon threads for now.
	}

	// Cross your fingers. This has potential for a fatal (panic) exit.
	status := py.Py_NewInterpreterFromConfig(&ts, &interpreterConfig)
	if status.Type != 0 {
		msg, _ := py.WCharToString(status.ErrMsg)
		logger.Errorf("Failed to create new sub-interpreter: %s", msg)
		return nil, errors.New(msg)
	}

	// Collect our information and drop the GIL.
	state := py.PyInterpreterState_Get()
	id := py.PyInterpreterState_GetID(state)
	py.PyEval_SaveThread()

	logger.Tracef("Initialized sub-interpreter %d.\n", id)

	return &subInterpreter{
		state:  state,
		thread: ts,
		id:     id,
	}, nil
}
