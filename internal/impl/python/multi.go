package python

import (
	"errors"
	"github.com/redpanda-data/benthos/v4/public/service"
	py "github.com/voutilad/gogopython"
	"golang.org/x/net/context"
	"runtime"
	"sync"
	"sync/atomic"
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
	interpreter *subInterpreter
	main        py.PyThreadStatePtr
}

// MultiInterpreterRuntime creates and manages multiple Python sub-interpreters.
type MultiInterpreterRuntime struct {
	exe    string              // Python exe (binary).
	home   string              // Python home.
	paths  []string            // Python package paths.
	thread py.PyThreadStatePtr // Main interpreter thread state.

	interpreters []*subInterpreter     // Sub-interpreters.
	tickets      chan InterpreterToken // Tickets for sub-interpreters.

	from chan reply   // protected by mtx.
	to   chan request // protected by mtx.
	mtx  sync.Mutex   // Mutex to write protect the runtime state transitions.

	started    atomic.Bool // Flag to signal if the Go routine is running.
	legacyMode bool        // Run in legacy mode?
	logger     *service.Logger
}

// State related to a Python Sub-interpreter.
type subInterpreter struct {
	state  py.PyInterpreterStatePtr // Interpreter State.
	thread py.PyThreadStatePtr      // Original Python ThreadState.
	id     int64                    // Unique identifier.
}

func NewMultiInterpreterRuntime(exe string, cnt int, legacyMode bool, logger *service.Logger) (*MultiInterpreterRuntime, error) {
	home, paths, err := py.FindPythonHomeAndPaths(exe)
	if err != nil {
		return nil, err
	}

	return &MultiInterpreterRuntime{
		exe:          exe,
		home:         home,
		paths:        paths,
		from:         make(chan reply),
		to:           make(chan request),
		interpreters: make([]*subInterpreter, cnt),
		tickets:      make(chan InterpreterToken, cnt),
		legacyMode:   legacyMode,
		logger:       logger,
	}, nil
}

// Start the Python runtime. A MultiInterpreterRuntime centralizes modification
// of the main interpreter in a go routine.
func (r *MultiInterpreterRuntime) Start() error {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	var startMode request
	if r.legacyMode {
		startMode = pythonStartLegacy
	} else {
		startMode = pythonStart
	}

	// We don't use CAS here as we're
	if !r.started.Load() {
		// Launch our main interpreter go routine.
		go r.mainPython()

		// Ask the main interpreter to start things up.
		r.to <- startMode
		response := <-r.from
		if response.err != nil {
			return response.err
		}
		r.started.Store(true)
		r.logger.Debug("Main Python interpreter started.")

		// Start up sub-interpreters.
		for idx := range len(r.interpreters) {
			r.to <- pythonSpawn
			response = <-r.from
			if response.err != nil {
				return response.err
			}

			// Populate our ticket booth and interpreter list.
			r.interpreters[idx] = response.interpreter
			r.tickets <- InterpreterToken{idx: idx, id: response.interpreter.id}
		}
		r.logger.Debugf("Started %d sub-interpreters.", len(r.interpreters))
	}
	return nil
}

// Stop a running Python Runtime.
func (r *MultiInterpreterRuntime) Stop() error {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	if !r.started.Load() {
		return errors.New("not started")
	}

	// Ask main go routine to stop.
	r.to <- pythonStop
	response := <-r.from
	if response.err != nil {
		return response.err
	}
	return nil
}

func (r *MultiInterpreterRuntime) Acquire(ctx context.Context) (InterpreterToken, error) {
	if !r.started.Load() {
		return InterpreterToken{}, errors.New("not started")
	}

	select {
	case token := <-r.tickets:
		// We're now operating in the context of an interpreter. We need to
		// pin to our OS thread.
		runtime.LockOSThread()
		interpreter := r.interpreters[token.idx]
		py.PyEval_RestoreThread(interpreter.thread)
		return token, nil

	case <-ctx.Done():
		return InterpreterToken{}, ctx.Err()
	}
}

func (r *MultiInterpreterRuntime) Release(token InterpreterToken) error {
	if !r.started.Load() {
		return errors.New("not started")
	}

	// Double-check the token is valid.
	if token.idx < 0 || token.idx > len(r.interpreters) {
		return errors.New("invalid token: bad index")
	}

	// Release our thread state and unpin thread.
	py.PyEval_SaveThread()
	runtime.UnlockOSThread()

	r.tickets <- token
	return nil
}

func (r *MultiInterpreterRuntime) Destroy(token InterpreterToken) error {
	if !r.started.Load() {
		return errors.New("not started")
	}

	// Double-check the token is valid.
	if token.idx < 0 || token.idx > len(r.interpreters) {
		return errors.New("invalid token: bad index")
	}
	interpreter := r.interpreters[token.idx]

	// Restore our thread-state and then nuke the world.
	py.PyEval_RestoreThread(interpreter.thread)
	py.PyThreadState_Clear(interpreter.thread)
	py.PyInterpreterState_Clear(interpreter.state)
	py.PyInterpreterState_Delete(interpreter.state)
	runtime.UnlockOSThread()
	r.logger.Tracef("Destroyed interpreter %d (thread state 0x%x)\n",
		interpreter.id, interpreter.thread)

	// We don't return the token to the ticket booth.
	return nil
}

// Map a function fn over all the interpreters, one at a time. Useful for
// initializing all interpreters to a given state.
func (r *MultiInterpreterRuntime) Map(f func(token InterpreterToken) error) error {
	if !r.started.Load() {
		return errors.New("not started")
	}

	// Lock our interpreter and pin our thread.
	r.mtx.Lock()
	runtime.LockOSThread()

	// Apply our function.
	var err error
	for idx, sub := range r.interpreters {
		py.PyEval_RestoreThread(sub.thread)
		err = f(InterpreterToken{idx: idx, id: sub.id})
		py.PyEval_SaveThread()
		if err != nil {
			break
		}
	}

	// Unpin and unlock.
	runtime.UnlockOSThread()
	r.mtx.Unlock()

	return err
}

// Teardown a Sub-Interpreter and delete its state. This will probably trigger a lot of Python
// cleanup under the hood.
//
// Note: This returns void because most of these calls are fatal.
func stopSubInterpreter(s *subInterpreter, logger *service.Logger) {
	logger.Debugf("Stopping sub-interpreter %d\n", s.id)

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
func (r *MultiInterpreterRuntime) mainPython() {
	pythonStarted := false
	keepGoing := true

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
				r.logger.Info("Starting python interpreter.")
				var err error
				r.thread, err = initPythonOnce()
				if err != nil {
					keepGoing = false
					r.logger.Errorf("Failed to start python interpreter: %s", err)
				}
				pythonStarted = true
				r.from <- reply{err: err, main: r.thread}
			} else {
				r.logger.Warn("Main interpreter already started")
				r.from <- reply{err: errors.New("main interpreter already started")}
			}

		case pythonStop:
			if pythonStarted {
				keepGoing = false
				for _, s := range r.interpreters {
					stopSubInterpreter(s, r.logger)
				}
				py.PyEval_RestoreThread(r.thread)
				r.logger.Trace("Tearing down Python")
				if py.Py_FinalizeEx() != 0 {
					// The chance we get here *without* an explosion is slim, but why not.
					r.from <- reply{err: errors.New("failed to shutdown python")}
				} else {
					r.logger.Trace("Python stopped")
					r.from <- reply{}
				}
			} else {
				r.logger.Warn("Main interpreter not running")
				r.from <- reply{err: errors.New("main interpreter not running")}
			}

		case pythonSpawn:
			if pythonStarted {
				r.logger.Trace("Spawning a new sub-interpreter.")
				sub, err := r.initSubInterpreter(r.logger)
				if err != nil {
					keepGoing = false
					r.logger.Warn("Failed to create sub-interpreter.")
					r.from <- reply{err: err}
				} else {
					r.from <- reply{interpreter: sub}
				}
			} else {
				r.logger.Warn("Main interpreter not running.")
				r.from <- reply{err: errors.New("main interpreter not running")}
			}

		case pythonStatus:
			r.logger.Debug("Main interpreter Go routine is alive.")
			r.from <- reply{}
		}
	}
}

// Initialize a Sub-interpreter.
//
// Relies on global Python state and being run only from the OS thread that
// manages the runtime. Will potentially panic otherwise.
func (r *MultiInterpreterRuntime) initSubInterpreter(logger *service.Logger) (*subInterpreter, error) {
	// Some of these args are required if we want to use Numpy, etc.
	var ts py.PyThreadStatePtr
	interpreterConfig := py.PyInterpreterConfig{}

	if !r.legacyMode {
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
