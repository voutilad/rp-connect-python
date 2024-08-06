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

	interpreters []*subInterpreter      // Sub-interpreters.
	tickets      chan *InterpreterToken // Tickets for sub-interpreters.

	from    chan reply   // protected by mtx.
	to      chan request // protected by mtx.
	mtxChan chan int     // Mutex to write protect the runtime state transitions.

	started    atomic.Bool     // Flag to signal if the Go routine is running.
	legacyMode bool            // Running in legacy mode?
	logger     *service.Logger // Redpanda Connect logger service.
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
		mtxChan:      make(chan int, 1),
		interpreters: make([]*subInterpreter, cnt),
		tickets:      make(chan *InterpreterToken, cnt),
		legacyMode:   legacyMode,
		logger:       logger,
	}, nil
}

func (r *MultiInterpreterRuntime) lock(ctx context.Context) error {
	select {
	case r.mtxChan <- 1:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (r *MultiInterpreterRuntime) unlock() {
	<-r.mtxChan
}

// Start the Python runtime. A MultiInterpreterRuntime centralizes modification
// of the main interpreter in a go routine.
func (r *MultiInterpreterRuntime) Start(ctx context.Context) error {
	err := r.lock(ctx)
	if err != nil {
		return err
	}
	defer r.unlock()

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
		r.logger.Debug("Python interpreter started.")

		// Start up sub-interpreters.
		for idx := range len(r.interpreters) {
			r.to <- pythonSpawn
			response = <-r.from
			if response.err != nil {
				return response.err
			}

			// Populate our ticket booth and interpreter list.
			r.interpreters[idx] = response.interpreter
			r.tickets <- &InterpreterToken{idx: idx, id: response.interpreter.id}
		}
		r.logger.Debugf("Started %d sub-interpreters.", len(r.interpreters))
	}
	return nil
}

// Stop a running Python Runtime.
func (r *MultiInterpreterRuntime) Stop(ctx context.Context) error {
	err := r.lock(ctx)
	if err != nil {
		return err
	}
	defer r.unlock()

	if !r.started.Load() {
		return errors.New("not started")
	}

	// Ask main go routine to stop.
	r.to <- pythonStop
	response := <-r.from
	if response.err != nil {
		return response.err
	}
	r.logger.Debug("Python interpreter stopped.")

	return nil
}

func (r *MultiInterpreterRuntime) Acquire(ctx context.Context) (*InterpreterToken, error) {
	if !r.started.Load() {
		return nil, errors.New("not started")
	}

	select {
	case token := <-r.tickets:
		return token, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (r *MultiInterpreterRuntime) Release(token *InterpreterToken) error {
	if !r.started.Load() {
		return errors.New("not started")
	}

	// Double-check the token is valid.
	if token.idx < 0 || token.idx > len(r.interpreters) {
		return errors.New("invalid token: bad index")
	}

	r.tickets <- token
	return nil
}

func (r *MultiInterpreterRuntime) Destroy(token *InterpreterToken) error {
	if !r.started.Load() {
		return errors.New("not started")
	}

	// Double-check the token is valid.
	if token.idx < 0 || token.idx > len(r.interpreters) {
		return errors.New("invalid token: bad index")
	}
	interpreter := r.interpreters[token.idx]
	if interpreter.id != token.id {
		return errors.New("invalid token: bad interpreter id")
	}

	// Restore our thread-state and then nuke the world.
	py.PyEval_RestoreThread(interpreter.thread)
	py.PyThreadState_Clear(interpreter.thread)
	py.PyInterpreterState_Clear(interpreter.state)
	py.PyInterpreterState_Delete(interpreter.state)
	runtime.UnlockOSThread()

	r.logger.Tracef("Destroyed sub-interpreter %d (thread state 0x%x)\n",
		interpreter.id, interpreter.thread)

	// We don't return the token to the ticket booth.
	return nil
}

func (r *MultiInterpreterRuntime) Apply(token *InterpreterToken, ctx context.Context, f func() error) error {
	// Double-check the token is valid.
	if token.idx < 0 || token.idx > len(r.interpreters) {
		return errors.New("invalid token: bad index")
	}

	interpreter := r.interpreters[token.idx]
	if interpreter.id != token.id {
		return errors.New("invalid token: bad interpreter id")
	}

	// Pin our go routine & enter the context of the interpreter thread state.
	runtime.LockOSThread()
	py.PyEval_RestoreThread(interpreter.thread)

	err := f()

	// Release our thread state and unpin thread.
	py.PyEval_SaveThread()
	runtime.UnlockOSThread()

	return err
}

// Map a function fn over all the interpreters, one at a time. Useful for
// initializing all interpreters to a given state.
func (r *MultiInterpreterRuntime) Map(ctx context.Context, f func(token *InterpreterToken) error) error {
	if !r.started.Load() {
		return errors.New("not started")
	}

	// Lock our interpreter. We need sole control.
	err := r.lock(ctx)
	if err != nil {
		return err
	}
	defer r.unlock()

	// Acquire all tickets since we don't protect the ticket channel with the
	// runtime mutex.
	tickets := make([]*InterpreterToken, len(r.tickets))
	defer func() {
		for _, token := range tickets {
			if token != nil {
				_ = r.Release(token)
			}
		}
	}()
	for idx := range tickets {
		ticket, err := r.Acquire(ctx)
		if err != nil {
			return err
		}
		tickets[idx] = ticket
	}

	// We should own all tickets and the runtime. Now pin our go routine and
	// apply the function to all interpreters. We bail on failure.
	runtime.LockOSThread()
	for _, ticket := range tickets {
		sub := r.interpreters[ticket.idx]
		py.PyEval_RestoreThread(sub.thread)
		err = f(ticket)
		py.PyEval_SaveThread()
		if err != nil {
			break
		}
	}
	runtime.UnlockOSThread()

	return err
}

// Teardown a Sub-Interpreter and delete its state. This will probably trigger a lot of Python
// cleanup under the hood.
//
// Note: This returns void because most of these calls are fatal.
func stopSubInterpreter(s *subInterpreter, logger *service.Logger) {
	logger.Tracef("Stopping sub-interpreter %d.\n", s.id)

	// We should be running from the main Go routine. Load the original
	// Python ThreadState so we can clean up.
	py.PyEval_RestoreThread(s.thread)
	py.PyThreadState_Clear(s.thread)

	// Clean up the ThreadState. Clear *must* be called before Delete.
	py.PyInterpreterState_Clear(s.state)
	py.PyInterpreterState_Delete(s.state)

	logger.Tracef("Stopped sub-interpreter %d.\n", s.id)
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
				r.logger.Info("Starting Python interpreter.")
				var err error
				r.thread, err = initPythonOnce()
				if err != nil {
					keepGoing = false
					r.logger.Errorf("Failed to start Python interpreter: %s", err)
				}
				pythonStarted = true
				r.from <- reply{err: err, main: r.thread}
			} else {
				r.logger.Warn("Interpreter already started")
				r.from <- reply{err: errors.New("main interpreter already started")}
			}

		case pythonStop:
			if pythonStarted {
				// No more run loop.
				keepGoing = false

				// Do we have any sub-interpreters running? If so, we need to stop them.
				for _, s := range r.interpreters {
					stopSubInterpreter(s, r.logger)
				}

				// Reload the main thread state so we can exit Python.
				err := unloadPython(r.thread)
				if err != nil {
					// The chance we get here *without* an explosion is slim, but why not.
					r.from <- reply{err: errors.New("failed to shutdown python")}
				} else {
					r.logger.Trace("Python stopped")
					r.from <- reply{}
				}
			} else {
				r.logger.Warn("Interpreter not running")
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
				r.logger.Warn("Interpreter not running.")
				r.from <- reply{err: errors.New("interpreter not running")}
			}

		case pythonStatus:
			r.logger.Debug("Interpreter Go routine is alive.")
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
