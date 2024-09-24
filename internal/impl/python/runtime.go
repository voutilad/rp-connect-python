package python

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"

	py "github.com/voutilad/gogopython"
)

// globalMtx guards the global Python program logic as only a single Python
// instance can be embedded into a process's address space.
var globalMtx *ContextAwareMutex

// pythonLoaded reports whether the Python dynamic libraries have already
// been loaded into the process's address space.
//
// Protected by globalMtx.
var pythonLoaded = false

var pythonStarted = false

// pythonExe contains the unique executable name used for finding the
// currently used Python implementation.
//
// Protected by globalMtx.
var pythonExe = ""

// pythonMain points to the thread-state of the main Python interpreter.
//
// Protected by globalMtx.
var pythonMain py.PyThreadStatePtr

type config struct {
	home  string
	paths []string
}

type moduleRequest struct {
	reply   chan interface{}
	modules []string
}

type fnRequest struct {
	reply chan error
	fn    func() error
}

var consumersCnt = 0                      // Number of current consumers. Protected by globalMtx.
var chanToMain chan *config               // Channel for sending pointers to main go routine.
var chanFromMain chan py.PyThreadStatePtr // Channel for receiving response from main go routine.

var chanDropRefs chan []py.PyObjectPtr
var chanLoadModules chan *moduleRequest
var chanExecFuncs chan *fnRequest

func init() {
	globalMtx = NewContextAwareMutex()
	chanToMain = make(chan *config)
	chanFromMain = make(chan py.PyThreadStatePtr)

	chanDropRefs = make(chan []py.PyObjectPtr)
	chanLoadModules = make(chan *moduleRequest)
	chanExecFuncs = make(chan *fnRequest)
}

// An InterpreterTicket represents ownership of the interpreter of a particular
// Runtime. Other than its id, it's opaque to the user.
type InterpreterTicket struct {
	idx    int     // Index of interpreter (used by the Runtime implementation).
	id     int64   // Python interpreter id.
	cookie uintptr // Optional cookie value (used by the Runtime implementation).
}

// Id provides a unique (to the backing Runtime) identifier for an interpreter.
// The caller may use this for identifying if they're re-using an interpreter.
func (i *InterpreterTicket) Id() int64 {
	return i.id
}

// A Runtime for a Python interpreter.
type Runtime interface {
	// Start the Python runtime.
	Start(ctx context.Context) error

	// Stop the Python runtime, removing all interpreter state.
	Stop(ctx context.Context) error

	// Acquire ownership of an interpreter until Release is called, providing
	// a ticket on success or returning err on error.
	Acquire(ctx context.Context) (ticket *InterpreterTicket, err error)

	// Release ownership of an interpreter identified by the given
	// InterpreterTicket.
	Release(token *InterpreterTicket) error

	// Apply a function f over the interpreter described by the given
	// InterpreterTicket.
	Apply(token *InterpreterTicket, ctx context.Context, f func() error) error

	// Map a function f over all possible interpreters.
	// In the case of multiple interpreters, an error aborts mapping over the
	// remainder.
	Map(ctx context.Context, f func(ticket *InterpreterTicket) error) error
}

// Initialize the main Python interpreter or increment the global count if
// already initialized.
//
// Returns the Python main thread state on success.
// On failure, returns a null PyThreadStatePtr and an error.
//
// Must be called globalMtx and the OS thread locked.
func loadPython(exe, home string, paths []string, ctx context.Context) (py.PyThreadStatePtr, error) {
	globalMtx.AssertLocked()

	// It's ok if we're starting another instance of the same executable, but
	// we don't want to re-load the libraries as we'll crash.
	if exe != pythonExe && pythonLoaded {
		panic("python was already initialized with a different implementation")
		//return py.NullThreadState,
		//	errors.New("python was already initialized with a different implementation")
	}

	// Load our dynamic libraries. This should happen only once per process
	// lifetime.
	if !pythonLoaded {
		err := py.LoadLibrary(exe)
		if err != nil {
			panic(err)
			// return py.NullThreadState, err
		}

		// From now on, we're considered "loaded."
		pythonLoaded = true
		pythonExe = exe
	}

	// Launch the main interpreter.
	launchOnce()

	// Increment our consumer count.
	consumersCnt++

	// If we're the first consumer, we're responsible for kicking it off.
	if consumersCnt == 1 {
		config := &config{
			home:  home,
			paths: paths,
		}
		select {
		case chanToMain <- config:
			// nop
		case <-ctx.Done():
			panic(ctx.Err())
		}

		// Wait for a reply. We hold the global mutex, so should be the only
		// consumer from this channel.
		select {
		case pythonMain = <-chanFromMain:
		// nop
		case <-ctx.Done():
			panic(ctx.Err())
		}
	}

	return pythonMain, nil
}

// unloadPython tears down the global interpreter state.
//
// Must be called with the go routine thread pinned and global mutex locked.
func unloadPython(_ context.Context) error {
	globalMtx.AssertLocked()

	if !pythonLoaded {
		return errors.New("invalid runtime state")
	}

	consumersCnt--
	if consumersCnt == 0 {
		chanToMain <- nil
	}

	return nil
}

func launchOnce() {
	globalMtx.AssertLocked()
	if pythonStarted {
		return
	}
	pythonStarted = true

	// Starting and stopping the Python main interpreter cannot be done from
	// different threads because of some reliance on thread-local storage,
	// it seems. Failure to stay on the same thread can lead to deadlocks.
	//
	// To manage this, we use a long-running Go routine that's pinned and
	// use a WaitGroup to signal time to shut down.
	//
	// Failures in here will panic.
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()

		var objsToDrop []py.PyObjectPtr

		// Outer for-loop governs the high-level interpreter lifecycle.
		for {
			// Block until we are handed a config.
			config := <-chanToMain

			// Pre-configure the Python Interpreter. Not 100% necessary, but gives us
			// more control and identifies errors early.
			preConfig := py.PyPreConfig{}
			py.PyPreConfig_InitIsolatedConfig(&preConfig)
			status := py.Py_PreInitialize(&preConfig)
			if status.Type != 0 {
				msg, _ := py.WCharToString(status.ErrMsg)
				panic(msg)
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
			status = py.PyConfig_SetBytesString(&pyConfig, &pyConfig.Home, config.home)
			if status.Type != 0 {
				msg, _ := py.WCharToString(status.ErrMsg)
				panic(msg)
			}
			path := strings.Join(config.paths, ":") // xxx ';' on windows
			status = py.PyConfig_SetBytesString(&pyConfig, &pyConfig.PythonPathEnv, path)
			if status.Type != 0 {
				msg, _ := py.WCharToString(status.ErrMsg)
				panic(msg)
			}

			// Start up the main interpreter.
			status = py.Py_InitializeFromConfig(&pyConfig)
			if status.Type != 0 {
				msg, _ := py.WCharToString(status.ErrMsg)
				panic(msg)
			}

			// If we made it here, the main interpreter is started.
			// Drop GIL and send back some details on our main thread.
			ts := py.PyEval_SaveThread()
			if ts == py.NullThreadState {
				panic("main thread state is null")
			}
			chanFromMain <- ts

			// Inner for-loop responds to events from callers.
			keepRunning := true
			for keepRunning {
				select {
				case <-chanToMain:
					// No consumers remain, so time to shut down the interpreter.
					keepRunning = false
				case objs := <-chanDropRefs:
					// Drop a reference to a global Python object.
					_ = globalMtx.Lock()
					py.PyEval_RestoreThread(ts)
					for _, obj := range objs {
						py.Py_DecRef(obj)
					}
					ts = py.PyEval_SaveThread()
					globalMtx.Unlock()
				case req := <-chanLoadModules:
					// Load requested module(s) into the global interpreter.
					_ = globalMtx.Lock()
					py.PyEval_RestoreThread(ts)

					for _, mod := range req.modules {
						m := py.PyImport_ImportModule(mod)
						if m == py.NullPyObjectPtr {
							// XXX panic here or what?
							panic(fmt.Sprintf("failed to import module %s", mod))
						}
						objsToDrop = append(objsToDrop, m)
					}

					ts = py.PyEval_SaveThread()
					globalMtx.Unlock()

					req.reply <- nil
				case req := <-chanExecFuncs:
					// XXX Hack to execute functions on the "main" go routine.
					//     Wastes cycles dancing with locks and thread states.
					py.PyEval_RestoreThread(ts)
					err := req.fn()
					ts = py.PyEval_SaveThread()
					req.reply <- err
				}
			}

			// Finalize Python. This has potential to deadlock or panic!
			py.PyEval_RestoreThread(ts)
			if py.Py_FinalizeEx() != 0 {
				panic("failed to finalize Python runtime")
			}
		}
	}()
}

func DropGlobalReferences(objs []py.PyObjectPtr, ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case chanDropRefs <- objs:
		return nil
	}
}

func LoadModules(modules []string, ctx context.Context) error {
	request := moduleRequest{
		reply:   make(chan interface{}),
		modules: modules,
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case chanLoadModules <- &request:
		// Wait for a reply. We hold the global mutex, so should be the only
		// consumer from this channel.
		select {
		case <-request.reply:
			return nil
		case <-ctx.Done():
			panic(ctx.Err())
		}
	}
}

func Evaluate(fn func() error, ctx context.Context) error {
	// XXX This is all a hack currently and needs optimization.
	globalMtx.AssertLocked()

	request := fnRequest{
		reply: make(chan error),
		fn:    fn,
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case chanExecFuncs <- &request:
		// Wait for a reply.
		select {
		case err := <-request.reply:
			return err
		case <-ctx.Done():
			panic(ctx.Err())
		}
	}
}
