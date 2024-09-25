package python

import (
	"context"
	"errors"
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

type fnRequest struct {
	reply chan error
	fn    func() error
}

type subRequest struct {
	legacyMode bool
	reply      chan *subReply
}

type subReply struct {
	err            error
	subInterpreter *SubInterpreter
}

type subStopRequest struct {
	reply          chan error
	subInterpreter *SubInterpreter
}

var consumersCnt = 0                      // Number of current consumers. Protected by globalMtx.
var chanToMain chan *config               // Channel for sending pointers to main go routine.
var chanFromMain chan py.PyThreadStatePtr // Channel for receiving response from main go routine.

var chanDropRefs chan []py.PyObjectPtr
var chanExecFuncs chan *fnRequest
var chanSpawnSub chan *subRequest
var chanStopSub chan *subStopRequest

func init() {
	globalMtx = NewContextAwareMutex()
	chanToMain = make(chan *config)
	chanFromMain = make(chan py.PyThreadStatePtr)

	chanDropRefs = make(chan []py.PyObjectPtr)
	chanExecFuncs = make(chan *fnRequest)
	chanSpawnSub = make(chan *subRequest)
	chanStopSub = make(chan *subStopRequest)
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
func loadPython(exe, home string, paths []string, ctx context.Context) {
	globalMtx.AssertLocked()

	// It's ok if we're starting another instance of the same executable, but
	// we don't want to re-load the libraries as we'll crash.
	if exe != pythonExe && pythonLoaded {
		panic("python was already initialized with a different implementation")
	}

	// Load our dynamic libraries. This should happen only once per process
	// lifetime.
	if !pythonLoaded {
		err := py.LoadLibrary(exe)
		if err != nil {
			panic(err)
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
}

// unloadPython tears down the global interpreter state.
//
// Must be called with the global mutex locked.
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
					py.PyEval_RestoreThread(ts)
					for _, obj := range objs {
						py.Py_DecRef(obj)
					}
					py.PyEval_SaveThread()

				case req := <-chanExecFuncs:
					// XXX Hack to execute functions on the "main" go routine.
					//     Wastes cycles dancing with locks and thread states.
					py.PyEval_RestoreThread(ts)
					result := req.fn()
					py.PyEval_SaveThread()
					req.reply <- result

				case req := <-chanSpawnSub:
					py.PyEval_RestoreThread(ts)
					sub, err := initSubInterpreter(req.legacyMode)
					// XXX No need to save here.
					req.reply <- &subReply{
						err:            err,
						subInterpreter: sub,
					}

				case req := <-chanStopSub:
					// Restore the sub-interpreter thread state.
					sub := req.subInterpreter
					py.PyEval_RestoreThread(sub.Thread)
					py.PyThreadState_Clear(sub.Thread)

					// Clean up the ThreadState. Clear *must* be called before Delete.
					py.PyInterpreterState_Clear(sub.State)
					py.PyInterpreterState_Delete(sub.State)
					req.reply <- nil
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

// DropGlobalReferences to a list of PyObjectPtr.
func DropGlobalReferences(objs []py.PyObjectPtr, ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case chanDropRefs <- objs:
		return nil
	}
}

// Evaluate a given function fn in the context of the main interpreter.
// A response is provided via the channel c.
//
// XXX This may look a little odd, both returning an error and using a
// channel, but it's to avoid having to allocate a channel for each call
// as this function will be called frequently.
func Evaluate(fn func() error, c chan error, ctx context.Context) error {
	request := fnRequest{
		reply: c,
		fn:    fn,
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case chanExecFuncs <- &request:
		// Wait for a reply.
		select {
		case err := <-c:
			return err
		case <-ctx.Done():
			panic(ctx.Err())
		}
	}
}

// Spawn a new sub-interpreter.
func Spawn(legacyMode bool, ctx context.Context) (*SubInterpreter, error) {
	request := subRequest{
		reply:      make(chan *subReply),
		legacyMode: legacyMode,
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case chanSpawnSub <- &request:
		select {
		case result := <-request.reply:
			return result.subInterpreter, result.err
		case <-ctx.Done():
			panic(ctx.Err())
		}
	}
}

// StopSub will attempt to shut down a sub-interpreter.
func StopSub(subInterpreter *SubInterpreter, ctx context.Context) error {
	request := subStopRequest{
		subInterpreter: subInterpreter,
		reply:          make(chan error),
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case chanStopSub <- &request:
		select {
		case err := <-request.reply:
			return err
		case <-ctx.Done():
			panic(ctx.Err())
		}
	}
}

// Initialize a Sub-interpreter.
//
// Caller must have the main interpreter state loaded and Go routine pinned.
// This must be called from the context of the "main" interpreter.
func initSubInterpreter(legacyMode bool) (*SubInterpreter, error) {
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
		return nil, errors.New(msg)
	}

	// Collect our information and drop the GIL.
	state := py.PyInterpreterState_Get()
	id := py.PyInterpreterState_GetID(state)
	ts = py.PyEval_SaveThread()

	return &SubInterpreter{
		State:  state,
		Thread: ts,
		Id:     id,
	}, nil
}
