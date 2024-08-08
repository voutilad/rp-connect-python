package python

import (
	"context"
	"errors"
	"strings"

	py "github.com/voutilad/gogopython"
)

// globalMtx guards the global Python program logic as only a single Python
// instance can be embedded into a process's address space.
var globalMtx *ContextAwareMutex

// pythonWasLoaded reports whether the Python dynamic libraries have already
// been loaded into the process's address space.
var pythonWasLoaded = false

// pythonExe contains the unique executable name used for finding the
// currently used Python implementation.
var pythonExe = ""

// pythonMain points to the thread-state of the main Python interpreter.
var pythonMain py.PyThreadStatePtr

// numRuntimes tracks the number of Runtime instances launched to identify if
// one stopping should tear down the main Python interpreter.
var numRuntimes = 0

func init() {
	globalMtx = NewContextAwareMutex()
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
func loadPython(exe, home string, paths []string) (py.PyThreadStatePtr, error) {
	globalMtx.AssertLocked()

	// It's ok if we're starting another instance of the same executable, but
	// we don't want to re-load the libraries as we'll crash.
	if exe != pythonExe && pythonWasLoaded {
		return py.NullThreadState, errors.New("python was already initialized")
	} else if pythonWasLoaded {
		numRuntimes++
		return pythonMain, nil
	}

	// Load our dynamic libraries.
	err := py.Load_library(exe)
	if err != nil {
		return py.NullThreadState, err
	}

	// Pre-configure the Python Interpreter. Not 100% necessary, but gives us
	// more control and identifies errors early.
	preConfig := py.PyPreConfig{}
	py.PyPreConfig_InitIsolatedConfig(&preConfig)
	status := py.Py_PreInitialize(&preConfig)
	if status.Type != 0 {
		msg, _ := py.WCharToString(status.ErrMsg)
		return py.NullThreadState, errors.New(msg)
	}

	// From now on, we're considered "loaded."
	pythonWasLoaded = true
	pythonExe = exe
	numRuntimes++

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
	status = py.PyConfig_SetBytesString(&pyConfig, &pyConfig.Home, home)
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
	pythonMain = py.PyEval_SaveThread()
	return pythonMain, nil
}

// unloadPython tears down the global interpreter state.
//
// Must be called with the go routine thread pinned and global mutex locked.
func unloadPython(mainThread py.PyThreadStatePtr) error {
	globalMtx.AssertLocked()

	if !pythonWasLoaded || numRuntimes < 1 {
		return errors.New("invalid runtime state")
	}

	numRuntimes--

	if numRuntimes == 0 {
		py.PyEval_RestoreThread(mainThread)
		if py.Py_FinalizeEx() != 0 {
			return errors.New("failed to finalize Python runtime")
		}
	}
	return nil
}
