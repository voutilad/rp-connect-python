package python

import (
	"context"
	"errors"
	py "github.com/voutilad/gogopython"
	"strings"
	"sync"
)

var globalMtx sync.Mutex
var pythonWasLoaded = false
var pythonExe = ""
var pythonMain py.PyThreadStatePtr
var numRuntimes = 0

type InterpreterToken struct {
	idx    int     // Index of interpreter (used by the Runtime implementation).
	id     int64   // Python interpreter id.
	cookie uintptr // Optional cookie value (used by the Runtime implementation).
}

func (i *InterpreterToken) Id() int64 {
	return i.id
}

type Runtime interface {
	// Start the Python runtime.
	Start(ctx context.Context) error

	// Stop the Python runtime, removing all interpreter state.
	Stop(ctx context.Context) error

	// Acquire ownership of an interpreter until Release is called.
	Acquire(ctx context.Context) (token *InterpreterToken, err error)

	// Release ownership of an interpreter identified by the given
	// InterpreterToken.
	Release(token *InterpreterToken) error

	// Destroy an interpreter identified by the given InterpreterToken.
	Destroy(token *InterpreterToken) error

	// Apply a function f over the interpreter described by the given
	// InterpreterToken.
	Apply(token *InterpreterToken, ctx context.Context, f func() error) error

	// Map a function f over the interpreter or interpreters.
	// In the case of multiple interpreters, an error aborts mapping over the
	// rest.
	Map(ctx context.Context, f func(token *InterpreterToken) error) error
}

// Initialize the main Python interpreter or increment the global count if
// already initialized.
//
// Returns the Python main thread state on success.
// On failure, returns a null PyThreadStatePtr and an error.
func initPython(exe, home string, paths []string) (py.PyThreadStatePtr, error) {
	globalMtx.Lock()
	defer globalMtx.Unlock()

	// It's ok if we're starting another instance of the same executable, but
	// we don't want to re-load the libraries as we'll crash.
	if exe != pythonExe && pythonWasLoaded {
		return py.NullThreadState, errors.New("python was already initialized")
	} else if pythonWasLoaded {
		numRuntimes++
		return pythonMain, nil
	}

	// Load our dynamic libraries.
	err := loadPython(exe)
	if err != nil {
		return py.NullThreadState, err
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
	pythonMain = py.PyEval_SaveThread()
	return pythonMain, nil
}

// loadPython loads and binds runtime libraries and pre-initializes the global
// interpreter state.
//
// Must be called with the go routine thread pinned.
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

// unloadPython tears down the global interpreter state.
//
// Must be called with the go routine thread pinned.
func unloadPython(mainThread py.PyThreadStatePtr) error {
	globalMtx.Lock()
	defer globalMtx.Unlock()

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
