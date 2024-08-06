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

type InterpreterToken struct {
	idx int   // Index of interpreter (used by the Runtime implementation)
	id  int64 // Python interpreter id.
}

func (i InterpreterToken) Id() int64 {
	return i.id
}

type Runtime interface {
	// Start the Python runtime.
	Start() error

	// Stop the Python runtime, removing all interpreter state.
	Stop() error

	// Acquire ownership of an interpreter until Release is called.
	Acquire(ctx context.Context) (token InterpreterToken, err error)

	// Release an interpreter.
	Release(token InterpreterToken) error

	// Destroy an interpreter.
	Destroy(token InterpreterToken) error
}

// Initialize the main Python interpreter. Should only be called once!
//
// Returns the Python main thread state on success.
// On failure, returns a null Python thread state and an error.
func initPython(exe, home string, paths []string) (py.PyThreadStatePtr, error) {
	globalMtx.Lock()
	defer globalMtx.Unlock()

	// It's ok if we're starting another instance of the same executable, but
	// we don't want to re-load the libraries as we'll crash.
	if exe != pythonExe && pythonWasLoaded {
		return py.NullThreadState, errors.New("python was already initialized")
	} else if pythonWasLoaded {
		return py.NullThreadState, nil
	}

	// Load our dynamic libraries.
	err := loadPython(exe)
	if err != nil {
		return py.NullThreadState, err
	}

	// From now on, we're considered "loaded."
	pythonWasLoaded = true
	pythonExe = exe

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
