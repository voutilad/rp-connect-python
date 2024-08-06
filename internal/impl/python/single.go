package python

import (
	"github.com/pkg/errors"
	"github.com/redpanda-data/benthos/v4/public/service"
	py "github.com/voutilad/gogopython"
	"golang.org/x/net/context"
	"runtime"
	"unsafe"
)

type state int

const (
	stopped state = iota
	started
	destroyed
)

type SingleInterpreterRuntime struct {
	exe     string
	home    string
	paths   []string
	mtxChan chan int
	thread  py.PyThreadStatePtr
	token   InterpreterToken
	state   state
	logger  *service.Logger
}

func NewSingleInterpreterRuntime(exe string, logger *service.Logger) (*SingleInterpreterRuntime, error) {
	home, paths, err := py.FindPythonHomeAndPaths(exe)
	if err != nil {
		return nil, err
	}

	return &SingleInterpreterRuntime{
		exe:     exe,
		home:    home,
		paths:   paths,
		mtxChan: make(chan int, 1),
		logger:  logger,
		token: InterpreterToken{
			id: -1,
		},
		state: stopped,
	}, nil
}

func (r *SingleInterpreterRuntime) lock(ctx context.Context) error {
	select {
	case r.mtxChan <- 1:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (r *SingleInterpreterRuntime) unlock() {
	<-r.mtxChan
}

func (r *SingleInterpreterRuntime) Start(ctx context.Context) error {
	err := r.lock(ctx)
	if err != nil {
		return err
	}
	defer r.unlock()

	if r.state == started {
		// Already running.
		return nil
	}

	// We can Start from a destroyed or stopped state as they are sort of the
	// the same thing in this case since it's a single interpreter runtime.

	runtime.LockOSThread()
	ts, err := initPython(r.exe, r.home, r.paths)
	runtime.UnlockOSThread()
	if err != nil {
		return err
	}

	r.thread = ts
	r.token.cookie = uintptr(unsafe.Pointer(r))
	r.state = started

	return nil
}

func (r *SingleInterpreterRuntime) Stop(ctx context.Context) error {
	err := r.lock(ctx)
	if err != nil {
		return err
	}
	defer r.unlock()

	runtime.LockOSThread()
	py.PyEval_RestoreThread(r.thread)
	py.Py_FinalizeEx()
	runtime.UnlockOSThread()
	r.logger.Debug("Python main interpreter stopped.")

	r.thread = py.NullThreadState
	r.token.cookie = 0
	r.state = stopped

	return nil
}

func (r *SingleInterpreterRuntime) Acquire(ctx context.Context) (*InterpreterToken, error) {
	err := r.lock(ctx)
	if err != nil {
		return nil, err
	}

	return &r.token, nil
}

func (r *SingleInterpreterRuntime) Release(token *InterpreterToken) error {
	if token.cookie != r.token.cookie {
		return errors.New("bad token")
	}

	r.unlock()
	return nil
}

func (r *SingleInterpreterRuntime) Destroy(token *InterpreterToken) error {
	err := r.lock(context.Background())
	if err != nil {
		return err
	}
	if token.cookie != r.token.cookie {
		return errors.New("bad token")
	}
	defer r.unlock()

	r.state = destroyed
	return nil
}

func (r *SingleInterpreterRuntime) Apply(token *InterpreterToken, ctx context.Context, f func() error) error {
	err := r.lock(context.Background())
	if err != nil {
		return err
	}
	defer r.unlock()

	return nil
}

func (r *SingleInterpreterRuntime) Map(ctx context.Context, f func(token *InterpreterToken) error) error {
	token, err := r.Acquire(ctx)
	if err != nil {
		return nil
	}

	return r.Release(token)
}
