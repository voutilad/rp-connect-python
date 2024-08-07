package python

import (
	"context"
	"errors"
	"runtime"
	"unsafe"

	"github.com/redpanda-data/benthos/v4/public/service"
	py "github.com/voutilad/gogopython"
)

type state int

const (
	stopped state = iota
	started
)

type SingleInterpreterRuntime struct {
	exe     string
	home    string
	paths   []string
	mtxChan chan int
	thread  py.PyThreadStatePtr
	ticket  InterpreterTicket // SingleInterpreterRuntime uses a single ticket.
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
		ticket: InterpreterTicket{
			id: -1,
		},
		state: stopped,
	}, nil
}

func (r *SingleInterpreterRuntime) Start(ctx context.Context) error {
	err := globalMtx.LockWithContext(ctx)
	if err != nil {
		return err
	}
	defer globalMtx.Unlock()

	if r.state == started {
		// Already running.
		return nil
	}

	// We can Start from a destroyed or stopped state as they are sort of the
	// same thing in this case since it's a single interpreter runtime.

	runtime.LockOSThread()
	ts, err := loadPython(r.exe, r.home, r.paths)
	runtime.UnlockOSThread()
	if err != nil {
		return err
	}

	r.thread = ts
	r.ticket.cookie = uintptr(unsafe.Pointer(r))
	r.state = started

	return nil
}

func (r *SingleInterpreterRuntime) Stop(ctx context.Context) error {
	err := globalMtx.LockWithContext(ctx)
	if err != nil {
		return err
	}
	defer globalMtx.Unlock()

	runtime.LockOSThread()
	err = unloadPython(r.thread)
	runtime.UnlockOSThread()

	r.thread = py.NullThreadState
	r.ticket.cookie = 0
	r.state = stopped

	if err == nil {
		r.logger.Debug("Python main interpreter stopped.")
	} else {
		r.logger.Warn("Failure while stopping interpreter. Runtime left in undefined state.")
	}

	return nil
}

func (r *SingleInterpreterRuntime) Acquire(ctx context.Context) (*InterpreterTicket, error) {
	err := globalMtx.LockWithContext(ctx)
	if err != nil {
		return nil, err
	}

	return &r.ticket, nil
}

func (r *SingleInterpreterRuntime) Release(ticket *InterpreterTicket) error {
	if ticket.cookie != r.ticket.cookie {
		return errors.New("invalid interpreter ticket")
	}

	globalMtx.Unlock()
	return nil
}

func (r *SingleInterpreterRuntime) Apply(ticket *InterpreterTicket, _ context.Context, f func() error) error {
	if ticket.cookie != r.ticket.cookie {
		return errors.New("invalid interpreter ticket")
	}

	// Pin our go routine & enter the context of the interpreter thread state.
	runtime.LockOSThread()
	py.PyEval_RestoreThread(r.thread)

	err := f()

	// Release our thread state and unpin thread.
	py.PyEval_SaveThread()
	runtime.UnlockOSThread()

	return err
}

func (r *SingleInterpreterRuntime) Map(ctx context.Context, f func(token *InterpreterTicket) error) error {
	token, err := r.Acquire(ctx)
	if err != nil {
		return nil
	}

	// Pin our go routine & enter the context of the interpreter thread state.
	runtime.LockOSThread()
	py.PyEval_RestoreThread(r.thread)

	err = f(token)

	// Release our thread state and unpin thread.
	py.PyEval_SaveThread()
	runtime.UnlockOSThread()

	_ = r.Release(token)
	return err
}
