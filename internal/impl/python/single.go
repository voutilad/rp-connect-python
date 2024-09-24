package python

import (
	"context"
	"errors"
	"unsafe"

	"github.com/redpanda-data/benthos/v4/public/service"
	py "github.com/voutilad/gogopython"
)

// SingleInterpreterRuntime provides an implementation for using main
// Python interpreter.
type SingleInterpreterRuntime struct {
	exe     string
	home    string
	paths   []string
	ticket  InterpreterTicket // SingleInterpreterRuntime uses a single ticket.
	started bool              // protected by globalMtx in runtime.go
	logger  *service.Logger
}

func NewSingleInterpreterRuntime(exe string, logger *service.Logger) (*SingleInterpreterRuntime, error) {
	home, paths, err := py.FindPythonHomeAndPaths(exe)
	if err != nil {
		return nil, err
	}

	return &SingleInterpreterRuntime{
		exe:    exe,
		home:   home,
		paths:  paths,
		logger: logger,
		ticket: InterpreterTicket{
			id: -1,
		},
	}, nil
}

func (r *SingleInterpreterRuntime) Start(ctx context.Context) error {
	err := globalMtx.LockWithContext(ctx)
	if err != nil {
		return err
	}
	defer globalMtx.Unlock()

	if r.started {
		// Already running.
		return nil
	}

	_, err = loadPython(r.exe, r.home, r.paths, ctx)
	if err != nil {
		return err
	}

	r.ticket.cookie = uintptr(unsafe.Pointer(r))
	r.started = true
	r.logger.Debug("Python single runtime interpreter started.")

	return nil
}

func (r *SingleInterpreterRuntime) Stop(ctx context.Context) error {
	err := globalMtx.LockWithContext(ctx)
	if err != nil {
		return err
	}
	defer globalMtx.Unlock()

	err = unloadPython(ctx)
	r.ticket.cookie = 0 // NULL
	r.started = false

	if err == nil {
		r.logger.Debug("Python single runtime interpreter stopped.")
	} else {
		r.logger.Warn("Failure while stopping interpreter. Runtime left in undefined state.")
	}

	return nil
}

func (r *SingleInterpreterRuntime) Acquire(ctx context.Context) (*InterpreterTicket, error) {
	return &r.ticket, nil
}

func (r *SingleInterpreterRuntime) Release(ticket *InterpreterTicket) error {
	if ticket.cookie != r.ticket.cookie {
		return errors.New("invalid interpreter ticket")
	}

	return nil
}

func (r *SingleInterpreterRuntime) Apply(ticket *InterpreterTicket, ctx context.Context, f func() error) error {
	if ticket.cookie != r.ticket.cookie {
		return errors.New("invalid interpreter ticket")
	}

	return Evaluate(f, ctx)
}

func (r *SingleInterpreterRuntime) Map(ctx context.Context, f func(ticket *InterpreterTicket) error) error {
	ticket, err := r.Acquire(ctx)
	if err != nil {
		return nil
	}

	err = Evaluate(func() error { return f(ticket) }, ctx)
	_ = r.Release(ticket)
	return err
}
