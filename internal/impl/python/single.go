package python

import (
	"context"
	"errors"
	"github.com/redpanda-data/benthos/v4/public/service"
	py "github.com/voutilad/gogopython"
)

// SingleInterpreterRuntime provides an implementation for using main
// Python interpreter.
type SingleInterpreterRuntime struct {
	exe   string
	home  string
	paths []string

	replyChans []chan error
	tickets    chan *InterpreterTicket // SingleInterpreterRuntime uses a single ticket.

	started bool // protected by globalMtx in runtime.go
	logger  *service.Logger
}

func NewSingleInterpreterRuntime(exe string, cnt int, logger *service.Logger) (*SingleInterpreterRuntime, error) {
	home, paths, err := py.FindPythonHomeAndPaths(exe)
	if err != nil {
		return nil, err
	}

	return &SingleInterpreterRuntime{
		exe:        exe,
		home:       home,
		paths:      paths,
		logger:     logger,
		tickets:    make(chan *InterpreterTicket, cnt),
		replyChans: make([]chan error, cnt),
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

	loadPython(r.exe, r.home, r.paths, ctx)
	r.logger.Debug("Python interpreter started.")

	for idx := range len(r.replyChans) {
		r.replyChans[idx] = make(chan error)
		r.tickets <- &InterpreterTicket{idx: idx, id: -1}
	}

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

	if !r.started {
		return errors.New("not started")
	}

	// Collect all the tickets so nobody else can get them.
	tickets := make([]*InterpreterTicket, len(r.tickets))
	for idx := range tickets {
		ticket, err := r.Acquire(ctx)
		if err != nil {
			panic("cannot acquire ticket while stopping")
		}
		tickets[idx] = ticket
	}

	err = unloadPython(ctx)
	if err != nil {
		return err
	}
	r.started = false

	if err == nil {
		r.logger.Debug("Python single runtime interpreter stopped.")
	} else {
		r.logger.Warn("Failure while stopping interpreter. Runtime left in undefined state.")
	}

	return nil
}

func (r *SingleInterpreterRuntime) Acquire(ctx context.Context) (*InterpreterTicket, error) {
	// Take a ticket from the pool.
	select {
	case ticket := <-r.tickets:
		return ticket, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (r *SingleInterpreterRuntime) Release(ticket *InterpreterTicket) error {
	// Double-check the token is valid.
	if ticket.idx < 0 || ticket.idx > len(r.tickets) {
		return errors.New("invalid ticket: bad index")
	}

	// Return the ticket to the pool. This should not block as the channel is
	// buffered.
	r.tickets <- ticket

	return nil
}

func (r *SingleInterpreterRuntime) Apply(ticket *InterpreterTicket, ctx context.Context, f func() error) error {
	// Double-check the token is valid.
	if ticket.idx < 0 || ticket.idx > len(r.tickets) {
		return errors.New("invalid ticket: bad index")
	}

	return Evaluate(f, r.replyChans[ticket.idx], ctx)
}

func (r *SingleInterpreterRuntime) Map(ctx context.Context, f func(ticket *InterpreterTicket) error) error {
	ticket, err := r.Acquire(ctx)
	if err != nil {
		return nil
	}

	err = r.Apply(ticket, ctx, func() error { return f(ticket) })
	_ = r.Release(ticket)
	return err
}
