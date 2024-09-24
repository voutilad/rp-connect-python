package python

import (
	"context"
	"errors"
	"runtime"

	"github.com/redpanda-data/benthos/v4/public/service"
	py "github.com/voutilad/gogopython"
)

// MultiInterpreterRuntime creates and manages multiple Python sub-interpreters.
type MultiInterpreterRuntime struct {
	exe   string   // Python exe (binary).
	home  string   // Python home.
	paths []string // Python package paths.

	interpreters []*SubInterpreter       // Sub-interpreters.
	tickets      chan *InterpreterTicket // Tickets for sub-interpreters.

	mtx        *ContextAwareMutex // Mutex to write protect the runtime state.
	started    bool
	legacyMode bool            // Running in legacy mode?
	logger     *service.Logger // Redpanda Connect logger service.
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
		mtx:          NewContextAwareMutex(),
		interpreters: make([]*SubInterpreter, cnt),
		tickets:      make(chan *InterpreterTicket, cnt),
		legacyMode:   legacyMode,
		logger:       logger,
	}, nil
}

// Start the Python runtime. A MultiInterpreterRuntime centralizes modification
// of the main interpreter in a go routine.
func (r *MultiInterpreterRuntime) Start(ctx context.Context) error {
	// Acquire the big lock as we're manipulating global runtime state.
	err := globalMtx.LockWithContext(ctx)
	if err != nil {
		return err
	}
	defer globalMtx.Unlock()

	if !r.started {
		ts, err := loadPython(r.exe, r.home, r.paths, ctx)
		if err != nil || ts == py.NullThreadState {
			r.logger.Errorf("Failed to start Python interpreter.")
			return err
		}
		r.logger.Debug("Python interpreter started.")

		// Start up sub-interpreters.
		for idx := range len(r.interpreters) {
			sub, err := Spawn(r.legacyMode, ctx)
			if err != nil {
				r.logger.Error("Failed to create new sub-interpreter.")
				return err
			}

			// Populate our ticket booth and interpreter list.
			r.interpreters[idx] = sub
			r.tickets <- &InterpreterTicket{idx: idx, id: sub.Id}
			r.logger.Tracef("Initialized sub-interpreter %d.\n", sub.Id)
		}

		r.started = true
		r.logger.Debugf("Started %d sub-interpreters.", len(r.tickets))
	}
	return nil
}

// Stop a running Python Runtime.
func (r *MultiInterpreterRuntime) Stop(ctx context.Context) error {
	err := globalMtx.LockWithContext(ctx)
	if err != nil {
		return err
	}
	defer globalMtx.Unlock()

	if !r.started {
		return errors.New("not started")
	}

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// Collect all the tickets before stopping the sub-interpreters.
	tickets := make([]*InterpreterTicket, len(r.tickets))
	for idx := range tickets {
		ticket, err := r.Acquire(ctx)
		if err != nil {
			panic("cannot acquire ticket while stopping")
		}
		tickets[idx] = ticket
	}

	// We have all the tickets. Time to kill the sub-interpreters.
	for _, ticket := range tickets {
		sub := r.interpreters[ticket.idx]
		err = StopSub(sub, ctx)
		if err != nil {
			return err
		}
		r.logger.Tracef("Stopped sub-interpreter %d.\n", sub.Id)
	}

	// Tear down the runtime.
	err = unloadPython(ctx)
	if err != nil {
		return err
	}
	r.started = false

	r.logger.Debug("Python interpreter stopped.")
	return nil
}

func (r *MultiInterpreterRuntime) Acquire(ctx context.Context) (*InterpreterTicket, error) {
	// Take a ticket from the pool.
	select {
	case ticket := <-r.tickets:
		return ticket, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (r *MultiInterpreterRuntime) Release(ticket *InterpreterTicket) error {
	// Double-check the token is valid.
	if ticket.idx < 0 || ticket.idx > len(r.interpreters) {
		return errors.New("invalid ticket: bad index")
	}

	// Return the ticket to the pool. This should not block as the channel is
	// buffered.
	r.tickets <- ticket

	return nil
}

func (r *MultiInterpreterRuntime) Apply(ticket *InterpreterTicket, _ context.Context, f func() error) error {
	// Double-check the token is valid.
	if ticket.idx < 0 || ticket.idx > len(r.interpreters) {
		return errors.New("invalid ticket: bad index")
	}

	interpreter := r.interpreters[ticket.idx]
	if interpreter.Id != ticket.id {
		return errors.New("invalid ticket: bad interpreter id")
	}

	// Pin our go routine & enter the context of the interpreter thread state.
	runtime.LockOSThread()
	py.PyEval_RestoreThread(interpreter.Thread)

	err := f()

	// Release our thread state and unpin thread.
	py.PyEval_SaveThread()
	runtime.UnlockOSThread()

	return err
}

// Map a function fn over all the interpreters, one at a time. Useful for
// initializing all interpreters to a given state.
func (r *MultiInterpreterRuntime) Map(ctx context.Context, f func(t *InterpreterTicket) error) error {
	// Acquire all tickets so we have sole control of the interpreter. Makes it
	// easier to know if we applied the function to all sub-interpreters.
	tickets := make([]*InterpreterTicket, len(r.tickets))
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
	defer runtime.UnlockOSThread()
	for _, ticket := range tickets {
		sub := r.interpreters[ticket.idx]
		py.PyEval_RestoreThread(sub.Thread)
		err := f(ticket)
		py.PyEval_SaveThread()
		if err != nil {
			return err
		}
	}

	return nil
}
