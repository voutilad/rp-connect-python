package python

import (
	"context"
)

type key int

const globalKey = key(0)

// A ContextAwareMutex implements a mutex lock that allows for an optional
// context to be provided so a caller can control if they want a deadline
// or timeout.
type ContextAwareMutex struct {
	ch         chan key
	defaultCtx context.Context
}

func NewContextAwareMutex() *ContextAwareMutex {
	return &ContextAwareMutex{
		ch:         make(chan key, 1),
		defaultCtx: context.Background(),
	}
}

// Lock the mutex using a background context.
func (c *ContextAwareMutex) Lock() error {
	return c.LockWithContext(c.defaultCtx)
}

// LockWithContext will attempt to take the lock but uses the provided
// [context.Context] to allow cancellation.
func (c *ContextAwareMutex) LockWithContext(ctx context.Context) error {
	select {
	// Put the key in the lock.
	case c.ch <- globalKey:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Unlock the mutex. Panics if the mutex is not currently locked in order
// to speed up debugging.
func (c *ContextAwareMutex) Unlock() {
	if len(c.ch) == 1 {
		// Take the key out of the lock
		<-c.ch
	} else {
		panic("unlock called too many times")
	}
}

// AssertLocked panics if the ContextAwareMutex is not currently locked
// by someone.
func (c *ContextAwareMutex) AssertLocked() {
	if len(c.ch) != 1 {
		// No key in the lock!
		panic("lock not held")
	}
}
