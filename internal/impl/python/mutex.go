package python

import (
	"context"
)

type key int

var globalKey = key(0)

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

func (c *ContextAwareMutex) Lock() error {
	return c.LockWithContext(c.defaultCtx)
}

func (c *ContextAwareMutex) LockWithContext(ctx context.Context) error {
	select {
	// Put the key in the lock.
	case c.ch <- globalKey:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *ContextAwareMutex) Unlock() {
	if len(c.ch) == 1 {
		// Take the key out of the lock
		<-c.ch
	} else {
		panic("unlock called too many times")
	}
}

func (c *ContextAwareMutex) AssertLocked() {
	if len(c.ch) != 1 {
		// No key in the lock!
		panic("lock not held")
	}
}
