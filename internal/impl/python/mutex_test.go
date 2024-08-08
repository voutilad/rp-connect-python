package python

import (
	"golang.org/x/net/context"
	"testing"
	"time"
)

func TestMutexAcquisitionCanBeCancelled(t *testing.T) {
	m := NewContextAwareMutex()
	ctx := context.Background()

	// First, we lock.
	_ = m.Lock()

	// Then we try locking again, but we _should_ time out.
	ctx2, cancelFn := context.WithTimeout(ctx, 1*time.Second)
	cancelled := make(chan bool)

	go func() {
		err := m.LockWithContext(ctx2)
		if err != nil {
			cancelled <- true
		} else {
			cancelled <- false
		}
	}()

	// Cancel our locking.
	cancelFn()
	select {
	case r := <-cancelled:
		if r == false {
			t.Fatal("Mutex locking was not cancelled.")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for cancellation.")
	}
}
