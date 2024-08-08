package python

import (
	"context"
	"testing"
)

// Test that we can start and stop the runtime multiple times.
func TestSingleInterpreterRuntimeLifecycle(t *testing.T) {
	r, err := NewSingleInterpreterRuntime("python3", nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	for i := 1; i <= 10; i++ {
		err = r.Start(ctx)
		if err != nil {
			t.Fatalf("failed to start on iteration %d: %s\n", i, err)
		}

		err = r.Stop(ctx)
		if err != nil {
			t.Fatalf("failed to stop on iteration %d: %s\n", i, err)
		}
	}
}
