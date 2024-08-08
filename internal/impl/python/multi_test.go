package python

import (
	"context"
	"fmt"
	"runtime"
	"testing"
)

// Test that we can start and stop the runtime multiple times.
func TestMultiInterpreterRuntimeLifecycle(t *testing.T) {
	for _, isLegacyMode := range []bool{true, false} {
		t.Run(fmt.Sprintf("legacyMode=%t", isLegacyMode), func(t *testing.T) {
			r, err := NewMultiInterpreterRuntime("python3", runtime.NumCPU(), isLegacyMode, nil)
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
		})
	}
}
