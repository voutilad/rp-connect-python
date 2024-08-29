package processor

import (
	"runtime"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/voutilad/rp-connect-python/internal/impl/python"
	"golang.org/x/net/context"
)

var script = `
import json
this = json.loads(content().decode())
root = json.dumps(this["thang"])
meta["junk"] = "hey man"
`

func TestDifferentInterpreterModes(t *testing.T) {
	expected := "be thangin'"
	data := map[string]interface{}{
		"thang": expected,
		"other": 123,
	}

	for _, m := range []python.Mode{python.Isolated, python.IsolatedLegacy, python.Global} {
		t.Run(string(m), func(t *testing.T) {
			proc, err := NewPythonProcessor("python3", script, runtime.NumCPU(), m, python.Bloblang, nil)
			if err != nil {
				t.Fatal(err)
			}

			msg := service.NewMessage(nil)
			msg.SetStructured(data)
			batch := service.MessageBatch{msg}

			batches, err := proc.ProcessBatch(context.Background(), batch)
			if err != nil {
				t.Fatal(err)
			}

			if len(batches) != 1 {
				t.Error("expected a single batch")
			}
			newBatch := batches[0]
			if len(newBatch) != 1 {
				t.Error("expected a single message")
			}
			obj, err := newBatch[0].AsStructured()
			if err != nil {
				t.Fatal(err)
			}
			root, ok := obj.(string)
			if !ok {
				t.Fatal("expected root to be a string")
			}
			if root != expected {
				t.Fatalf("expected '%s', got '%s'\n", expected, root)
			}
			val, ok := newBatch[0].MetaGetMut("junk")
			if !ok {
				t.Fatal("expected meta to have 'junk' key")
			}
			if val != "hey man" {
				t.Fatal("expected 'hey man', got ", val)
			}
		})
	}
}
