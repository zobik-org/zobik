package console_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/console"
)

// zobik rates set validates the table and writes it to the rates bucket with
// the console's identity, which the open session holds (implementation §8.5).
func TestRates(t *testing.T) {
	var writes atomic.Int32
	var f *fixture
	f = newFixture(t, func(o *console.Options) {
		o.WriteRates = func(ctx context.Context, creds []byte, name string, table []byte) error {
			writes.Add(1)
			path := filepath.Join(t.TempDir(), "act.creds")
			if err := os.WriteFile(path, creds, 0o600); err != nil {
				return err
			}
			nc, err := nats.Connect(f.Srv.ClientURL(), nats.UserCredentials(path))
			if err != nil {
				return err
			}
			defer nc.Close()
			js, _ := jetstream.New(nc)
			kv, err := js.KeyValue(ctx, bus.BucketRates)
			if err != nil {
				return err
			}
			_, err = kv.Put(ctx, name, table)
			return err
		}
	})
	c := f.signIn()
	table := `{"as_of":"2026-10-01","max_per_call":2.0,"rates":{"gpt-5":{"input":0.00000125,"output":0.00001}}}`
	if err := c.SetRates(f.Ctx, "openai", []byte(table)); err != nil {
		t.Fatal(err)
	}
	kv, err := f.Console.KeyValue(f.Ctx, bus.BucketRates)
	if err != nil {
		t.Fatal(err)
	}
	if e, err := kv.Get(f.Ctx, "openai"); err != nil || !strings.Contains(string(e.Value()), "gpt-5") {
		t.Errorf("the bucket holds %v, %v", e, err)
	}

	for name, body := range map[string]string{
		"openai":   `{"as_of":"2026-10-01","max_per_call":-1}`,
		"a b":      table,
		"no-as-of": `{"max_per_call":1}`,
	} {
		if err := c.SetRates(f.Ctx, name, []byte(body)); err == nil {
			t.Errorf("%s %s was written", name, body)
		}
	}
	if n := writes.Load(); n != 1 {
		t.Errorf("%d acts ran, want only the valid one", n)
	}
}
