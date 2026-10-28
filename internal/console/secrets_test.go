package console_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"

	"zobik.org/zobik/internal/console"
)

// The console writes a secret only with the session open, never returns it, and
// rejects a handle the Secret Store does not admit (implementation §6, The
// operator console).
func TestSecrets(t *testing.T) {
	var mu sync.Mutex
	written := map[string]string{}
	f := newFixture(t, func(o *console.Options) {
		o.WriteSecret = func(_ context.Context, handle string, value []byte) error {
			if strings.Contains(handle, " ") {
				return fmt.Errorf("%w: %q", console.ErrHandle, handle)
			}
			mu.Lock()
			defer mu.Unlock()
			if value == nil {
				delete(written, handle)
			} else {
				written[handle] = string(value)
			}
			return nil
		}
	})
	if code := f.status(http.MethodPut, console.PathSecrets+"/openai", nil, nil); code != http.StatusUnauthorized {
		t.Errorf("without session: %d", code)
	}
	c := f.signIn()
	if err := c.SetSecret(f.Ctx, "openai", []byte("sk-1")); err != nil {
		t.Fatal(err)
	}
	if err := c.SetSecret(f.Ctx, "zobik/listener/a.example", []byte("cert")); err != nil {
		t.Fatal(err)
	}
	if err := c.SetSecret(f.Ctx, "with space", []byte("x")); err == nil || !strings.Contains(err.Error(), "not a handle") {
		t.Errorf("an invalid handle: %v", err)
	}
	if err := c.DeleteSecret(f.Ctx, "openai"); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(written) != 1 || written["zobik/listener/a.example"] != "cert" {
		t.Errorf("written: %v", written)
	}
	if code := f.status(http.MethodGet, console.PathSecrets+"/zobik/listener/a.example", nil, nil); code == http.StatusOK {
		t.Error("the API returned a value")
	}
}
