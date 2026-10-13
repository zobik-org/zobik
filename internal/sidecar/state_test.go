package sidecar

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"zobik.org/zobik/internal/bus"
)

// The outbox keeps what was not confirmed, and the delivered ids are kept for
// the Bus's retention, whatever name an id has.
func TestState(t *testing.T) {
	dir := t.TempDir()
	st, err := openState(dir)
	if err != nil {
		t.Fatal(err)
	}
	e, _ := bus.NewEvent(bus.TypeCompleted, "node", "task", map[string]any{})
	e.ID = "../../escape"
	if err := st.put(&pending{Subject: "task.completed.x.y", Event: e, Embed: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "..", "escape")); !os.IsNotExist(err) {
		t.Fatalf("an id named a file outside the outbox: %v", err)
	}
	unsent, err := st.unsent()
	if err != nil || len(unsent) != 1 || unsent[0].Event.ID != e.ID || !unsent[0].Embed {
		t.Fatalf("unsent: %v, %v", unsent, err)
	}
	// A damaged file is set aside and holds back nothing.
	if err := os.WriteFile(filepath.Join(st.outbox, "damaged.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if unsent, err := st.unsent(); err != nil || len(unsent) != 1 {
		t.Fatalf("unsent with a damaged file: %v, %v", unsent, err)
	}
	if _, err := os.Stat(filepath.Join(st.outbox, "damaged.json.bad")); err != nil {
		t.Errorf("the damaged file: %v", err)
	}
	if err := st.sent(e.ID); err != nil {
		t.Fatal(err)
	}
	if unsent, _ := st.unsent(); len(unsent) != 0 {
		t.Errorf("unsent after sending: %v", unsent)
	}

	if st.delivered("a") {
		t.Error("delivered before marking")
	}
	for _, id := range []string{"a", "b"} {
		if err := st.mark(id); err != nil {
			t.Fatal(err)
		}
	}
	old := time.Now().Add(-bus.Retention - time.Hour)
	if err := os.Chtimes(filepath.Join(st.seen, fileName("a")), old, old); err != nil {
		t.Fatal(err)
	}
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}
	if st.delivered("a") || !st.delivered("b") {
		t.Errorf("after pruning: a %v, b %v", st.delivered("a"), st.delivered("b"))
	}
}
