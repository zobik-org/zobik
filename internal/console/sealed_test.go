package console

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	"github.com/nats-io/nkeys"
)

func TestSealedKey(t *testing.T) {
	root, err := nkeys.CreateOperator()
	if err != nil {
		t.Fatal(err)
	}
	want, _ := root.PublicKey()

	sealed, err := SealKey(root, []byte("correct horse"))
	if err != nil {
		t.Fatal(err)
	}
	if seed, _ := root.Seed(); bytes.Contains(sealed, seed) {
		t.Fatal("sealed key contains the seed in the clear")
	}

	got, err := OpenKey(sealed, []byte("correct horse"))
	if err != nil {
		t.Fatal(err)
	}
	if pub, _ := got.PublicKey(); pub != want {
		t.Errorf("opened %s, want %s", pub, want)
	}

	if _, err := OpenKey(sealed, []byte("wrong")); !errors.Is(err, ErrWrongPassword) {
		t.Errorf("wrong password: got %v", err)
	}

	resealed, err := ResealKey(sealed, []byte("correct horse"), []byte("new password"))
	if err != nil {
		t.Fatal(err)
	}
	got, err = OpenKey(resealed, []byte("new password"))
	if err != nil {
		t.Fatal(err)
	}
	if pub, _ := got.PublicKey(); pub != want {
		t.Errorf("resealed %s, want %s", pub, want)
	}
}

func TestSealedKeyParamsAreBound(t *testing.T) {
	root, _ := nkeys.CreateOperator()
	sealed, err := SealKey(root, []byte("pw"))
	if err != nil {
		t.Fatal(err)
	}
	var s map[string]any
	if err := json.Unmarshal(sealed, &s); err != nil {
		t.Fatal(err)
	}
	s["time"] = 4
	altered, _ := json.Marshal(s)
	if _, err := OpenKey(altered, []byte("pw")); !errors.Is(err, ErrWrongPassword) {
		t.Errorf("altered parameters: got %v", err)
	}

	s["memory_kib"] = 1 << 30
	if _, err := OpenKey(mustJSON(t, s), []byte("pw")); err == nil {
		t.Error("oversized memory parameter accepted")
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
