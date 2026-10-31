package openai

import (
	"encoding/json"
	"os"
	"testing"

	"zobik.org/zobik/internal/egress"
)

func TestEntry(t *testing.T) {
	for _, handle := range []string{"", "openai"} {
		e, err := egress.ParseEntry(Entry(Target, handle))
		if err != nil {
			t.Fatalf("handle %q: %v", handle, err)
		}
		if e.CredentialHandle != handle || e.RateSource != RateSource {
			t.Errorf("handle %q: entry has handle %q and rate_source %q", handle, e.CredentialHandle, e.RateSource)
		}
	}
}

func TestRates(t *testing.T) {
	if _, err := egress.ParseTable(Rates); err != nil {
		t.Fatal(err)
	}
}

// The assistant blueprint copies the interface into its slot; a copy that
// drifts would leave the slot unbound.
func TestBlueprintSlot(t *testing.T) {
	raw, err := os.ReadFile("../../../examples/blueprints/assistant/blueprint.json")
	if err != nil {
		t.Fatal(err)
	}
	var b struct {
		Requires struct {
			EgressRequired []struct {
				Interface json.RawMessage `json:"interface"`
			} `json:"egress_required"`
		} `json:"requires"`
	}
	if err := json.Unmarshal(raw, &b); err != nil {
		t.Fatal(err)
	}
	want, err := egress.ParseInterface(Interface)
	if err != nil {
		t.Fatal(err)
	}
	got, err := egress.ParseInterface(b.Requires.EgressRequired[0].Interface)
	if err != nil {
		t.Fatal(err)
	}
	if got.Digest != want.Digest {
		t.Errorf("the blueprint's slot has digest %s, the interface %s", got.Digest, want.Digest)
	}
}
