package deploy

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/nats-io/nats.go/jetstream"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/egress"
)

// The act that writes a rate table, and the file that carries it into the
// ephemeral container (implementation §8.5).
const (
	actRates     = "rates"
	actRatesPath = "/run/zobik/rates.json"
)

type ratesAct struct {
	Name  string          `json:"name"`
	Table json.RawMessage `json:"table"`
}

// WriteRates writes the table of name to the rates bucket, with the console's
// identity creds, from an ephemeral container on the network's Docker network.
// It is an act of deployment, and publishes no event (implementation §8.5).
func WriteRates(ctx context.Context, e *Engine, dir, network, image string, creds []byte, name string, table []byte) error {
	if !egress.ValidRateSource(name) {
		return fmt.Errorf("deploy: %q is not a key of the rates bucket", name)
	}
	if _, err := egress.ParseTable(table); err != nil {
		return err
	}
	root, err := readRoot(dir)
	if err != nil {
		return err
	}
	if root == "" {
		return ErrNoPanel
	}
	act, err := json.Marshal(ratesAct{Name: name, Table: table})
	if err != nil {
		return err
	}
	return runAct(ctx, e, names{network, root}, image, actRates, creds, File{Path: actRatesPath, Mode: 0o600, Data: act})
}

// putRates is the act inside the container.
func putRates(ctx context.Context, js jetstream.JetStream) error {
	raw, err := os.ReadFile(actRatesPath)
	if err != nil {
		return err
	}
	var a ratesAct
	if err := json.Unmarshal(raw, &a); err != nil {
		return err
	}
	kv, err := js.KeyValue(ctx, bus.BucketRates)
	if err != nil {
		return err
	}
	_, err = kv.Put(ctx, a.Name, a.Table)
	return err
}
