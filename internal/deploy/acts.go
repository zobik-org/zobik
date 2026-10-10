package deploy

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"zobik.org/zobik/internal/bus"
)

// The console reaches the Bus through an ephemeral container of the zobik image
// on the network's Docker network, because the NATS server publishes no port on
// the host. The console copies the act's material into it before starting it.
const (
	// CredsPath is where an ephemeral container finds the identity it connects with.
	CredsPath      = "/run/zobik/bus.creds"
	actAccountPath = "/run/zobik/account.jwt"
	// ActCommand is the subcommand the ephemeral container runs: zobik bus-act <act>.
	ActCommand = "bus-act"
)

// The acts.
const (
	actStreams = "streams"
	actAccount = "account"
)

// RunAct executes an act inside the ephemeral container.
func RunAct(ctx context.Context, act string) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	nc, err := bus.Connect(ctx, bus.URL, CredsPath)
	if err != nil {
		return fmt.Errorf("connecting to the Bus: %w", err)
	}
	defer nc.Close()

	switch act {
	case actStreams:
		js, err := jetstream.New(nc)
		if err != nil {
			return err
		}
		return bus.EnsureStreams(ctx, js)
	case actAccount:
		account, err := os.ReadFile(actAccountPath)
		if err != nil {
			return err
		}
		// The full resolver takes the update through the system account (implementation §1.2.1).
		msg, err := nc.RequestWithContext(ctx, "$SYS.REQ.CLAIMS.UPDATE", account)
		if err != nil {
			return err
		}
		var resp struct {
			Error *struct {
				Description string `json:"description"`
			} `json:"error"`
		}
		if err := json.Unmarshal(msg.Data, &resp); err != nil {
			return err
		}
		if resp.Error != nil {
			return fmt.Errorf("account update: %s", resp.Error.Description)
		}
		return nil
	default:
		return fmt.Errorf("unknown act %q", act)
	}
}
