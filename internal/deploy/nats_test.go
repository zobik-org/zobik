package deploy

import (
	"testing"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nkeys"

	"zobik.org/zobik/internal/bus"
)

// The configuration is parsed by the same server version the image pins.
func TestNATSConfig(t *testing.T) {
	root, _ := nkeys.CreateOperator()
	a, err := bus.NewAccount(root, "test", []bus.Scope{bus.ConsoleScope})
	if err != nil {
		t.Fatal(err)
	}
	config, err := natsConfig(t.TempDir(), a.Operator, a.System, a.JWT)
	if err != nil {
		t.Fatal(err)
	}
	var o server.Options
	if err := o.ProcessConfigString(string(config)); err != nil {
		t.Fatalf("%v\n%s", err, config)
	}
	op, _ := jwt.DecodeOperatorClaims(a.Operator)
	if o.Port != 4222 || !o.JetStream || o.SystemAccount != op.SystemAccount || len(o.TrustedOperators) != 1 {
		t.Errorf("parsed as port %d, jetstream %v, system %q, %d operators", o.Port, o.JetStream, o.SystemAccount, len(o.TrustedOperators))
	}
}
