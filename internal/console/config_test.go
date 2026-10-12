package console_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/console"
	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/internal/role"
	"zobik.org/zobik/internal/role/channel"
	"zobik.org/zobik/internal/role/config"
	"zobik.org/zobik/internal/role/taskbroker"
	"zobik.org/zobik/internal/testnet"
)

// operator is the console against a network with the Config Store, the Task
// Broker and the Operator Channel running.
func operator(t *testing.T) (*console.Operator, *testnet.Net) {
	n := testnet.New(t, func(v globalconfig.Values) {
		testnet.Set(v, map[string]any{"auction_timeout": 200})
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	n.Start("config", func(ctx context.Context, rt *role.Runtime, nc *nats.Conn) error {
		return config.Run(ctx, rt, nc, config.Options{ContextAddr: n.ContextAddr})
	})
	n.Start("task_broker", taskbroker.Run)
	n.Start("channel_operator", func(ctx context.Context, rt *role.Runtime, nc *nats.Conn) error {
		return channel.Run(ctx, rt, nc, channel.Options{Audience: channel.AudienceOperator, Listen: addr, ContextAddr: n.ContextAddr})
	})
	pub, _ := n.ConsoleKey.PublicKey()
	token, err := bus.IssueUser(n.Account.SigningKeys[bus.ConsoleScope.Role], n.Account.Public, pub, "console", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	client, closeConn, err := console.Dial(addr, token, n.ConsoleKey)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeConn() })
	return &console.Operator{Root: n.Root, ChannelID: n.ID("channel_operator"), Client: client}, n
}

func assign(values map[string]any) map[string]json.RawMessage {
	out := map[string]json.RawMessage{}
	for k, v := range values {
		out[k], _ = json.Marshal(v)
	}
	return out
}

func TestConfigSet(t *testing.T) {
	op, n := operator(t)
	snap, err := op.Snapshot(n.Ctx)
	if err != nil || snap.ConfigVersion != "v0" {
		t.Fatalf("snapshot %+v %v", snap, err)
	}
	out, err := op.ConfigSet(n.Ctx, snap, assign(map[string]any{"task_lease_ttl": 45000}))
	if err != nil || out.ConfigVersion != "v1" || out.FailureCode != "" {
		t.Fatalf("set: %+v %v", out, err)
	}

	// A precondition the edit does not meet stops it before it is signed.
	_, err = op.ConfigSet(n.Ctx, snap, assign(map[string]any{"network_admission": "open"}))
	var invalid *globalconfig.InvalidError
	if !errors.As(err, &invalid) {
		t.Errorf("precondition: %v", err)
	}

	// An edit on the version that is no longer the head is stale.
	out, err = op.ConfigSet(n.Ctx, snap, assign(map[string]any{"task_lease_ttl": 50000}))
	if err != nil || out.FailureCode != "base_version_stale" {
		t.Errorf("stale: %+v %v", out, err)
	}
	if now, _ := op.Snapshot(n.Ctx); now.ConfigVersion != "v1" || string(now.Values["task_lease_ttl"]) != "45000" {
		t.Errorf("head %+v", now)
	}
}
