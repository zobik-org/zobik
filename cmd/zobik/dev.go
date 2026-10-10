//go:build dev

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/deploy"
)

// The development viewer of the first stages and its companion: zobik tap shows
// every event on the Bus live, and zobik publish puts one there by hand. Each runs
// as an ephemeral container on the network's Docker network, with an identity of
// a scope that only a development binary registers.
func init() {
	devScopes = append(devScopes, bus.TapScope, bus.PublishScope)
	devCommands["tap"] = tapCmd
	devCommands["publish"] = publishCmd
}

const (
	devIdentityLifetime = 12 * time.Hour
	eventPath           = "/run/zobik/event.json"
	subjectPath         = "/run/zobik/subject"
	// tapReady is the line zobik tap writes once it receives.
	tapReady = "tap: listening"
)

func tapCmd(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("tap", flag.ContinueOnError)
	var n networkFlags
	n.register(fs)
	inside := fs.Bool("inside", false, "run inside the network (the ephemeral container does)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *inside {
		return tapInside(ctx)
	}
	return runOnBus(ctx, n, bus.TapScope.Role, []string{"tap", "--inside"}, true)
}

func tapInside(ctx context.Context) error {
	nc, err := bus.Connect(ctx, bus.URL, deploy.CredsPath)
	if err != nil {
		return err
	}
	defer nc.Close()
	for _, family := range bus.TapScope.Subscribe {
		if _, err := nc.Subscribe(family, printEvent); err != nil {
			return err
		}
	}
	if err := nc.Flush(); err != nil {
		return err
	}
	fmt.Printf("%s on %s\n", tapReady, strings.Join(bus.TapScope.Subscribe, ", "))
	<-ctx.Done()
	return nil
}

func printEvent(m *nats.Msg) {
	var b strings.Builder
	fmt.Fprintf(&b, "\n── %s  %s\n", time.Now().Format("15:04:05.000"), m.Subject)
	if len(m.Data) == 0 {
		b.WriteString("(no body)\n")
		fmt.Print(b.String())
		return
	}
	var e bus.Event
	if err := json.Unmarshal(m.Data, &e); err != nil {
		fmt.Fprintf(&b, "not an envelope: %v\n%s\n", err, m.Data)
		fmt.Print(b.String())
		return
	}
	if err := e.Validate(); err != nil {
		fmt.Fprintf(&b, "INVALID: %v\n", err)
	}
	var pretty bytes.Buffer
	if json.Indent(&pretty, m.Data, "", "  ") == nil {
		b.Write(pretty.Bytes())
	} else {
		b.Write(m.Data)
	}
	b.WriteString("\n")
	fmt.Print(b.String())
}

func publishCmd(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("publish", flag.ContinueOnError)
	var n networkFlags
	n.register(fs)
	inside := fs.Bool("inside", false, "run inside the network (the ephemeral container does)")
	typ := fs.String("type", bus.TypeAnnounced, "the event's type")
	topic := fs.String("topic", "", "the topic, for the types whose subject carries it")
	identity := fs.String("identity", "", "the recipient or proposer identity, for the types whose subject carries it")
	task := fs.String("task", "", "the event's subject: the TaskID (default: a new one)")
	source := fs.String("source", "zobik-publish", "the event's source")
	data := fs.String("data", "{}", "the event's data, as a JSON object")
	traceparent := fs.String("traceparent", "", "the W3C traceparent (default: a new trace for task.* events)")
	configVersion := fs.String("configversion", "v0", "the configversion, stamped with traceparent")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *inside {
		return publishInside(ctx)
	}

	var d map[string]any
	if err := json.Unmarshal([]byte(*data), &d); err != nil {
		return fmt.Errorf("--data: %w", err)
	}
	if *topic != "" && *typ != bus.TypeAssigned && *typ != bus.TypeRejected {
		if _, ok := d["topic"]; !ok {
			d["topic"] = *topic
		}
	}
	if *task == "" {
		*task = uuid.NewString()
	}
	e, err := bus.NewEvent(*typ, *source, *task, d)
	if err != nil {
		return err
	}
	if *traceparent == "" && strings.HasPrefix(*typ, "task.") {
		*traceparent = newTraceparent()
	}
	if *traceparent != "" {
		e.TraceParent, e.ConfigVersion = *traceparent, *configVersion
	}
	subject, err := subjectFor(*typ, *topic, *identity)
	if err != nil {
		return err
	}
	if err := e.Validate(); err != nil {
		return err
	}
	event, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return runOnBus(ctx, n, bus.PublishScope.Role, []string{"publish", "--inside"}, false,
		deploy.File{Path: eventPath, Mode: 0o600, Data: event},
		deploy.File{Path: subjectPath, Mode: 0o600, Data: []byte(subject)})
}

func publishInside(ctx context.Context) error {
	raw, err := os.ReadFile(eventPath)
	if err != nil {
		return err
	}
	subject, err := os.ReadFile(subjectPath)
	if err != nil {
		return err
	}
	var e bus.Event
	if err := json.Unmarshal(raw, &e); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	nc, err := bus.Connect(ctx, bus.URL, deploy.CredsPath)
	if err != nil {
		return err
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	if err := bus.Publish(ctx, js, string(subject), &e); err != nil {
		return err
	}
	fmt.Printf("published %s %s on %s\n", e.Type, e.ID, subject)
	return nil
}

func subjectFor(typ, topic, identity string) (string, error) {
	need := func(name, v string) error {
		if v == "" {
			return fmt.Errorf("a %s needs --%s", typ, name)
		}
		return nil
	}
	switch typ {
	case bus.TypeAnnounced:
		return bus.SubjectAnnounced(topic), need("topic", topic)
	case bus.TypeNotice:
		return bus.SubjectNotice(topic), need("topic", topic)
	case bus.TypeProposed:
		if err := need("identity", identity); err != nil {
			return "", err
		}
		return bus.SubjectProposed(identity, topic), need("topic", topic)
	case bus.TypeAssigned, bus.TypeRejected:
		return bus.SubjectAddressed(typ, identity), need("identity", identity)
	case bus.TypeCompleted, bus.TypeFailed, bus.TypeAborted:
		if err := need("identity", identity); err != nil {
			return "", err
		}
		return bus.SubjectTerminal(typ, identity, topic), need("topic", topic)
	}
	return "", fmt.Errorf("unknown type %q", typ)
}

// newTraceparent starts a W3C trace: version 00, a random trace and span, sampled.
func newTraceparent() string {
	b := make([]byte, 24)
	rand.Read(b)
	return "00-" + hex.EncodeToString(b[:16]) + "-" + hex.EncodeToString(b[16:]) + "-01"
}

// runOnBus runs cmd in an ephemeral container with a fresh identity of role:
// attached until interrupted, or to completion.
func runOnBus(ctx context.Context, n networkFlags, role string, cmd []string, attached bool, files ...deploy.File) error {
	if err := n.resolve(); err != nil {
		return err
	}
	creds, err := deploy.EphemeralIdentity(n.dir, role, devIdentityLifetime)
	if err != nil {
		return err
	}
	e, err := deploy.NewEngine(ctx)
	if err != nil {
		return err
	}
	defer e.Close()
	spec := deploy.BusSpec(n.network, role, image, cmd, creds, files...)
	if attached {
		return e.RunAttached(ctx, spec, os.Stdout)
	}
	out, err := e.RunEphemeral(ctx, spec)
	os.Stdout.Write(out)
	return err
}
