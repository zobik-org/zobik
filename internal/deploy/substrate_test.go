package deploy

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/globalconfig"
)

// jetStream runs an in-process server with JetStream; the scopes are tested in bus.
func jetStream(t *testing.T) jetstream.JetStream {
	t.Helper()
	s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("server not ready")
	}
	t.Cleanup(s.Shutdown)
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	return js
}

func applySubstrate(t *testing.T, ctx context.Context, js jetstream.JetStream, ids map[string]string) *substrate {
	t.Helper()
	raw, err := newSubstrate(ids)
	if err != nil {
		t.Fatal(err)
	}
	var s substrate
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatal(err)
	}
	if err := s.ensure(ctx, js); err != nil {
		t.Fatal(err)
	}
	return &s
}

var testIDs = map[string]string{"config": "UCONFIG", "task_broker": "UBROKER", "context": "UCONTEXT", "channel_operator": "UCHANNEL"}

func TestSubstrate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	js := jetStream(t)
	applySubstrate(t, ctx, js, testIDs)

	head, _, err := globalconfig.Head(ctx, js)
	if err != nil {
		t.Fatal(err)
	}
	if head.ConfigVersion != "v0" || head.AuthorizedBy != "deployment" || string(head.Values["network_admission"]) != `"frozen"` {
		t.Errorf("head %+v", head)
	}
	v0, err := globalconfig.Version(ctx, js, "v0")
	if err != nil || !v0.PublishedAt.Equal(head.PublishedAt) {
		t.Errorf("history v0: %+v, %v", v0, err)
	}

	tasks, err := js.KeyValue(ctx, bus.BucketTasks)
	if err != nil {
		t.Fatal(err)
	}
	st, _ := tasks.Status(ctx)
	if st.TTL() != 7*24*time.Hour {
		t.Errorf("tasks TTL %v", st.TTL())
	}

	want := map[string][]string{
		"task/UCONFIG":  {"task.announced.config_change", "task.assigned.UCONFIG", "task.rejected.UCONFIG"},
		"task/UBROKER":  {"task.announced.>", "task.completed.>", "task.failed.>", "task.aborted.>"},
		"prop/UBROKER":  {"prop.>"},
		"task/UCONTEXT": {"task.completed.>", "task.failed.>", "task.aborted.>"},
		"task/UCHANNEL": {"task.announced.hitl_contact_operator", "task.assigned.UCHANNEL", "task.rejected.UCHANNEL", "task.completed.UCHANNEL.>", "task.failed.UCHANNEL.>", "task.aborted.UCHANNEL.>"},
	}
	for key, filters := range want {
		stream, name := key[:4], key[5:]
		c, err := js.Consumer(ctx, stream, name)
		if err != nil {
			t.Errorf("%s: %v", key, err)
			continue
		}
		info, _ := c.Info(ctx)
		if len(info.Config.FilterSubjects) != len(filters) {
			t.Errorf("%s filters %v", key, info.Config.FilterSubjects)
		}
	}
}

// Running it again keeps v0 as it was published, and a replaced identity's
// consumer is deleted.
func TestSubstrateConverges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	js := jetStream(t)
	applySubstrate(t, ctx, js, testIDs)
	first, _, _ := globalconfig.Head(ctx, js)

	ids := map[string]string{}
	for k, v := range testIDs {
		ids[k] = v
	}
	ids["config"] = "UCONFIG2"
	time.Sleep(10 * time.Millisecond)
	applySubstrate(t, ctx, js, ids)

	again, _, _ := globalconfig.Head(ctx, js)
	if !again.PublishedAt.Equal(first.PublishedAt) {
		t.Error("v0 was published again")
	}
	history, _ := js.Stream(ctx, bus.StreamHistory)
	info, _ := history.Info(ctx)
	if info.State.Msgs != 1 {
		t.Errorf("history has %d versions", info.State.Msgs)
	}
	if _, err := js.Consumer(ctx, "task", "UCONFIG"); err == nil {
		t.Error("the replaced identity's consumer remains")
	}
	if _, err := js.Consumer(ctx, "task", "UCONFIG2"); err != nil {
		t.Errorf("the new identity's consumer: %v", err)
	}
}

// Nothing in the history can be deleted (architecture §2.3).
func TestHistoryIsKept(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	js := jetStream(t)
	applySubstrate(t, ctx, js, testIDs)
	history, _ := js.Stream(ctx, bus.StreamHistory)
	if err := history.DeleteMsg(ctx, 1); err == nil {
		t.Error("a version was deleted")
	}
	if err := history.Purge(ctx); err == nil {
		t.Error("the history was purged")
	}
	// A version is never published twice.
	if _, err := js.Publish(ctx, globalconfig.HistorySubject("v0"), []byte("{}"), jetstream.WithExpectLastSequencePerSubject(0)); err == nil {
		t.Error("v0 was published over")
	}
}
