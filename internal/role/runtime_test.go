package role

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nkeys"
)

func testRuntime(t *testing.T) *Runtime {
	t.Helper()
	dir := t.TempDir()
	kp, _ := nkeys.CreateUser()
	seed, _ := kp.Seed()
	root, _ := nkeys.CreateOperator()
	rootPub, _ := root.PublicKey()
	os.WriteFile(filepath.Join(dir, FileIdentity), seed, 0o600)
	os.WriteFile(filepath.Join(dir, FileRootPub), []byte(rootPub), 0o644)
	rt, err := Load("test", dir)
	if err != nil {
		t.Fatal(err)
	}
	return rt
}

// A loop that stops marking past its deadline ends the process.
func TestWatchdog(t *testing.T) {
	rt := testRuntime(t)
	exited := make(chan string, 1)
	rt.exit = func(reason string) {
		select {
		case exited <- reason:
		default:
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	alive := rt.Loop("alive", 300*time.Millisecond)
	go func() {
		for ctx.Err() == nil {
			alive.Mark()
			time.Sleep(50 * time.Millisecond)
		}
	}()
	go rt.Watch(ctx, 50*time.Millisecond)
	select {
	case r := <-exited:
		t.Fatalf("a loop that marks was killed: %s", r)
	case <-time.After(time.Second):
	}

	rt.Loop("stuck", 200*time.Millisecond)
	select {
	case <-exited:
	case <-time.After(2 * time.Second):
		t.Fatal("a stuck loop did not end the process")
	}
}

// The health socket answers ready only when the role is ready and connected.
func TestHealth(t *testing.T) {
	rt := testRuntime(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sock := filepath.Join(t.TempDir(), "health.sock")
	if err := rt.ServeHealth(ctx, sock); err != nil {
		t.Fatal(err)
	}
	if err := CheckHealth(sock, time.Second); !errors.Is(err, ErrNotReady) {
		t.Errorf("a role that just started: %v", err)
	}

	s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("server not ready")
	}
	defer s.Shutdown()

	// The server accepts any client, so the JWT in the volume only has to exist.
	os.WriteFile(filepath.Join(rt.dir, FileBusJWT), []byte("jwt"), 0o600)

	// Without the NATS server the role keeps trying, and is not ready.
	down, err := rt.Connect("nats://127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	defer down.Close()
	rt.SetReady(true)
	if err := CheckHealth(sock, time.Second); !errors.Is(err, ErrNotReady) {
		t.Errorf("a role without a connection: %v", err)
	}

	up, err := rt.Connect(s.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer up.Close()
	if err := CheckHealth(sock, time.Second); err != nil {
		t.Errorf("a connected role that is ready: %v", err)
	}
	rt.SetReady(false)
	if err := CheckHealth(sock, time.Second); !errors.Is(err, ErrNotReady) {
		t.Errorf("a connected role that is not ready: %v", err)
	}

	if err := CheckHealth(filepath.Join(t.TempDir(), "none.sock"), time.Second); err == nil {
		t.Error("a role that does not answer is healthy")
	}
}

// Retry keeps the loop alive while what it waits for is missing.
func TestRetryMarks(t *testing.T) {
	rt := testRuntime(t)
	l := rt.Loop("main", time.Minute)
	before := l.last.Load()
	tries := 0
	err := Retry(context.Background(), l, time.Second, func(context.Context) error {
		tries++
		if tries < 2 {
			return errors.New("missing")
		}
		return nil
	})
	if err != nil || tries != 2 {
		t.Fatalf("Retry: %v after %d tries", err, tries)
	}
	if l.last.Load() == before {
		t.Error("Retry did not mark the loop")
	}
}
