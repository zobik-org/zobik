//go:build dev

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/deploy"
	"zobik.org/zobik/internal/role"
	"zobik.org/zobik/internal/rpcauth"
	"zobik.org/zobik/internal/signing"
	contextv1 "zobik.org/zobik/proto/zobik/context/v1"
	nriv1 "zobik.org/zobik/proto/zobik/nri/v1"
)

// zobik context-probe exercises the Context Store from inside the network as a
// channel that opens a trace would: it writes a portion under its root's token,
// reads it, lists it, tries to read another trace, and purges. The channel's
// credential is minted by the root, as the harness does for what no component
// issues yet; so it asks for the operator's username and password.
func init() {
	devCommands["context-probe"] = contextProbeCmd
}

const (
	probeTokenPath = "/run/zobik/token.jws"
	probeTracePath = "/run/zobik/trace"
)

func contextProbeCmd(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("context-probe", flag.ContinueOnError)
	var n networkFlags
	n.register(fs)
	inside := fs.Bool("inside", false, "run inside the network (the ephemeral container does)")
	keep := fs.Bool("keep", false, "leave the portion written instead of purging the trace")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *inside {
		return contextProbeInside(ctx, *keep)
	}
	if err := n.resolve(); err != nil {
		return err
	}
	user, password, err := readCredentials(false)
	if err != nil {
		return err
	}
	root, err := deploy.OpenRoot(n.dir, user, password)
	if err != nil {
		return err
	}
	creds, err := deploy.EphemeralIdentity(n.dir, bus.ChannelScope.Role, devIdentityLifetime)
	if err != nil {
		return err
	}
	kp, err := nkeys.ParseDecoratedNKey(creds)
	if err != nil {
		return err
	}
	id, _ := kp.PublicKey()
	cred, err := signing.Sign(root, signing.TypeCapability, signing.Capability{Sub: id, Iat: time.Now().Unix(), Topic: "hitl_contact_operator"}, "")
	if err != nil {
		return err
	}
	trace := make([]byte, 16)
	rand.Read(trace)
	traceID := hex.EncodeToString(trace)
	token, err := signing.Sign(kp, signing.TypeScopeToken, signing.ScopeToken{Sub: id, Iat: time.Now().Unix(), TraceID: traceID, Section: traceID}, cred)
	if err != nil {
		return err
	}
	cmd := []string{"context-probe", "--inside"}
	if *keep {
		cmd = append(cmd, "--keep")
	}
	return runOnBusAs(ctx, n, bus.ChannelScope.Role, creds, cmd, false,
		deploy.File{Path: probeTokenPath, Mode: 0o600, Data: []byte(token)},
		deploy.File{Path: probeTracePath, Mode: 0o600, Data: []byte(traceID)})
}

func contextProbeInside(ctx context.Context, keep bool) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	creds, err := os.ReadFile(deploy.CredsPath)
	if err != nil {
		return err
	}
	userJWT, err := jwt.ParseDecoratedJWT(creds)
	if err != nil {
		return err
	}
	kp, err := nkeys.ParseDecoratedNKey(creds)
	if err != nil {
		return err
	}
	token, err := os.ReadFile(probeTokenPath)
	if err != nil {
		return err
	}
	tok := string(token)
	trace, err := os.ReadFile(probeTracePath)
	if err != nil {
		return err
	}
	// The root's section is its TraceID (architecture §3.14.3).
	traceID := string(trace)
	conn, err := grpc.NewClient(role.AddrContext, grpc.WithTransportCredentials(&rpcauth.Client{
		JWT: func() (string, error) { return userJWT, nil }, Key: kp,
	}))
	if err != nil {
		return err
	}
	defer conn.Close()
	c := contextv1.NewContextStoreClient(conn)
	ref := &nriv1.ContextRef{TraceId: traceID, TaskId: traceID, Name: "probe"}
	fmt.Printf("trace %s\n", traceID)

	w, err := c.Write(ctx)
	if err != nil {
		return err
	}
	for _, part := range []*contextv1.StoreWrite{
		{Body: &contextv1.StoreWrite_Header{Header: &contextv1.StoreWriteHeader{Token: tok, Name: "probe"}}},
		{Body: &contextv1.StoreWrite_Chunk{Chunk: []byte("hola desde la red")}},
	} {
		if err := w.Send(part); err != nil && !errors.Is(err, io.EOF) {
			return fmt.Errorf("write: %w", err)
		}
	}
	if _, err := w.CloseAndRecv(); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	fmt.Println("write:  ok")

	got, err := probeRead(ctx, c, tok, ref)
	fmt.Printf("read:   %q %s\n", got, outcome(err))

	list, err := c.List(ctx, &contextv1.StoreList{Token: tok, TaskId: traceID})
	fmt.Printf("list:   %d portion(s) %s\n", len(list.GetRefs()), outcome(err))

	other := &nriv1.ContextRef{TraceId: "00000000000000000000000000000000", TaskId: traceID, Name: "probe"}
	_, err = probeRead(ctx, c, tok, other)
	fmt.Printf("read of another trace: %s\n", outcome(err))

	if keep {
		return nil
	}
	_, err = c.Purge(ctx, &contextv1.StorePurge{Token: tok})
	fmt.Printf("purge:  %s\n", outcome(err))

	_, err = probeRead(ctx, c, tok, ref)
	fmt.Printf("read after the purge: %s\n", outcome(err))
	return nil
}

func probeRead(ctx context.Context, c contextv1.ContextStoreClient, tok string, ref *nriv1.ContextRef) (string, error) {
	s, err := c.Read(ctx, &contextv1.StoreRead{Token: tok, Ref: ref})
	if err != nil {
		return "", err
	}
	var out bytes.Buffer
	for {
		ch, err := s.Recv()
		if errors.Is(err, io.EOF) {
			return out.String(), nil
		}
		if err != nil {
			return "", err
		}
		out.Write(ch.GetData())
	}
}

func outcome(err error) string {
	if err == nil {
		return "ok"
	}
	return status.Code(err).String()
}
