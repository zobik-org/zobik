package sidecar

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"zobik.org/zobik/internal/claim"
	"zobik.org/zobik/internal/egress"
	"zobik.org/zobik/internal/signing"
	nriv1 "zobik.org/zobik/proto/zobik/nri/v1"
)

// callsDir is where the Integration Sidecar keeps the copy of each call's body,
// in its private volume, which the Logic Container does not reach
// (implementation §3, Context Store and egress).
const callsDir = "calls"

// Call runs the procedure of architecture §3.16.4 on a call the Logic
// Container names by slot: the slot's association, the operation and its
// fields, the rules, and with allow, the execution, whose charge goes into the
// task's metrics (implementation §3, Context Store and egress).
func (sv *service) Call(ctx context.Context, req *nriv1.EgressRequest) (*nriv1.EgressResponse, error) {
	n := sv.n
	a, done, err := n.callTask(req.GetTaskId())
	if err != nil {
		return nil, err
	}
	defer done()
	// The work area opens inside the shared volume: a work area the Logic
	// Container replaced with a symlink out of it does not open.
	area, err := n.areas.OpenRoot(filepath.Join(tasksDir, a.TaskID))
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "the work area of %s: %v", a.TaskID, err)
	}
	defer area.Close()

	// Step 2: the slot is associated, with an entry that still implements its
	// interface and still reaches the node.
	key, e, version, err := n.served(req.GetSlot())
	if err != nil {
		return nil, err
	}

	// Step 3: the operation, the fields and the body, measured on a copy the
	// Logic Container does not reach.
	call, err := e.NewCall(req.GetOperation(), req.GetFields(), req.GetBody() != "")
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	var body *os.File
	if req.GetBody() != "" {
		if body, call.BodySize, call.BodyDigest, err = n.copyBody(area, req.GetBody(), e.MaxBodySize); err != nil {
			return nil, err
		}
		defer func() {
			body.Close()
			os.Remove(body.Name())
		}()
	}

	// Step 4: the rules, over the call, the lists and the time.
	now := time.Now().UTC()
	o := e.Decide(call, egress.Lists, now)
	switch o.Then {
	case egress.Allow:
	case egress.Deny:
		return nil, status.Error(codes.PermissionDenied, evaluated(o, now, version))
	default:
		// Steps 6 and 7: the audience approves, and what executes is the
		// request checked again against the current registry.
		if e, call, err = n.gate(ctx, a, req, &gated{key: key, entry: e, call: call, outcome: o, at: now, version: version}); err != nil {
			return nil, err
		}
	}

	// Step 5: the execution, with the response's body left in the work area.
	var out *os.File
	if e.Interface.Operations[call.Operation].Body.Response {
		if out, err = area.OpenFile("zobik-response-"+uuid.NewString(), os.O_CREATE|os.O_EXCL|os.O_WRONLY|syscall.O_NOFOLLOW, 0o600); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		defer out.Close()
	}
	var dst io.Writer
	if out != nil {
		dst = out
	}
	res, err := n.egress.Execute(ctx, e, call, body, dst)
	// A call whose response could not be read was executed all the same: it
	// carries its charge with the error.
	if res != nil {
		ch := res.Charge
		for _, err := range ch.Errors {
			log.Printf("sidecar: billing a call of %s on %s: %v", req.GetSlot(), a.TaskID, err)
		}
		a.Charge(claim.Spent{TokensIn: ch.TokensIn, TokensOut: ch.TokensOut, Cost: ch.Cost, BillingErrors: ch.BillingErrors})
	}
	if err != nil {
		if out != nil {
			_ = area.Remove(filepath.Base(out.Name()))
		}
		return nil, executeError(err)
	}

	fields, err := json.Marshal(res.Response)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	resp := &nriv1.EgressResponse{Fields: fields}
	if out != nil {
		resp.Body = filepath.Base(out.Name())
	}
	return resp, nil
}

// executeError is the status of a call that failed on executing. A missing
// rate table returns like a deny (implementation §8.5), and a response that
// could not be read, as aborted: the destination executed the call.
func executeError(err error) error {
	switch {
	case errors.Is(err, egress.ErrNoTable):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, egress.ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, egress.ErrUnreadable):
		return status.Error(codes.Aborted, err.Error())
	}
	return status.Error(codes.Unavailable, err.Error())
}

// served is the entry the slot is associated with in the current registry, with
// its key and the version read, if it still implements the slot's interface and
// still reaches the node (architecture §3.16.4, step 2).
func (n *node) served(slot string) (string, *egress.Entry, string, error) {
	g, ok := grant(n.cred, slot)
	if !ok {
		return "", nil, "", status.Errorf(codes.PermissionDenied, "slot %q is not associated in the node's egress_grants", slot)
	}
	reg, version, err := n.cfg.registry()
	if err != nil {
		return "", nil, "", status.Error(codes.Unavailable, err.Error())
	}
	e := reg[g.Entry]
	if e == nil || !e.Reach().Serves(g, n.cred, n.root) {
		return "", nil, "", status.Errorf(codes.PermissionDenied, "entry %q no longer serves slot %q", g.Entry, slot)
	}
	return g.Entry, e, version, nil
}

// evaluated names an outcome with the rule that produced it, the evaluation
// time and the version read (architecture §3.16.4).
func evaluated(o egress.Outcome, at time.Time, version string) string {
	return fmt.Sprintf("%s, evaluated at %s under config_version %s", o, at.Format(time.RFC3339), version)
}

// grant is the slot's association in the credential.
func grant(cred *signing.Capability, slot string) (signing.Grant, bool) {
	for _, g := range cred.EgressGrants {
		if g.Slot == slot {
			return g, true
		}
	}
	return signing.Grant{}, false
}

// callTask is a task the node holds and is not closing: a call is charged to
// it, and its close waits until done is called (implementation §3, Context
// Store and egress).
func (n *node) callTask(taskID string) (*claim.Assignment, func(), error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	t := n.tasks[taskID]
	if t == nil || t.closing {
		return nil, nil, status.Error(codes.FailedPrecondition, "the node holds no task "+taskID)
	}
	t.calls.Add(1)
	return t.a, t.calls.Done, nil
}

// copyBody copies the call's body from the task's work area to the private
// volume, opening it inside the area without following a symlink, and measures
// the copy (implementation §3, Context Store and egress).
func (n *node) copyBody(area *os.Root, name string, limit int64) (*os.File, int64, string, error) {
	invalid := func(format string, args ...any) error {
		return status.Errorf(codes.InvalidArgument, "body: "+format, args...)
	}
	// Opened once, and checked on the open file: a name the Logic Container
	// swaps meanwhile is not what is measured. A FIFO does not block the open.
	src, err := area.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return nil, 0, "", invalid("%s is not in the task's work area", name)
	case err != nil:
		return nil, 0, "", invalid("%s does not open as a file of the work area", name)
	}
	defer src.Close()
	if info, err := src.Stat(); err != nil || !info.Mode().IsRegular() {
		return nil, 0, "", invalid("%s is not a regular file", name)
	}
	dir := filepath.Join(n.private, callsDir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, 0, "", status.Error(codes.Internal, err.Error())
	}
	dst, err := os.CreateTemp(dir, "body-")
	if err != nil {
		return nil, 0, "", status.Error(codes.Internal, err.Error())
	}
	fail := func(err error) (*os.File, int64, string, error) {
		dst.Close()
		os.Remove(dst.Name())
		return nil, 0, "", err
	}
	h := sha256.New()
	size, err := io.Copy(io.MultiWriter(dst, h), io.LimitReader(src, limit+1))
	if err != nil {
		return fail(status.Error(codes.Internal, err.Error()))
	}
	// A call body over max_body_size is rejected (architecture §3.16.2).
	if size > limit {
		return fail(invalid("it exceeds max_body_size, %d bytes", limit))
	}
	if _, err := dst.Seek(0, io.SeekStart); err != nil {
		return fail(status.Error(codes.Internal, err.Error()))
	}
	return dst, size, "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}

// clearCalls deletes the bodies a previous run left, at startup.
func clearCalls(private string) {
	if err := os.RemoveAll(filepath.Join(private, callsDir)); err != nil {
		log.Printf("sidecar: deleting %s: %v", callsDir, err)
	}
}

// checkCredentials resolves the handle of each entry the node's grants
// associate, without its value, and the node does not start if one does not
// resolve (architecture §3.16.5). It waits while the secrets role is out of reach.
func checkCredentials(ctx context.Context, cfg *config, cred *signing.Capability, root string, check func(context.Context, string) error) error {
	reg, _, err := cfg.registry()
	if err != nil {
		return err
	}
	for _, g := range cred.EgressGrants {
		e := reg[g.Entry]
		if e == nil || e.CredentialHandle == "" || !e.Reach().Serves(g, cred, root) {
			continue
		}
		for {
			opCtx, cancel := context.WithTimeout(ctx, opTimeout)
			err := check(opCtx, e.CredentialHandle)
			cancel()
			if err == nil {
				break
			}
			if status.Code(err) != codes.Unavailable && status.Code(err) != codes.DeadlineExceeded {
				return fmt.Errorf("sidecar: the credential of %s does not resolve: %w", g.Entry, err)
			}
			select {
			case <-ctx.Done():
				return fmt.Errorf("sidecar: checking the credential of %s: %w", g.Entry, err)
			case <-time.After(time.Second):
			}
		}
	}
	return nil
}
