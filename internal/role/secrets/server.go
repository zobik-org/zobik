package secrets

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/rpcauth"
	"zobik.org/zobik/internal/signing"
	secretsv1 "zobik.org/zobik/proto/zobik/secrets/v1"
)

// The handles no entry names, which the role resolves by the caller's role
// (implementation §1.2.18).
const (
	HandleCatalog        = "zobik/catalog"
	HandleListenerPrefix = "zobik/listener/"
)

// roleIngress is the ingress proxy's role, which resolves the listener
// certificates (implementation §2.3).
const roleIngress = "ingress"

// Server is the secrets role's service (implementation §1.2.18).
type Server struct {
	secretsv1.UnimplementedSecretsServer
	// Root is the deployment root every credential verifies against.
	Root string
	// Dir is the role's volume, with the Secret Store and its key.
	Dir string
	// Head reads the current Global Configuration, on each request, so narrowing
	// an entry withdraws its secret on the next one (architecture §3.16.6).
	Head func(ctx context.Context) (map[string]json.RawMessage, error)

	mu    sync.Mutex
	store cached
	// verified caches each credential's verification: the service has no
	// session, and a node presents the same one on each request.
	verified map[string]*signing.Verified[signing.Capability]
}

// cached is the Secret Store as last decrypted, with the file it came from.
type cached struct {
	sealed []byte
	values map[string][]byte
}

// maxVerified bounds the credentials kept verified; past it the cache restarts.
const maxVerified = 1024

func (s *Server) Resolve(ctx context.Context, req *secretsv1.SecretRequest) (*secretsv1.Secret, error) {
	value, err := s.resolve(ctx, req)
	if err != nil {
		return nil, err
	}
	return &secretsv1.Secret{Value: value}, nil
}

// Check evaluates as Resolve and returns no value: the Integration Sidecar's
// startup check (architecture §3.16.5).
func (s *Server) Check(ctx context.Context, req *secretsv1.SecretRequest) (*secretsv1.CheckAck, error) {
	if _, err := s.resolve(ctx, req); err != nil {
		return nil, err
	}
	return &secretsv1.CheckAck{}, nil
}

func (s *Server) resolve(ctx context.Context, req *secretsv1.SecretRequest) ([]byte, error) {
	info, ok := rpcauth.FromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "unauthenticated connection")
	}
	if req.Handle == "" {
		return nil, status.Error(codes.InvalidArgument, "no handle")
	}
	if err := s.authorize(ctx, info, req); err != nil {
		return nil, err
	}
	values, err := s.values()
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	value, ok := values[req.Handle]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "handle %q has no value", req.Handle)
	}
	return value, nil
}

// authorize decides whether the caller reaches the handle: a node, by its
// grants; a role, by a closed list (implementation §1.2.18).
func (s *Server) authorize(ctx context.Context, info rpcauth.Info, req *secretsv1.SecretRequest) error {
	denied := status.Errorf(codes.PermissionDenied, "handle %q is out of scope", req.Handle)
	if info.Role == roleIngress {
		if strings.HasPrefix(req.Handle, HandleListenerPrefix) {
			return nil
		}
		return denied
	}
	head, err := s.Head(ctx)
	if err != nil {
		return status.Error(codes.Unavailable, err.Error())
	}
	space := embeddingHandle(head)
	switch info.Role {
	case bus.NodeScope.Role:
		cred, err := s.verify(req.Credential)
		if err != nil {
			return status.Error(codes.PermissionDenied, err.Error())
		}
		if cred.Claims.Sub != info.Identity {
			return status.Error(codes.PermissionDenied, "the credential is not the caller's")
		}
		if req.Handle == space || granted(head, cred.Claims, s.Root, req.Handle) {
			return nil
		}
	case bus.SpawnerScope.Role:
		if req.Handle == space || req.Handle == HandleCatalog {
			return nil
		}
	case bus.ChannelScope.Role:
		// Of the channels, only the Entry Node, which generates the entry
		// subtask's embedding (implementation §1.2.15).
		entry, err := os.ReadFile(filepath.Join(s.Dir, FileEntryNode))
		if err == nil && strings.TrimSpace(string(entry)) == info.Identity && req.Handle == space {
			return nil
		}
	}
	return denied
}

// verify verifies a capability credential against the root, once per credential.
func (s *Server) verify(token string) (*signing.Verified[signing.Capability], error) {
	if token == "" {
		return nil, errors.New("a node presents its capability credential")
	}
	s.mu.Lock()
	v, ok := s.verified[token]
	s.mu.Unlock()
	if ok {
		return v, nil
	}
	v, err := signing.VerifyCapability(token, s.Root)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	if s.verified == nil || len(s.verified) >= maxVerified {
		s.verified = map[string]*signing.Verified[signing.Capability]{}
	}
	s.verified[token] = v
	s.mu.Unlock()
	return v, nil
}

// entry is what the role reads of an egress_registry entry.
type entry struct {
	GrantScope       string   `json:"grant_scope"`
	GranteeTopics    []string `json:"grantee_topics"`
	CredentialHandle string   `json:"credential_handle"`
}

func registry(head map[string]json.RawMessage) map[string]entry {
	var r map[string]entry
	if raw, ok := head["egress_registry"]; ok {
		_ = json.Unmarshal(raw, &r)
	}
	return r
}

// embeddingHandle is the handle of the entry embedding_model names, or empty.
func embeddingHandle(head map[string]json.RawMessage) string {
	var m struct {
		Entry string `json:"entry"`
	}
	if raw, ok := head["embedding_model"]; !ok || json.Unmarshal(raw, &m) != nil {
		return ""
	}
	return registry(head)[m.Entry].CredentialHandle
}

// granted reports whether an entry that names handle is associated in the
// node's egress_grants and still reaches it in the current registry: the
// predicate of step 2 of architecture §3.16.4. The grants come from the
// credential the Spawner signed, never from what the caller declares
// (architecture §3.16.6).
func granted(head map[string]json.RawMessage, cred signing.Capability, root, handle string) bool {
	reg := registry(head)
	for _, g := range cred.EgressGrants {
		e, ok := reg[g.Entry]
		if !ok || e.CredentialHandle != handle {
			continue
		}
		switch e.GrantScope {
		case "", "open":
			return true
		case "topics":
			if slices.Contains(e.GranteeTopics, cred.Topic) {
				return true
			}
		case "provision":
			if provisioned(cred, root, g) {
				return true
			}
		}
	}
	return false
}

// provisioned reports whether the credential carries the proof of a
// node_provision that associates g and names the running artifact_ref
// (implementation §9.2): a Spawner that lies about the grants cannot fabricate it.
func provisioned(cred signing.Capability, root string, g signing.Grant) bool {
	proof, err := signing.VerifyOperatorProof(cred.OperatorProof, root)
	if err != nil {
		return false
	}
	p := proof.Claims
	return p.Act == signing.ActNodeProvision && p.ArtifactRef == cred.ArtifactRef && slices.Contains(p.ProvisionGrants, g)
}

// values reads the Secret Store on each request, and decrypts it again when the
// console has rewritten it; it keeps no value of an earlier file.
func (s *Server) values() (map[string][]byte, error) {
	sealed, err := os.ReadFile(filepath.Join(s.Dir, FileStore))
	if errors.Is(err, os.ErrNotExist) {
		return map[string][]byte{}, nil
	}
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.store.values != nil && bytes.Equal(sealed, s.store.sealed) {
		return s.store.values, nil
	}
	key, err := os.ReadFile(filepath.Join(s.Dir, FileKey))
	if err != nil {
		return nil, fmt.Errorf("secrets: key: %w", err)
	}
	values, err := Unseal(string(key), sealed)
	if err != nil {
		return nil, err
	}
	s.store = cached{sealed: sealed, values: values}
	return values, nil
}
