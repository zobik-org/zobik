package secrets

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/nats-io/nkeys"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/digest"
	"zobik.org/zobik/internal/rpcauth"
	"zobik.org/zobik/internal/signing"
	secretsv1 "zobik.org/zobik/proto/zobik/secrets/v1"
)

// head is a configuration with an entry of each scope and the embedding model's.
const head = `{
  "embedding_model": {"entry": "embeddings"},
  "egress_registry": {
    "embeddings": {"interface": {"name": "a"}, "credential_handle": "embeddings_key"},
    "model":      {"interface": {"name": "a"}, "credential_handle": "openai"},
    "review":     {"interface": {"name": "a"}, "grant_scope": "topics", "grantee_topics": ["code_review"], "credential_handle": "github"},
    "bank":       {"interface": {"name": "a"}, "grant_scope": "provision", "credential_handle": "bank"},
    "other":      {"interface": {"name": "a"}, "credential_handle": "other"}
  }
}`

// iface is the digest of the interface every entry of head implements.
var iface, _ = digest.JSON([]byte(`{"name": "a"}`))

type fixture struct {
	t        *testing.T
	root     nkeys.KeyPair
	rootPub  string
	srv      *Server
	key      string
	head     map[string]json.RawMessage
	entryPub string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	root, _ := nkeys.CreateOperator()
	rootPub, _ := root.PublicKey()
	dir := t.TempDir()
	key, err := NewKey()
	if err != nil {
		t.Fatal(err)
	}
	entry, _ := nkeys.CreateUser()
	entryPub, _ := entry.PublicKey()
	for name, data := range map[string]string{FileKey: key, FileEntryNode: entryPub} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	f := &fixture{t: t, root: root, rootPub: rootPub, key: key, entryPub: entryPub}
	if err := json.Unmarshal([]byte(head), &f.head); err != nil {
		t.Fatal(err)
	}
	f.srv = &Server{Root: rootPub, Dir: dir, Head: func(context.Context) (map[string]json.RawMessage, error) { return f.head, nil }}
	f.write(map[string]string{
		"embeddings_key": "e", "openai": "sk-1", "github": "gh", "bank": "b", "other": "o",
		HandleCatalog: "c", HandleListenerPrefix + "a.example": "cert",
	})
	return f
}

// write rewrites the store, as the console does.
func (f *fixture) write(values map[string]string) {
	f.t.Helper()
	m := map[string][]byte{}
	for k, v := range values {
		m[k] = []byte(v)
	}
	sealed, err := Seal(f.key, m)
	if err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.srv.Dir, FileStore), sealed, 0o600); err != nil {
		f.t.Fatal(err)
	}
}

// caller is a connection authenticated as id under role.
func caller(role, id string) context.Context {
	return peer.NewContext(context.Background(), &peer.Peer{AuthInfo: rpcauth.Info{Identity: id, Role: role}})
}

// node mints a node's identity and a credential with its grants, signed by the root.
func (f *fixture) node(topic string, grants []signing.Grant, proof string) (string, string) {
	f.t.Helper()
	kp, _ := nkeys.CreateUser()
	id, _ := kp.PublicKey()
	cred, err := signing.Sign(f.root, signing.TypeCapability, signing.Capability{
		Sub: id, Topic: topic, ArtifactRef: "sha256:aa", EgressGrants: grants, OperatorProof: proof,
	}, "")
	if err != nil {
		f.t.Fatal(err)
	}
	return id, cred
}

func (f *fixture) proof(artifact string, grants ...signing.Grant) string {
	f.t.Helper()
	var associations []signing.Association
	for _, g := range grants {
		associations = append(associations, g.Association())
	}
	p, err := signing.Sign(f.root, signing.TypeOperatorProof, signing.OperatorProof{
		Act: signing.ActNodeProvision, ArtifactRef: artifact, ProvisionGrants: associations,
	}, "")
	if err != nil {
		f.t.Fatal(err)
	}
	return p
}

func code(err error) codes.Code { return status.Code(err) }

func TestResolve(t *testing.T) {
	f := newFixture(t)
	grants := []signing.Grant{{Slot: "model", Entry: "model", Interface: iface}, {Slot: "repo", Entry: "review", Interface: iface}, {Slot: "pay", Entry: "bank", Interface: iface}}
	reviewer, reviewerCred := f.node("code_review", grants, "")
	writer, writerCred := f.node("writing", grants, "")
	payer, payerCred := f.node("payments", grants, f.proof("sha256:aa", grants[2]))
	// A node built against another interface of the same entry.
	older, olderCred := f.node("writing", []signing.Grant{{Slot: "model", Entry: "model", Interface: "sha256:older"}}, "")
	stale, staleCred := f.node("payments", grants, f.proof("sha256:bb", grants[2]))
	other, _ := nkeys.CreateUser()
	otherID, _ := other.PublicKey()
	operatorChannel, _ := nkeys.CreateUser()
	operatorID, _ := operatorChannel.PublicKey()

	cases := []struct {
		name         string
		ctx          context.Context
		handle, cred string
		want         codes.Code
		value        string
	}{
		{"an open grant", caller(bus.NodeScope.Role, reviewer), "openai", reviewerCred, codes.OK, "sk-1"},
		{"the embedding model's, without a grant", caller(bus.NodeScope.Role, writer), "embeddings_key", writerCred, codes.OK, "e"},
		{"an entry that implements another interface than the slot's", caller(bus.NodeScope.Role, older), "openai", olderCred, codes.PermissionDenied, ""},
		{"an entry the grants do not associate", caller(bus.NodeScope.Role, reviewer), "other", reviewerCred, codes.PermissionDenied, ""},
		{"a topics grant of the node's topic", caller(bus.NodeScope.Role, reviewer), "github", reviewerCred, codes.OK, "gh"},
		{"a topics grant of another topic", caller(bus.NodeScope.Role, writer), "github", writerCred, codes.PermissionDenied, ""},
		{"a provision grant without proof", caller(bus.NodeScope.Role, reviewer), "bank", reviewerCred, codes.PermissionDenied, ""},
		{"a provision grant with its proof", caller(bus.NodeScope.Role, payer), "bank", payerCred, codes.OK, "b"},
		{"a proof of another artifact", caller(bus.NodeScope.Role, stale), "bank", staleCred, codes.PermissionDenied, ""},
		{"another node's credential", caller(bus.NodeScope.Role, otherID), "openai", reviewerCred, codes.PermissionDenied, ""},
		{"a node without credential", caller(bus.NodeScope.Role, reviewer), "openai", "", codes.PermissionDenied, ""},
		{"the Spawner, the embedding model's", caller(bus.SpawnerScope.Role, otherID), "embeddings_key", "", codes.OK, "e"},
		{"the Spawner, its Catalog credential", caller(bus.SpawnerScope.Role, otherID), HandleCatalog, "", codes.OK, "c"},
		{"the Spawner, an entry's", caller(bus.SpawnerScope.Role, otherID), "openai", "", codes.PermissionDenied, ""},
		{"the Entry Node, the embedding model's", caller(bus.ChannelScope.Role, f.entryPub), "embeddings_key", "", codes.OK, "e"},
		{"the Entry Node, an entry's", caller(bus.ChannelScope.Role, f.entryPub), "openai", "", codes.PermissionDenied, ""},
		{"the Operator Channel", caller(bus.ChannelScope.Role, operatorID), "embeddings_key", "", codes.PermissionDenied, ""},
		{"ingress, a listener's", caller(roleIngress, otherID), HandleListenerPrefix + "a.example", "", codes.OK, "cert"},
		{"ingress, an entry's", caller(roleIngress, otherID), "openai", "", codes.PermissionDenied, ""},
		{"another role", caller(bus.ConfigScope.Role, otherID), "embeddings_key", "", codes.PermissionDenied, ""},
		{"no connection identity", context.Background(), "openai", "", codes.Unauthenticated, ""},
	}
	for _, c := range cases {
		got, err := f.srv.Resolve(c.ctx, &secretsv1.SecretRequest{Handle: c.handle, Credential: c.cred})
		if code(err) != c.want {
			t.Errorf("%s: %v, want %s", c.name, err, c.want)
			continue
		}
		if err == nil && string(got.Value) != c.value {
			t.Errorf("%s: %q, want %q", c.name, got.Value, c.value)
		}
	}
}

// Narrowing an entry in the registry withdraws the secret in the same act
// (architecture §3.16.6), and rotating it is rewriting the store.
func TestNarrowAndRotate(t *testing.T) {
	f := newFixture(t)
	id, cred := f.node("code_review", []signing.Grant{{Slot: "repo", Entry: "review", Interface: iface}}, "")
	resolve := func() (*secretsv1.Secret, error) {
		return f.srv.Resolve(caller(bus.NodeScope.Role, id), &secretsv1.SecretRequest{Handle: "github", Credential: cred})
	}
	if v, err := resolve(); err != nil || string(v.Value) != "gh" {
		t.Fatalf("before: %v %v", v, err)
	}
	f.write(map[string]string{"github": "gh-2"})
	if v, err := resolve(); err != nil || string(v.Value) != "gh-2" {
		t.Fatalf("rotated: %v %v", v, err)
	}
	f.head["egress_registry"] = json.RawMessage(`{"review": {"interface": {"name": "b"}, "grant_scope": "topics", "grantee_topics": ["code_review"], "credential_handle": "github"}}`)
	if _, err := resolve(); code(err) != codes.PermissionDenied {
		t.Errorf("another interface: %v", err)
	}
	f.head["egress_registry"] = json.RawMessage(`{"review": {"interface": {"name": "a"}, "grant_scope": "topics", "grantee_topics": ["triage"], "credential_handle": "github"}}`)
	if _, err := resolve(); code(err) != codes.PermissionDenied {
		t.Errorf("narrowed: %v", err)
	}
}

func TestCheckAndNotFound(t *testing.T) {
	f := newFixture(t)
	id, cred := f.node("writing", []signing.Grant{{Slot: "model", Entry: "model", Interface: iface}}, "")
	ctx := caller(bus.NodeScope.Role, id)
	if _, err := f.srv.Check(ctx, &secretsv1.SecretRequest{Handle: "openai", Credential: cred}); err != nil {
		t.Errorf("check: %v", err)
	}
	f.write(map[string]string{})
	if _, err := f.srv.Check(ctx, &secretsv1.SecretRequest{Handle: "openai", Credential: cred}); code(err) != codes.NotFound {
		t.Errorf("a handle in scope without value: %v", err)
	}
}

func TestSealRoundTrip(t *testing.T) {
	key, _ := NewKey()
	other, _ := NewKey()
	sealed, err := Seal(key, map[string][]byte{"a": []byte("1")})
	if err != nil {
		t.Fatal(err)
	}
	got, err := Unseal(key, sealed)
	if err != nil || string(got["a"]) != "1" {
		t.Fatalf("%v %v", got, err)
	}
	if _, err := Unseal(other, sealed); err == nil {
		t.Error("another key opened the store")
	}
}
