package signing

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/nats-io/nkeys"
)

type fixture struct {
	t    *testing.T
	root nkeys.KeyPair
	pub  string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	root, err := nkeys.CreateOperator()
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{t: t, root: root, pub: pub(t, root)}
}

func pub(t *testing.T, kp nkeys.KeyPair) string {
	t.Helper()
	k, err := kp.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func key(t *testing.T) nkeys.KeyPair {
	t.Helper()
	kp, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	return kp
}

func (f *fixture) sign(kp nkeys.KeyPair, typ Type, claims any, signer string) string {
	f.t.Helper()
	tok, err := Sign(kp, typ, claims, signer)
	if err != nil {
		f.t.Fatal(err)
	}
	return tok
}

func (f *fixture) roleCert(kp nkeys.KeyPair, role string) string {
	return f.sign(f.root, TypeRoleCertificate, RoleCertificate{Sub: pub(f.t, kp), Iat: 1, Role: role}, "")
}

func TestAdmittedForms(t *testing.T) {
	f := newFixture(t)
	spawner, broker, node := key(t), key(t), key(t)
	spawnerCert := f.roleCert(spawner, RoleSpawner)
	brokerCert := f.roleCert(broker, RoleTaskBroker)

	if _, err := VerifyRoleCertificate(spawnerCert, f.pub); err != nil {
		t.Errorf("role_certificate: %v", err)
	}

	proof := f.sign(f.root, TypeOperatorProof, OperatorProof{Iat: 1, Act: "config_change", TaskID: "t1", DataDigest: "sha256:00"}, "")
	if _, err := VerifyOperatorProof(proof, f.pub); err != nil {
		t.Errorf("operator_proof: %v", err)
	}

	structural := f.sign(f.root, TypeCapability, Capability{Sub: pub(t, node), Topic: "hitl_contact_user"}, "")
	if _, err := VerifyCapability(structural, f.pub); err != nil {
		t.Errorf("structural capability: %v", err)
	}

	emergent := f.sign(spawner, TypeCapability, Capability{Sub: pub(t, node), Topic: "summarize"}, spawnerCert)
	v, err := VerifyCapability(emergent, f.pub)
	if err != nil {
		t.Fatalf("emergent capability: %v", err)
	}
	if v.Header.Kid != pub(t, spawner) || v.Claims.Topic != "summarize" {
		t.Errorf("emergent capability decoded as %+v", v)
	}

	scope := ScopeToken{Sub: pub(t, node), TraceID: "tr", Section: "tk"}
	if _, err := VerifyScopeToken(f.sign(broker, TypeScopeToken, scope, brokerCert), f.pub); err != nil {
		t.Errorf("scope_token from task_broker: %v", err)
	}
	if _, err := VerifyScopeToken(f.sign(node, TypeScopeToken, scope, structural), f.pub); err != nil {
		t.Errorf("scope_token from structural channel: %v", err)
	}

	grant := f.sign(f.root, TypeOperatorProof, OperatorProof{Act: ActNodeProvision, Audience: "hitl_contact_client", EntryTopic: "support"}, "")
	channel := f.sign(spawner, TypeCapability, Capability{Sub: pub(t, node), Topic: "hitl_contact_client", OperatorProof: grant}, spawnerCert)
	if _, err := VerifyScopeToken(f.sign(node, TypeScopeToken, scope, channel), f.pub); err != nil {
		t.Errorf("scope_token from emergent channel: %v", err)
	}
}

func TestRejectedForms(t *testing.T) {
	f := newFixture(t)
	spawner, broker, node, other := key(t), key(t), key(t), key(t)
	spawnerCert := f.roleCert(spawner, RoleSpawner)
	brokerCert := f.roleCert(broker, RoleTaskBroker)
	scope := ScopeToken{Sub: pub(t, node), TraceID: "tr", Section: "tk"}
	noEntry := f.sign(f.root, TypeOperatorProof, OperatorProof{Act: ActNodeProvision, Audience: "hitl_contact_client"}, "")
	channelNoEntry := f.sign(spawner, TypeCapability, Capability{Sub: pub(t, node), OperatorProof: noEntry}, spawnerCert)

	cases := []struct {
		name   string
		verify func() error
		want   error
	}{
		{"scope token presented as capability", func() error {
			_, err := VerifyCapability(f.sign(broker, TypeScopeToken, scope, brokerCert), f.pub)
			return err
		}, ErrType},
		{"role certificate not signed by the root", func() error {
			_, err := VerifyRoleCertificate(f.sign(other, TypeRoleCertificate, RoleCertificate{Sub: pub(t, spawner), Role: RoleSpawner}, ""), f.pub)
			return err
		}, ErrUntrustedKey},
		{"operator proof with signer_credential", func() error {
			_, err := VerifyOperatorProof(f.sign(spawner, TypeOperatorProof, OperatorProof{Act: "config_change"}, spawnerCert), f.pub)
			return err
		}, ErrForm},
		{"capability signed by a task broker", func() error {
			_, err := VerifyCapability(f.sign(broker, TypeCapability, Capability{Sub: pub(t, node)}, brokerCert), f.pub)
			return err
		}, ErrForm},
		{"capability whose kid is not the certificate's sub", func() error {
			_, err := VerifyCapability(f.sign(other, TypeCapability, Capability{Sub: pub(t, node)}, spawnerCert), f.pub)
			return err
		}, ErrUntrustedKey},
		{"scope token signed by a spawner", func() error {
			_, err := VerifyScopeToken(f.sign(spawner, TypeScopeToken, scope, spawnerCert), f.pub)
			return err
		}, ErrForm},
		{"scope token signed by the root", func() error {
			_, err := VerifyScopeToken(f.sign(f.root, TypeScopeToken, scope, ""), f.pub)
			return err
		}, ErrForm},
		{"scope token from emergent channel without entry_topic", func() error {
			_, err := VerifyScopeToken(f.sign(node, TypeScopeToken, scope, channelNoEntry), f.pub)
			return err
		}, ErrForm},
	}
	for _, c := range cases {
		if err := c.verify(); !errors.Is(err, c.want) {
			t.Errorf("%s: got %v, want %v", c.name, err, c.want)
		}
	}
}

func TestTamperedAndForeignAlg(t *testing.T) {
	f := newFixture(t)
	tok := f.sign(f.root, TypeRoleCertificate, RoleCertificate{Sub: "x", Role: RoleSpawner}, "")
	parts := strings.Split(tok, ".")

	forged, _ := json.Marshal(RoleCertificate{Sub: "x", Role: RoleTaskBroker})
	tampered := parts[0] + "." + b64.EncodeToString(forged) + "." + parts[2]
	if _, err := VerifyRoleCertificate(tampered, f.pub); !errors.Is(err, ErrSignature) {
		t.Errorf("tampered payload: got %v", err)
	}

	hs, _ := json.Marshal(map[string]string{"alg": "HS256", "typ": string(TypeRoleCertificate), "kid": f.pub})
	if _, err := VerifyRoleCertificate(b64.EncodeToString(hs)+"."+parts[1]+"."+parts[2], f.pub); !errors.Is(err, ErrAlg) {
		t.Errorf("foreign alg: got %v", err)
	}
}

func TestUnknownFieldsAreIgnored(t *testing.T) {
	f := newFixture(t)
	claims := map[string]any{"sub": "x", "iat": 1, "role": RoleSpawner, "added_later": true}
	v, err := VerifyRoleCertificate(f.sign(f.root, TypeRoleCertificate, claims, ""), f.pub)
	if err != nil || v.Claims.Role != RoleSpawner {
		t.Fatalf("got %+v, %v", v, err)
	}
}
