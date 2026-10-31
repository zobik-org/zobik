package deploy

import (
	"slices"
	"strings"
	"testing"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/role"
	"zobik.org/zobik/internal/signing"
)

func testMaterial(t *testing.T) *material {
	t.Helper()
	root, err := nkeys.CreateOperator()
	if err != nil {
		t.Fatal(err)
	}
	a, err := bus.NewAccount(root, "test", bus.RoleScopes)
	if err != nil {
		t.Fatal(err)
	}
	return &material{root: root, account: a}
}

func files(fs []File) map[string]string {
	out := map[string]string{}
	for _, f := range fs {
		out[f.Path] = string(f.Data)
	}
	return out
}

// What each role receives verifies against the root, as the admitted forms of
// implementation §9.1 require.
func TestMintRole(t *testing.T) {
	m := testMaterial(t)
	rootPub, _ := m.root.PublicKey()
	for _, r := range Roles {
		pub, fs, err := m.mintRole(r, "zobik:test")
		if err != nil {
			t.Fatalf("%s: %v", r.Unit, err)
		}
		got := files(fs)
		if got[role.FileRootPub] != rootPub {
			t.Errorf("%s: root.pub is %q", r.Unit, got[role.FileRootPub])
		}
		seed, err := nkeys.FromSeed([]byte(got[role.FileIdentity]))
		if err != nil {
			t.Fatalf("%s: identity: %v", r.Unit, err)
		}
		if p, _ := seed.PublicKey(); p != pub {
			t.Errorf("%s: the seed is not the identity's", r.Unit)
		}

		uc, err := jwt.DecodeUserClaims(got[role.FileBusJWT])
		if err != nil {
			t.Fatalf("%s: bus.jwt: %v", r.Unit, err)
		}
		signer, _ := m.account.SigningKeys[r.Scope.Role].PublicKey()
		if uc.Subject != pub || uc.Issuer != signer || uc.IssuerAccount != m.account.Public {
			t.Errorf("%s: the JWT is not the identity's under its scope", r.Unit)
		}

		cred, hasCred := got[role.FileCredential]
		if hasCred != (r.Topic != "") {
			t.Errorf("%s: credential present = %v", r.Unit, hasCred)
		}
		if hasCred {
			v, err := signing.VerifyCapability(cred, rootPub)
			if err != nil {
				t.Fatalf("%s: credential: %v", r.Unit, err)
			}
			if v.Header.SignerCredential != "" {
				t.Errorf("%s: a structural credential verifies directly against the root", r.Unit)
			}
			if v.Claims.Sub != pub || v.Claims.Topic != r.Topic || v.Claims.ModelID != "" || v.Claims.CapabilityEmbedding != "" {
				t.Errorf("%s: credential claims %+v", r.Unit, v.Claims)
			}
		}

		cert, hasCert := got[role.FileRoleCertificate]
		if hasCert != (r.Certificate != "") {
			t.Errorf("%s: certificate present = %v", r.Unit, hasCert)
		}
		if hasCert {
			v, err := signing.VerifyRoleCertificate(cert, rootPub)
			if err != nil {
				t.Fatalf("%s: certificate: %v", r.Unit, err)
			}
			if v.Claims.Sub != pub || v.Claims.Role != r.Certificate {
				t.Errorf("%s: certificate claims %+v", r.Unit, v.Claims)
			}
		}
	}
}

// A role that issues identities keeps its scope's signing key, and minting it
// again leaves the account with only the new one.
func TestMintIssuer(t *testing.T) {
	m := testMaterial(t)
	for _, r := range Roles {
		if r.Issues == nil {
			continue
		}
		var keys []string
		for range 2 {
			_, fs, err := m.mintRole(r, "zobik:test")
			if err != nil {
				t.Fatal(err)
			}
			kp, err := nkeys.FromSeed([]byte(files(fs)[role.FileSigningKey]))
			if err != nil {
				t.Fatalf("%s: signing key: %v", r.Unit, err)
			}
			pub, _ := kp.PublicKey()
			keys = append(keys, pub)
		}
		ac, err := jwt.DecodeAccountClaims(m.account.JWT)
		if err != nil {
			t.Fatal(err)
		}
		var scoped []string
		for _, k := range ac.SigningKeys.Keys() {
			if s, ok := ac.SigningKeys.GetScope(k); ok && s.(*jwt.UserScope).Role == r.Issues.Role {
				scoped = append(scoped, k)
			}
		}
		if len(scoped) != 1 || scoped[0] != keys[1] || keys[0] == keys[1] {
			t.Errorf("%s: the account's %s keys are %v after minting %v", r.Unit, r.Issues.Role, scoped, keys)
		}
		return
	}
	t.Fatal("no role issues identities")
}

// The channel of the panel competes from round 1 on, the final one included:
// until the Spawner closes the vacant rounds, no upper limit.
func TestOperatorChannelWindow(t *testing.T) {
	m := testMaterial(t)
	rootPub, _ := m.root.PublicKey()
	for _, r := range Roles {
		if r.Topic != "hitl_contact_operator" {
			continue
		}
		_, fs, err := m.mintRole(r, "zobik:test")
		if err != nil {
			t.Fatal(err)
		}
		v, err := signing.VerifyCapability(files(fs)[role.FileCredential], rootPub)
		if err != nil {
			t.Fatal(err)
		}
		w := v.Claims.AttemptWindow
		if w == nil || *w.From != 1 || w.To != nil || !*w.FinalRound {
			t.Errorf("attempt_window %+v", w)
		}
		return
	}
	t.Fatal("no operator channel")
}

func TestRevoke(t *testing.T) {
	m := testMaterial(t)
	user, _ := nkeys.CreateUser()
	pub, _ := user.PublicKey()
	updated, err := bus.Revoke(m.root, m.account.JWT, pub)
	if err != nil {
		t.Fatal(err)
	}
	ac, err := jwt.DecodeAccountClaims(updated)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := ac.Revocations[pub]; !ok {
		t.Error("the identity is not revoked")
	}
}

// The Entry Node resolves the embedding model's credential, so it waits for the
// secrets role (implementation §6, The roles of the binary).
func TestEntryNodeWaitsForSecrets(t *testing.T) {
	for _, r := range Roles {
		if r.Unit == "channel_user" && !slices.Contains(r.WaitsFor, unitSecrets) {
			t.Errorf("channel_user waits for %v", r.WaitsFor)
		}
	}
}

func TestHandleForm(t *testing.T) {
	for h, want := range map[string]bool{
		"openai": true, "zobik/listener/a.example": true, "a_b-c.d": true,
		"": false, "a//b": false, "a/../b": false, "./a": false, "a/": false, "with space": false,
		strings.Repeat("a", 129): false,
	} {
		if handleForm(h) != want {
			t.Errorf("%q: %v", h, !want)
		}
	}
}
