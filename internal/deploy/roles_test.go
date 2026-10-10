package deploy

import (
	"testing"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"

	"zobik.org/zobik/internal/bus"
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
		if got[FileRootPub] != rootPub {
			t.Errorf("%s: root.pub is %q", r.Unit, got[FileRootPub])
		}
		seed, err := nkeys.FromSeed([]byte(got[FileIdentity]))
		if err != nil {
			t.Fatalf("%s: identity: %v", r.Unit, err)
		}
		if p, _ := seed.PublicKey(); p != pub {
			t.Errorf("%s: the seed is not the identity's", r.Unit)
		}

		uc, err := jwt.DecodeUserClaims(got[FileBusJWT])
		if err != nil {
			t.Fatalf("%s: bus.jwt: %v", r.Unit, err)
		}
		signer, _ := m.account.SigningKeys[r.Scope.Role].PublicKey()
		if uc.Subject != pub || uc.Issuer != signer || uc.IssuerAccount != m.account.Public {
			t.Errorf("%s: the JWT is not the identity's under its scope", r.Unit)
		}

		cred, hasCred := got[FileCredential]
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

		cert, hasCert := got[FileRoleCertificate]
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

// The channel of the panel competes in round 1 and the final one.
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
		v, err := signing.VerifyCapability(files(fs)[FileCredential], rootPub)
		if err != nil {
			t.Fatal(err)
		}
		w := v.Claims.AttemptWindow
		if w == nil || *w.From != 1 || *w.To != 1 || !*w.FinalRound {
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
