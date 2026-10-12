package taskbroker

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/nats-io/nkeys"

	"zobik.org/zobik/internal/embedding"
	"zobik.org/zobik/internal/signing"
)

// harness mints, with the same root, the credentials the Spawner and zobik init
// issue (architecture §3.14.2).
type harness struct {
	t       *testing.T
	root    nkeys.KeyPair
	rootPub string
	spawner nkeys.KeyPair
	cert    string
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	root, _ := nkeys.CreateOperator()
	rootPub, _ := root.PublicKey()
	spawner, _ := nkeys.CreateUser()
	spawnerPub, _ := spawner.PublicKey()
	cert, err := signing.Sign(root, signing.TypeRoleCertificate, signing.RoleCertificate{Sub: spawnerPub, Role: signing.RoleSpawner}, "")
	if err != nil {
		t.Fatal(err)
	}
	return &harness{t: t, root: root, rootPub: rootPub, spawner: spawner, cert: cert}
}

func (h *harness) identity() string {
	kp, _ := nkeys.CreateUser()
	pub, _ := kp.PublicKey()
	return pub
}

// emergent is a Spawner-signed credential.
func (h *harness) emergent(c signing.Capability) string {
	h.t.Helper()
	tok, err := signing.Sign(h.spawner, signing.TypeCapability, c, h.cert)
	if err != nil {
		h.t.Fatal(err)
	}
	return tok
}

// structural is a root-signed credential.
func (h *harness) structural(c signing.Capability) string {
	h.t.Helper()
	tok, err := signing.Sign(h.root, signing.TypeCapability, c, "")
	if err != nil {
		h.t.Fatal(err)
	}
	return tok
}

func (h *harness) proof(p signing.OperatorProof) string {
	h.t.Helper()
	tok, err := signing.Sign(h.root, signing.TypeOperatorProof, p, "")
	if err != nil {
		h.t.Fatal(err)
	}
	return tok
}

// unit returns a normalized vector at angle theta from e1 in the e1-e2 plane,
// so its similarity with e1 is cos(theta).
func unit(cos float64) []float32 {
	return []float32{float32(cos), float32(math.Sqrt(1 - cos*cos)), 0}
}

func ptr[T any](v T) *T { return &v }

func TestJudgeEmergent(t *testing.T) {
	h := newHarness(t)
	task := &announced{topic: "echo", taskID: "T1", embedding: unit(1), modelID: "m1", decimals: 3}
	id := h.identity()
	cred := func(cos, floor float64, model string) string {
		return h.emergent(signing.Capability{Sub: id, Topic: "echo", ModelID: model,
			CapabilityEmbedding: embedding.Encode(unit(cos)), SimilarityFloor: floor})
	}
	creds := newCredentials(h.rootPub)

	v, err := judge(creds, task, proposal{identity: id, credential: cred(0.8, 0.6, "m1")})
	if err != nil || !v.admissible || v.score != 800 || *v.similarity != 0.8 {
		t.Errorf("recomputed: %+v %v", v, err)
	}
	// The declared value is a ceiling: it lowers, never raises.
	v, _ = judge(creds, task, proposal{identity: id, declared: ptr(0.99), credential: cred(0.8, 0.6, "m1")})
	if v.score != 800 {
		t.Errorf("a declared value above the recomputed one raised it: %d", v.score)
	}
	v, _ = judge(creds, task, proposal{identity: id, declared: ptr(0.7), credential: cred(0.8, 0.6, "m1")})
	if v.score != 700 {
		t.Errorf("a declared value below the recomputed one did not lower it: %d", v.score)
	}
	// The floor decides admissibility on the rounded value, and has to be exceeded.
	v, _ = judge(creds, task, proposal{identity: id, credential: cred(0.6001, 0.6, "m1")})
	if v.admissible {
		t.Error("a similarity that rounds to the floor is admissible")
	}
	if _, err := judge(creds, task, proposal{identity: id, credential: cred(0.8, 0.6, "m2")}); !errors.Is(err, errDiscarded) {
		t.Errorf("a credential of another vector space: %v", err)
	}
	if _, err := judge(creds, task, proposal{identity: h.identity(), credential: cred(0.8, 0.6, "m1")}); !errors.Is(err, errDiscarded) {
		t.Errorf("a credential replayed by another identity: %v", err)
	}
	noEmbedding := &announced{topic: "echo", taskID: "T1", modelID: "m1", decimals: 3}
	if _, err := judge(creds, noEmbedding, proposal{identity: id, credential: cred(0.8, 0.6, "m1")}); !errors.Is(err, errDiscarded) {
		t.Errorf("an emergent proposal on an announcement without embedding: %v", err)
	}
}

func TestJudgeStructural(t *testing.T) {
	h := newHarness(t)
	creds := newCredentials(h.rootPub)
	config := h.identity()
	configCred := h.structural(signing.Capability{Sub: config, Topic: "config_change"})
	change := &announced{topic: "config_change", taskID: "T1", decimals: 3}

	// No embedding, no model_id, no floor: admission by credential.
	v, err := judge(creds, change, proposal{identity: config, credential: configCred})
	if err != nil || !v.admissible || v.similarity != nil {
		t.Errorf("structural on config_change: %+v %v", v, err)
	}
	node := h.identity()
	nodeCred := h.emergent(signing.Capability{Sub: node, Topic: "config_change", SimilarityFloor: -1})
	if _, err := judge(creds, change, proposal{identity: node, credential: nodeCred}); !errors.Is(err, errDiscarded) {
		t.Errorf("an emergent node on config_change: %v", err)
	}
	// The Spawner's sentinel on an emergent topic ranks by what it declares.
	echo := &announced{topic: "echo", taskID: "T2", embedding: unit(1), modelID: "m2", decimals: 3}
	v, err = judge(creds, echo, proposal{identity: config, declared: ptr(-1.0), credential: configCred})
	if err != nil || !v.admissible || v.score != -1000 {
		t.Errorf("sentinel: %+v %v", v, err)
	}
}

func TestJudgeChannel(t *testing.T) {
	h := newHarness(t)
	creds := newCredentials(h.rootPub)
	query := &announced{topic: "hitl_contact_operator", taskID: "T1", decimals: 3}

	deployed := h.identity()
	v, err := judge(creds, query, proposal{identity: deployed,
		credential: h.structural(signing.Capability{Sub: deployed, Topic: "hitl_contact_operator"})})
	if err != nil || !v.admissible {
		t.Errorf("the deployment's channel: %+v %v", v, err)
	}
	other := h.identity()
	if _, err := judge(creds, query, proposal{identity: other,
		credential: h.structural(signing.Capability{Sub: other, Topic: "config_change"})}); !errors.Is(err, errDiscarded) {
		t.Errorf("a structural credential that is not a channel of the audience: %v", err)
	}

	channel := func(audience, artifact string) (string, string) {
		id := h.identity()
		proof := h.proof(signing.OperatorProof{Act: signing.ActNodeProvision, TaskID: "P", ArtifactRef: "img@sha256:a", Audience: audience})
		return id, h.emergent(signing.Capability{Sub: id, Topic: "hitl_contact_operator", ArtifactRef: artifact, OperatorProof: proof})
	}
	id, cred := channel("operator", "img@sha256:a")
	if _, err := judge(creds, query, proposal{identity: id, credential: cred}); err != nil {
		t.Errorf("an emergent channel with its proof: %v", err)
	}
	id, cred = channel("user", "img@sha256:a")
	if _, err := judge(creds, query, proposal{identity: id, credential: cred}); !errors.Is(err, errDiscarded) {
		t.Errorf("a proof of another audience: %v", err)
	}
	id, cred = channel("operator", "img@sha256:b")
	if _, err := judge(creds, query, proposal{identity: id, credential: cred}); !errors.Is(err, errDiscarded) {
		t.Errorf("a proof of another artifact: %v", err)
	}
	id = h.identity()
	cred = h.emergent(signing.Capability{Sub: id, Topic: "hitl_contact_operator"})
	if _, err := judge(creds, query, proposal{identity: id, credential: cred}); !errors.Is(err, errDiscarded) {
		t.Errorf("an emergent channel without a proof: %v", err)
	}
}

// The tie goes to the highest hash(TaskID, node_id), so it is recomputable and
// does not always go to the same node.
func TestAwardDraw(t *testing.T) {
	vs := []verdict{
		{identity: "UA", score: 800, admissible: true},
		{identity: "UB", score: 800, admissible: true},
		{identity: "UC", score: 900, admissible: false},
		{identity: "UD", score: 700, admissible: true},
	}
	wins := map[string]int{}
	for _, task := range []string{"T1", "T2", "T3", "T4", "T5", "T6", "T7", "T8"} {
		w, ok := award(task, vs)
		if !ok {
			t.Fatal("no winner")
		}
		again, _ := award(task, []verdict{vs[3], vs[2], vs[1], vs[0]})
		if vs[w].identity != []verdict{vs[3], vs[2], vs[1], vs[0]}[again].identity {
			t.Errorf("%s: the draw depends on the order of arrival", task)
		}
		wins[vs[w].identity]++
	}
	if wins["UA"] == 0 || wins["UB"] == 0 || wins["UC"]+wins["UD"] != 0 {
		t.Errorf("wins %v", wins)
	}
	if _, ok := award("T1", []verdict{{identity: "UA", admissible: false}}); ok {
		t.Error("an inadmissible proposal won")
	}
}

func TestAdmits(t *testing.T) {
	for _, c := range []struct {
		admission, topic string
		want             bool
	}{
		{"open", "intake", true},
		{"open", "echo", true},
		{"drain", "intake", false},
		{"drain", "echo", true},
		{"frozen", "intake", false},
		{"frozen", "echo", false},
		{"frozen", "config_change", true},
		{"frozen", "hitl_contact_operator", true},
		{"unknown", "config_change", false},
	} {
		if got := admits(c.admission, c.topic); got != c.want {
			t.Errorf("admits(%s, %s) = %v", c.admission, c.topic, got)
		}
	}
}

func TestBackoff(t *testing.T) {
	p := &params{backoffBase: 5 * time.Second, backoffMax: 5 * time.Minute}
	var got []time.Duration
	for n := range 8 {
		got = append(got, p.backoff(n))
	}
	want := []time.Duration{5, 10, 20, 40, 80, 160, 300, 300}
	for i := range want {
		if got[i] != want[i]*time.Second {
			t.Fatalf("backoff %v", got)
		}
	}
}
