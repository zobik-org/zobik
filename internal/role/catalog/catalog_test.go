package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	ggcrregistry "github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/random"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	scannerv1 "zobik.org/zobik/proto/zobik/scanner/v1"
)

// fakeScanner answers pass with NRI 1.0 unless results names the digest, and
// NOT_FOUND for a digest the registry does not have.
type fakeScanner struct {
	mu      sync.Mutex
	ruleset string
	results map[string]*scannerv1.ScanResult
	missing map[string]bool
	calls   []string
}

func (f *fakeScanner) Scan(_ context.Context, in *scannerv1.ScanRequest, _ ...grpc.CallOption) (*scannerv1.ScanResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, in.ArtifactRef)
	if f.missing[in.ArtifactRef] {
		return nil, status.Error(codes.NotFound, "gone")
	}
	if r, ok := f.results[in.ArtifactRef]; ok {
		return r, nil
	}
	return &scannerv1.ScanResult{Verdict: "pass", RulesetVersion: f.ruleset, NriVersion: "1.0"}, nil
}

type fixture struct {
	t       *testing.T
	c       *Catalog
	url     string
	repo    name.Repository
	opt     remote.Option
	scanner *fakeScanner
	admin   string
	now     time.Time
	base    v1.Image
}

func newFixture(t *testing.T, capacity int64) *fixture {
	t.Helper()
	regSrv := httptest.NewTLSServer(ggcrregistry.New(ggcrregistry.Logger(log.New(io.Discard, "", 0))))
	t.Cleanup(regSrv.Close)
	repo, err := name.NewRepository(strings.TrimPrefix(regSrv.URL, "https://") + "/artifacts")
	if err != nil {
		t.Fatal(err)
	}
	reg, err := newRegistry(repo.String(), regSrv.Client().Transport)
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{t: t, repo: repo, opt: remote.WithTransport(regSrv.Client().Transport), now: time.Unix(1_800_000_000, 123_456_789)}
	admin, _ := NewCredential()
	f.admin = admin
	f.c, err = New(filepath.Join(t.TempDir(), "catalog.db"), reg, nil, capacity, HashCredential(admin))
	if err != nil {
		t.Fatal(err)
	}
	f.scanner = &fakeScanner{ruleset: f.c.ruleset}
	f.c.scanner = f.scanner
	f.c.store.now = func() time.Time { return f.now }
	t.Cleanup(func() { f.c.Close() })
	srv := httptest.NewServer(f.c.Handler())
	t.Cleanup(srv.Close)
	f.url = srv.URL

	// The base image, retained by its base-* tag (implementation §1.2.16).
	if f.base, err = random.Image(512, 2); err != nil {
		t.Fatal(err)
	}
	d, _ := f.base.Digest()
	if err := remote.Write(repo.Tag("base-"+strings.Replace(d.String(), ":", "-", 1)), f.base, f.opt); err != nil {
		t.Fatal(err)
	}
	return f
}

// artifact pushes an image of the base plus one layer of size bytes, by digest
// only, and returns its digest and that layer's size in the manifest.
func (f *fixture) artifact(size int64) (string, int64) {
	f.t.Helper()
	layer, err := random.Layer(size, "application/vnd.oci.image.layer.v1.tar+gzip")
	if err != nil {
		f.t.Fatal(err)
	}
	img, err := mutate.AppendLayers(f.base, layer)
	if err != nil {
		f.t.Fatal(err)
	}
	d, _ := img.Digest()
	if err := remote.Write(f.repo.Digest(d.String()), img, f.opt); err != nil {
		f.t.Fatal(err)
	}
	n, _ := layer.Size()
	return d.String(), n
}

func (f *fixture) credential(network, scope string) string {
	f.t.Helper()
	var out struct{ Credential string }
	if code := f.call(f.admin, http.MethodPost, "/v1/credentials", map[string]string{"network": network, "scope": scope}, nil, &out); code != http.StatusCreated {
		f.t.Fatalf("issuing a credential: %d", code)
	}
	return out.Credential
}

// call sends body as JSON (or as is, for []byte) and decodes the answer into
// out, returning the status.
func (f *fixture) call(cred, method, path string, body any, header http.Header, out any) int {
	f.t.Helper()
	code, _ := f.callHeader(cred, method, path, body, header, out)
	return code
}

func (f *fixture) callHeader(cred, method, path string, body any, header http.Header, out any) (int, http.Header) {
	f.t.Helper()
	var r io.Reader
	switch b := body.(type) {
	case nil:
	case []byte:
		r = bytes.NewReader(b)
	default:
		raw, err := json.Marshal(b)
		if err != nil {
			f.t.Fatal(err)
		}
		r = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, f.url+path, r)
	if err != nil {
		f.t.Fatal(err)
	}
	maps := header.Clone()
	if maps != nil {
		req.Header = maps
	}
	if cred != "" {
		req.Header.Set("Authorization", "Bearer "+cred)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		f.t.Fatal(err)
	}
	defer resp.Body.Close()
	if out != nil {
		raw, _ := io.ReadAll(resp.Body)
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, out); err != nil {
				f.t.Fatalf("%s %s: %v in %s", method, path, err, raw)
			}
		}
	}
	return resp.StatusCode, resp.Header
}

func registration(d string) Registration {
	return Registration{
		Topic:            "echo",
		TopicDescription: "Repeating a text back.",
		ArtifactRef:      d,
		CapabilityText:   "Repeats back the text of the task it receives.",
		AuthorID:         "seeding",
		Requires:         map[string]json.RawMessage{"memory_required": json.RawMessage(`128`)},
	}
}

type entryView struct {
	Topic        string     `json:"topic"`
	ArtifactRef  string     `json:"artifact_ref"`
	ScanResult   ScanResult `json:"scan_result"`
	NRIVersion   string     `json:"nri_version"`
	ArtifactSize int64      `json:"artifact_size"`
	DeletionMark bool       `json:"deletion_mark"`
}

func TestRegister(t *testing.T) {
	f := newFixture(t, 1<<20)
	reg := f.credential("main", ScopeRegister)

	t.Run("registered", func(t *testing.T) {
		d, size := f.artifact(1024)
		var e entryView
		if code := f.call(reg, http.MethodPost, "/v1/entries", registration(d), nil, &e); code != http.StatusCreated {
			t.Fatalf("status %d", code)
		}
		if e.Topic != "echo" || e.ScanResult.Verdict != "pass" || e.ScanResult.RulesetVersion != f.c.ruleset ||
			e.NRIVersion != "1.0" || e.ArtifactSize != size || e.DeletionMark {
			t.Errorf("entry %+v, want pass with NRI 1.0 and size %d", e, size)
		}
		// The registration puts the tag that retains the image.
		if _, err := remote.Head(f.repo.Tag(entryTag(d)), f.opt); err != nil {
			t.Errorf("the entry-* tag: %v", err)
		}

		var again entryView
		if code := f.call(reg, http.MethodPost, "/v1/entries", registration(d), nil, &again); code != http.StatusOK || !reflect.DeepEqual(again, e) {
			t.Errorf("registering twice: %d %+v, want 200 and the same entry", code, again)
		}
		other := registration(d)
		other.CapabilityText = "Something else."
		if code := f.call(reg, http.MethodPost, "/v1/entries", other, nil, nil); code != http.StatusConflict {
			t.Errorf("another half under the same digest: %d, want 409", code)
		}
	})

	reject := func(t *testing.T, d string, want string) Failure {
		t.Helper()
		var fail Failure
		if code := f.call(reg, http.MethodPost, "/v1/entries", registration(d), nil, &fail); code != http.StatusUnprocessableEntity || fail.Reason != want {
			t.Fatalf("status %d, reason %q; want 422 %s", code, fail.Reason, want)
		}
		return fail
	}
	t.Run("scan_failed", func(t *testing.T) {
		d, _ := f.artifact(100)
		f.scanner.results = map[string]*scannerv1.ScanResult{d: {Verdict: "fail", RulesetVersion: f.c.ruleset, Findings: []string{"private-key"}, NriVersion: "1.0"}}
		fail := reject(t, d, ReasonScanFailed)
		if fail.RulesetVersion != f.c.ruleset || !slices.Equal(fail.Findings, []string{"private-key"}) {
			t.Errorf("failure %+v, want the ruleset and the findings", fail)
		}
	})
	t.Run("unresolvable", func(t *testing.T) {
		reject(t, "sha256:"+strings.Repeat("0", 64), ReasonUnresolvable)
	})
	t.Run("nri_version_missing", func(t *testing.T) {
		d, _ := f.artifact(100)
		f.scanner.results = map[string]*scannerv1.ScanResult{d: {Verdict: "pass", RulesetVersion: f.c.ruleset}}
		reject(t, d, ReasonNRIVersionMissing)
	})
	t.Run("storage_full", func(t *testing.T) {
		d, _ := f.artifact(2 << 20)
		reject(t, d, ReasonStorageFull)
	})
	t.Run("malformed", func(t *testing.T) {
		d, _ := f.artifact(100)
		for _, mutate := range []func(*Registration){
			func(r *Registration) { r.Topic = "" },
			func(r *Registration) { r.Topic = "Echo Node" },
			func(r *Registration) { r.ArtifactRef = "zobik-echo:dev" },
			func(r *Registration) { r.Requires = nil },
			func(r *Registration) { r.Requires["memory_required"] = json.RawMessage(`"lots"`) },
			func(r *Registration) { r.Acquisition = &Acquisition{Provenance: "derived", AsOf: f.now} },
		} {
			r := registration(d)
			mutate(&r)
			if code := f.call(reg, http.MethodPost, "/v1/entries", r, nil, nil); code != http.StatusBadRequest {
				t.Errorf("%+v: status %d, want 400", r, code)
			}
		}
	})
}

// The act of registration registers the emergent topic of the first entry
// that claims it, with the description it brings, and the next ones keep it
// (architecture §3.7.6, §3.13.3).
func TestTopicRegistry(t *testing.T) {
	f := newFixture(t, 1<<20)
	reg := f.credential("main", ScopeRegister)
	read := f.credential("main", ScopeRead)
	register := func(r Registration) (int, Failure) {
		t.Helper()
		var fail Failure
		code := f.call(reg, http.MethodPost, "/v1/entries", r, nil, &fail)
		return code, fail
	}

	d1, _ := f.artifact(100)
	missing := registration(d1)
	missing.TopicDescription = ""
	if code, fail := register(missing); code != http.StatusUnprocessableEntity || fail.Reason != ReasonTopicDescriptionMissing {
		t.Fatalf("a new topic without description: %d %q, want 422 %s", code, fail.Reason, ReasonTopicDescriptionMissing)
	}
	if code, _ := register(registration(d1)); code != http.StatusCreated {
		t.Fatalf("registering: %d", code)
	}

	// A later entry on the same topic needs no description, and its own does
	// not replace the registered one.
	d2, _ := f.artifact(200)
	other := registration(d2)
	other.TopicDescription = "Something else."
	if code, _ := register(other); code != http.StatusCreated {
		t.Fatalf("a second entry with another description: %d", code)
	}
	d3, _ := f.artifact(300)
	bare := registration(d3)
	bare.TopicDescription = ""
	if code, _ := register(bare); code != http.StatusCreated {
		t.Fatalf("a third entry without description: %d", code)
	}

	// intake and the hitl_contact_* family are not in the registry.
	for _, topic := range []string{"intake", "hitl_contact_operator"} {
		d, _ := f.artifact(50)
		r := registration(d)
		r.Topic, r.TopicDescription = topic, ""
		if code, _ := register(r); code != http.StatusCreated {
			t.Fatalf("an entry on %s without description: %d", topic, code)
		}
	}

	var list struct{ Topics []Topic }
	if code := f.call(read, http.MethodGet, "/v1/topics", nil, nil, &list); code != http.StatusOK {
		t.Fatalf("listing the topics: %d", code)
	}
	want := []Topic{{Topic: "echo", Description: "Repeating a text back."}}
	if !reflect.DeepEqual(list.Topics, want) {
		t.Errorf("topics %+v, want %+v", list.Topics, want)
	}
	var one Topic
	if code := f.call(read, http.MethodGet, "/v1/topics/echo", nil, nil, &one); code != http.StatusOK || one != want[0] {
		t.Errorf("reading echo: %d %+v", code, one)
	}
	if code := f.call(read, http.MethodGet, "/v1/topics/absent", nil, nil, nil); code != http.StatusNotFound {
		t.Errorf("an absent topic: %d, want 404", code)
	}

	// The entry reads without the description, which belongs to the topic.
	var e map[string]json.RawMessage
	f.call(read, http.MethodGet, "/v1/entries/"+d2, nil, nil, &e)
	if _, ok := e["topic_description"]; ok {
		t.Errorf("the entry carries topic_description: %s", e["topic_description"])
	}
}

func TestCredentials(t *testing.T) {
	f := newFixture(t, 1<<20)
	read := f.credential("main", ScopeRead)
	spawner := f.credential("main", ScopeSpawner)
	other := f.credential("other", ScopeSpawner)
	d, _ := f.artifact(100)

	cases := []struct {
		name, cred, method, path string
		body                     any
		want                     int
	}{
		{"none", "", http.MethodGet, "/v1/status", nil, http.StatusUnauthorized},
		{"an unknown one", "nope", http.MethodGet, "/v1/status", nil, http.StatusUnauthorized},
		{"read registers", read, http.MethodPost, "/v1/entries", registration(d), http.StatusForbidden},
		{"read writes a record", read, http.MethodPut, "/v1/records/main/niche:echo", map[string]int{"n": 1}, http.StatusForbidden},
		{"read stamps the deletion mark", read, http.MethodPut, "/v1/entries/" + d + "/deletion_mark", nil, http.StatusForbidden},
		{"another network's spawner writes", other, http.MethodPut, "/v1/records/main/niche:echo", map[string]int{"n": 1}, http.StatusForbidden},
		{"a spawner issues credentials", spawner, http.MethodPost, "/v1/credentials", map[string]string{"network": "main", "scope": "spawner"}, http.StatusForbidden},
		{"the administration reads", f.admin, http.MethodGet, "/v1/status", nil, http.StatusForbidden},
		{"read reads", read, http.MethodGet, "/v1/status", nil, http.StatusOK},
		{"the spawner writes its own", spawner, http.MethodPut, "/v1/records/main/niche:echo", map[string]int{"n": 1}, http.StatusNoContent},
	}
	for _, c := range cases {
		if code := f.call(c.cred, c.method, c.path, c.body, nil, nil); code != c.want {
			t.Errorf("%s: status %d, want %d", c.name, code, c.want)
		}
	}

	if code := f.call(f.admin, http.MethodDelete, "/v1/credentials/main", nil, nil, nil); code != http.StatusNoContent {
		t.Fatalf("revoking: %d", code)
	}
	if code := f.call(read, http.MethodGet, "/v1/status", nil, nil, nil); code != http.StatusUnauthorized {
		t.Errorf("a revoked credential: status %d, want 401", code)
	}
	if code := f.call(other, http.MethodGet, "/v1/status", nil, nil, nil); code != http.StatusOK {
		t.Errorf("revoking one network revoked another's: %d", code)
	}
}

func TestRecords(t *testing.T) {
	f := newFixture(t, 1<<20)
	spawner := f.credential("main", ScopeSpawner)
	other := f.credential("other", ScopeSpawner)
	absent := http.Header{"If-None-Match": {"*"}}
	match := func(rev string) http.Header { return http.Header{"If-Match": {rev}} }
	put := func(cred, path string, value any, h http.Header) (int, string) {
		code, got := f.callHeader(cred, http.MethodPut, path, value, h, nil)
		return code, got.Get("ETag")
	}

	// The write-if-absent the Spawners arbitrate with (implementation §1.2.11).
	if code, etag := put(spawner, "/v1/records/main/provision:echo", map[string]string{"by": "a"}, absent); code != http.StatusNoContent || etag != `"1"` {
		t.Fatalf("the first write-if-absent: %d %s", code, etag)
	}
	if code, _ := put(spawner, "/v1/records/main/provision:echo", map[string]string{"by": "b"}, absent); code != http.StatusPreconditionFailed {
		t.Errorf("If-None-Match: * on an existing key: %d, want 412", code)
	}
	if code, etag := put(spawner, "/v1/records/main/provision:echo", map[string]string{"by": "c"}, match(`"1"`)); code != http.StatusNoContent || etag != `"2"` {
		t.Errorf("If-Match with the revision read: %d %s", code, etag)
	}
	if code, _ := put(spawner, "/v1/records/main/provision:echo", map[string]string{"by": "d"}, match(`"1"`)); code != http.StatusPreconditionFailed {
		t.Errorf("If-Match with a stale revision: %d, want 412", code)
	}
	var value map[string]string
	code, h := f.callHeader(spawner, http.MethodGet, "/v1/records/main/provision:echo", nil, nil, &value)
	if code != http.StatusOK || value["by"] != "c" || h.Get("ETag") != `"2"` {
		t.Errorf("reading: %d %v %s", code, value, h.Get("ETag"))
	}

	// A network reads every network's records, and the listing without
	// network is how deletion reads the presence marks.
	put(spawner, "/v1/records/main/presence:sha256:aa", true, nil)
	put(other, "/v1/records/other/presence:sha256:aa", true, nil)
	put(other, "/v1/records/other/veto:sha256:aa", true, nil)
	var list struct{ Records []Record }
	f.call(spawner, http.MethodGet, "/v1/records?prefix=presence:", nil, nil, &list)
	if len(list.Records) != 2 || list.Records[0].Network != "main" || list.Records[1].Network != "other" {
		t.Errorf("the presence marks of every network: %+v", list.Records)
	}
	f.call(spawner, http.MethodGet, "/v1/records?prefix=presence:&network=other", nil, nil, &list)
	if len(list.Records) != 1 || list.Records[0].Key != "presence:sha256:aa" {
		t.Errorf("those of one network: %+v", list.Records)
	}

	// A record with expires_in ceases to exist for every read when it expires.
	if code, _ := put(spawner, "/v1/records/main/presence:sha256:bb?expires_in=60", true, nil); code != http.StatusNoContent {
		t.Fatalf("writing with expires_in: %d", code)
	}
	f.now = f.now.Add(61 * time.Second)
	if code := f.call(spawner, http.MethodGet, "/v1/records/main/presence:sha256:bb", nil, nil, nil); code != http.StatusNotFound {
		t.Errorf("an expired record: %d, want 404", code)
	}
	f.call(spawner, http.MethodGet, "/v1/records?prefix=presence:sha256:bb", nil, nil, &list)
	if len(list.Records) != 0 {
		t.Errorf("the listing returns an expired record: %+v", list.Records)
	}
	if code, etag := put(spawner, "/v1/records/main/presence:sha256:bb", true, absent); code != http.StatusNoContent || etag != `"2"` {
		t.Errorf("If-None-Match: * over an expired record: %d %s, want 204 with a revision past it", code, etag)
	}

	if code := f.call(spawner, http.MethodDelete, "/v1/records/main/provision:echo", nil, match(`"1"`), nil); code != http.StatusPreconditionFailed {
		t.Errorf("deleting with a stale revision: %d, want 412", code)
	}
	if code := f.call(spawner, http.MethodDelete, "/v1/records/main/provision:echo", nil, match(`"2"`), nil); code != http.StatusNoContent {
		t.Errorf("deleting: %d", code)
	}
	if code := f.call(spawner, http.MethodGet, "/v1/records/main/provision:echo", nil, nil, nil); code != http.StatusNotFound {
		t.Errorf("a deleted record: %d, want 404", code)
	}
	if code, _ := put(spawner, "/v1/records/main/niche:echo", []byte("not json"), nil); code != http.StatusBadRequest {
		t.Errorf("a value that is not JSON: %d, want 400", code)
	}
}

func TestDeletionMarkAndAcquisition(t *testing.T) {
	f := newFixture(t, 1<<20)
	reg := f.credential("main", ScopeRegister)
	spawner := f.credential("main", ScopeSpawner)
	d, size := f.artifact(1024)
	r := registration(d)
	r.Acquisition = &Acquisition{Provenance: ProvenanceSeeded, AsOf: f.now}
	if code := f.call(reg, http.MethodPost, "/v1/entries", r, nil, nil); code != http.StatusCreated {
		t.Fatalf("registering: %d", code)
	}
	var acq Acquisition
	if code := f.call(reg, http.MethodGet, "/v1/records/main/acquisition:"+d, nil, nil, &acq); code != http.StatusOK || acq.Provenance != ProvenanceSeeded {
		t.Errorf("the acquisition row: %d %+v", code, acq)
	}
	// A row already written stays.
	r.Acquisition = &Acquisition{Provenance: ProvenancePurchased, Cost: 3, AsOf: f.now}
	f.call(reg, http.MethodPost, "/v1/entries", r, nil, nil)
	f.call(reg, http.MethodGet, "/v1/records/main/acquisition:"+d, nil, nil, &acq)
	if acq.Provenance != ProvenanceSeeded {
		t.Errorf("a second registration rewrote the acquisition row: %+v", acq)
	}

	var st Status
	f.call(reg, http.MethodGet, "/v1/status", nil, nil, &st)
	if st.Occupied != size || st.Capacity != 1<<20 || st.RulesetVersion != f.c.ruleset {
		t.Errorf("status %+v, want %d occupied", st, size)
	}
	if code := f.call(spawner, http.MethodPut, "/v1/entries/"+d+"/deletion_mark", nil, nil, nil); code != http.StatusNoContent {
		t.Fatalf("stamping the deletion mark: %d", code)
	}
	var e entryView
	f.call(reg, http.MethodGet, "/v1/entries/"+d, nil, nil, &e)
	f.call(reg, http.MethodGet, "/v1/status", nil, nil, &st)
	if !e.DeletionMark || st.Occupied != 0 {
		t.Errorf("with the mark: entry %+v, occupied %d", e, st.Occupied)
	}
	// Registering it again removes the mark (architecture §3.13.3).
	if code := f.call(reg, http.MethodPost, "/v1/entries", r, nil, &e); code != http.StatusOK || e.DeletionMark {
		t.Errorf("registering again: %d %+v", code, e)
	}
	if code := f.call(spawner, http.MethodPut, "/v1/entries/sha256:"+strings.Repeat("1", 64)+"/deletion_mark", nil, nil, nil); code != http.StatusNotFound {
		t.Errorf("the mark of an entry that does not exist: %d, want 404", code)
	}
}

func TestBundles(t *testing.T) {
	f := newFixture(t, 1<<20)
	reg := f.credential("main", ScopeRegister)
	a, _ := f.artifact(100)
	b, _ := f.artifact(100)
	for _, d := range []string{a, b} {
		if code := f.call(reg, http.MethodPost, "/v1/entries", registration(d), nil, nil); code != http.StatusCreated {
			t.Fatalf("registering: %d", code)
		}
	}
	var first, second Bundle
	body := Bundle{PurposeText: "Echo twice.", Members: []string{a, b}}
	if code := f.call(reg, http.MethodPost, "/v1/bundles", body, nil, &first); code != http.StatusOK || !strings.HasPrefix(first.BundleID, "sha256:") {
		t.Fatalf("registering a bundle: %d %+v", code, first)
	}
	f.call(reg, http.MethodPost, "/v1/bundles", body, nil, &second)
	if second.BundleID != first.BundleID {
		t.Errorf("the same bundle twice: %s and %s", first.BundleID, second.BundleID)
	}
	var got Bundle
	if code := f.call(reg, http.MethodGet, "/v1/bundles/"+first.BundleID, nil, nil, &got); code != http.StatusOK || !slices.Equal(got.Members, body.Members) {
		t.Errorf("reading it: %d %+v", code, got)
	}
	var fail Failure
	missing := "sha256:" + strings.Repeat("2", 64)
	if code := f.call(reg, http.MethodPost, "/v1/bundles", Bundle{PurposeText: "x", Members: []string{a, missing}}, nil, &fail); code != http.StatusUnprocessableEntity ||
		fail.Reason != ReasonUnregisteredMember || fail.Member != missing {
		t.Errorf("a member without an entry: %d %+v", code, fail)
	}
}

// When the ruleset advances, the entries are analyzed again, those running
// somewhere first (implementation §1.2.19).
func TestRescan(t *testing.T) {
	f := newFixture(t, 1<<20)
	reg := f.credential("main", ScopeRegister)
	spawner := f.credential("main", ScopeSpawner)
	var ds []string
	for range 3 {
		d, _ := f.artifact(100)
		if code := f.call(reg, http.MethodPost, "/v1/entries", registration(d), nil, nil); code != http.StatusCreated {
			t.Fatalf("registering: %d", code)
		}
		ds = append(ds, d)
	}
	slices.Sort(ds)
	running := ds[2]
	f.call(spawner, http.MethodPut, "/v1/records/main/presence:"+running+"?expires_in=60", true, nil, nil)

	f.c.ruleset = "next"
	f.scanner.ruleset = "next"
	f.scanner.calls = nil
	f.scanner.results = map[string]*scannerv1.ScanResult{ds[0]: {Verdict: "fail", RulesetVersion: "next", Findings: []string{"zobik-new"}}}
	f.c.Rescan(context.Background(), time.Millisecond)

	if !slices.Equal(f.scanner.calls, []string{running, ds[0], ds[1]}) {
		t.Errorf("analyzed in the order %v, want the running one first", f.scanner.calls)
	}
	var e entryView
	f.call(reg, http.MethodGet, "/v1/entries/"+ds[0], nil, nil, &e)
	if e.ScanResult.Verdict != "fail" || e.ScanResult.RulesetVersion != "next" || !slices.Equal(e.ScanResult.Findings, []string{"zobik-new"}) {
		t.Errorf("the entry that moved to fail: %+v", e.ScanResult)
	}
	f.call(reg, http.MethodGet, "/v1/entries/"+ds[1], nil, nil, &e)
	if e.ScanResult.Verdict != "pass" || e.ScanResult.RulesetVersion != "next" {
		t.Errorf("the entry that still passes: %+v", e.ScanResult)
	}
	f.scanner.calls = nil
	f.c.Rescan(context.Background(), time.Millisecond)
	if len(f.scanner.calls) != 0 {
		t.Errorf("analyzed again what is current: %v", f.scanner.calls)
	}
}
