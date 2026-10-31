//go:build e2e

package deploy

import (
	"archive/tar"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/configschema"
	"zobik.org/zobik/internal/egress"
	"zobik.org/zobik/internal/embedding"
	"zobik.org/zobik/internal/embedding/embeddingtest"
	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/internal/sidecar"
	"zobik.org/zobik/internal/signing"
)

// testBase is the image the test's Logic Container and Integration Sidecar are
// made from.
const testBase = "alpine:3.22@sha256:5291449c3df73caf6ed85e649dec1b9e818b39a5d8c871e97afc13e9cd5e8fa8"

type provisionFixture struct {
	e        *Engine
	js       jetstream.JetStream
	iss      *Issuer
	n        names
	registry string // the address the engine pulls the artifacts from
	logic    string // an artifact that declares its NRI version and stays up
	sidecar  string // the image of the same, which the engine has
	unnamed  string // an artifact that declares none
	signer   string // the node scope's signing key
}

func newProvisionFixture(t *testing.T, ctx context.Context) *provisionFixture {
	t.Helper()
	e, err := NewEngine(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })

	root, _ := nkeys.CreateOperator()
	rootPub, _ := root.PublicKey()
	suffix := make([]byte, 4)
	rand.Read(suffix)
	f := &provisionFixture{e: e, n: names{"provision-" + hex.EncodeToString(suffix), rootPub}}
	if err := e.EnsureNetwork(ctx, f.n.dockerNetwork(), f.n.labels()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.removeAll(t) })

	if err := e.EnsureImage(ctx, testBase); err != nil {
		t.Fatal(err)
	}
	f.registry = testRegistry(t, ctx, e)
	f.sidecar = f.image(t, ctx, "LABEL "+LabelNRIVersion+"=1.0", `CMD ["sleep", "3600"]`)
	f.logic = f.push(t, ctx, f.sidecar)
	f.unnamed = f.artifact(t, ctx, `CMD ["sleep", "3600"]`)

	fake := embeddingtest.NewServer(t)
	f.js = jetStream(t)
	if err := bus.EnsureStreams(ctx, f.js); err != nil {
		t.Fatal(err)
	}
	if err := bus.EnsureBuckets(ctx, f.js, time.Hour); err != nil {
		t.Fatal(err)
	}
	schema, err := configschema.Load()
	if err != nil {
		t.Fatal(err)
	}
	values := schema.V0()
	values["egress_registry"] = json.RawMessage(`{"embeddings":` + string(egress.EmbeddingsEntry(fake.URL)) + `}`)
	values["embedding_model"] = json.RawMessage(`{"entry":"embeddings","model":"test","document_prefix":"search_document: ","query_prefix":"search_query: ","max_text_size":8192}`)
	if err := globalconfig.WriteV0(ctx, f.js, globalconfig.Record{ConfigVersion: "v0", PublishedAt: time.Now(), AuthorizedBy: "deployment", Values: values}); err != nil {
		t.Fatal(err)
	}

	spawner, _ := nkeys.CreateUser()
	spawnerPub, _ := spawner.PublicKey()
	cert, err := signing.Sign(root, signing.TypeRoleCertificate, signing.RoleCertificate{Sub: spawnerPub, Iat: time.Now().Unix(), Role: signing.RoleSpawner}, "")
	if err != nil {
		t.Fatal(err)
	}
	key, _ := nkeys.CreateAccount()
	f.signer, _ = key.PublicKey()
	account, _ := nkeys.CreateAccount()
	accountPub, _ := account.PublicKey()
	f.iss = &Issuer{
		ID: spawnerPub, Certificate: cert, SigningKey: key, Account: accountPub, Root: rootPub,
		Sign: func(typ signing.Type, claims any, signer string) (string, error) {
			return signing.Sign(spawner, typ, claims, signer)
		},
		// The test's embeddings endpoint takes no credential and costs nothing.
		Egress: &egress.Executor{HTTP: egress.HTTPClient(nil), Rates: func(string) (*egress.Table, bool) { return nil, false }},
	}
	return f
}

// artifact commits testBase with changes and pushes it to the test's registry,
// and returns its digest.
func (f *provisionFixture) artifact(t *testing.T, ctx context.Context, changes ...string) string {
	t.Helper()
	return f.push(t, ctx, f.image(t, ctx, changes...))
}

// push pushes an image the engine has to the test's registry, as the console
// does through its ephemeral container, and returns its digest.
func (f *provisionFixture) push(t *testing.T, ctx context.Context, ref string) string {
	t.Helper()
	saved, done, err := f.e.SaveImage(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	d, err := Push(ctx, saved.Name(), f.registry, TagEntry, http.DefaultTransport, name.Insecure)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// testRegistry runs the registry image in the clear on a port of 127.0.0.1,
// where the engine pulls from it as it does through the link, and returns its
// address.
func testRegistry(t *testing.T, ctx context.Context, e *Engine) string {
	t.Helper()
	if err := e.EnsureImage(ctx, RegistryImage); err != nil {
		t.Fatal(err)
	}
	suffix := make([]byte, 4)
	rand.Read(suffix)
	port := freePort(t)
	cname := "zobik-test-registry-" + hex.EncodeToString(suffix)
	t.Cleanup(func() { e.Remove(context.Background(), []string{cname}, nil) })
	config := `{"distSpecVersion":"1.1.1","storage":{"rootDirectory":"` + registryDataDir + `"},"http":{"address":"0.0.0.0","port":"` + RegistryPort + `"}}`
	if err := e.EnsureRunning(ctx, Spec{
		Name:    cname,
		Network: "bridge",
		Image:   RegistryImage,
		Files:   []File{{Path: registryConfig, Mode: 0o644, Data: []byte(config)}},
		Publish: map[string]string{RegistryPort + "/tcp": port},
	}); err != nil {
		t.Fatal(err)
	}
	addr := "127.0.0.1:" + port
	for deadline := time.Now().Add(time.Minute); ; {
		resp, err := http.Get("http://" + addr + "/v2/")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return addr
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("the test registry does not answer: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// image commits testBase with changes, and removes it when the test ends.
func (f *provisionFixture) image(t *testing.T, ctx context.Context, changes ...string) string {
	t.Helper()
	return testImage(t, ctx, f.e, changes...)
}

func testImage(t *testing.T, ctx context.Context, e *Engine, changes ...string) string {
	t.Helper()
	if err := e.EnsureImage(ctx, testBase); err != nil {
		t.Fatal(err)
	}
	c, err := e.c.ContainerCreate(ctx, client.ContainerCreateOptions{Config: &container.Config{Image: testBase}})
	if err != nil {
		t.Fatal(err)
	}
	defer e.discard(ctx, c.ID)
	img, err := e.c.ContainerCommit(ctx, c.ID, client.ContainerCommitOptions{Changes: changes})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		e.c.ImageRemove(context.Background(), img.ID, client.ImageRemoveOptions{Force: true})
	})
	return img.ID
}

// removeAll removes every resource of the test's network.
func (f *provisionFixture) removeAll(t *testing.T) {
	ctx := context.Background()
	byNetwork := client.Filters{}.Add("label", labelNetwork+"="+f.n.network)
	var containers, volumes []string
	if cs, err := f.e.c.ContainerList(ctx, client.ContainerListOptions{All: true, Filters: byNetwork}); err == nil {
		for _, c := range cs.Items {
			containers = append(containers, c.ID)
		}
	}
	if vs, err := f.e.c.VolumeList(ctx, client.VolumeListOptions{Filters: byNetwork}); err == nil {
		for _, v := range vs.Items {
			volumes = append(volumes, v.Name)
		}
	}
	if err := f.e.Remove(ctx, containers, volumes); err != nil {
		t.Errorf("cleanup: %v", err)
	}
	f.e.c.NetworkRemove(ctx, f.n.dockerNetwork(), client.NetworkRemoveOptions{})
}

// resources counts the containers and volumes of the test's network.
func (f *provisionFixture) resources(t *testing.T, ctx context.Context) int {
	t.Helper()
	byNetwork := client.Filters{}.Add("label", labelNetwork+"="+f.n.network)
	cs, err := f.e.c.ContainerList(ctx, client.ContainerListOptions{All: true, Filters: byNetwork})
	if err != nil {
		t.Fatal(err)
	}
	vs, err := f.e.c.VolumeList(ctx, client.VolumeListOptions{Filters: byNetwork})
	if err != nil {
		t.Fatal(err)
	}
	return len(cs.Items) + len(vs.Items)
}

func (f *provisionFixture) request(b Blueprint) ProvisionRequest {
	if b.ArtifactRef == "" {
		b.ArtifactRef = f.logic
	}
	if b.CapabilityText == "" {
		b.CapabilityText = "Repeats the text of the task."
	}
	return ProvisionRequest{Network: f.n.network, Topic: "echo", Blueprint: b, SidecarImage: f.sidecar, Registry: f.registry}
}

// The act brings up a node with everything architecture §3.13.1 enumerates,
// and the Logic Container with the spec of implementation §2.2.
func TestProvision(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	f := newProvisionFixture(t, ctx)

	node, err := Provision(ctx, f.e, f.js, f.iss, f.request(Blueprint{
		AttemptWindow: &signing.AttemptWindow{From: ptr(1)},
		Requires:      map[string]json.RawMessage{"memory_required": json.RawMessage(`64`)},
	}))
	if err != nil {
		t.Fatal(err)
	}

	logic, err := f.e.c.ContainerInspect(ctx, node.Logic, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	h := logic.Container.HostConfig
	if h.NetworkMode != "none" || len(logic.Container.NetworkSettings.Networks) != 1 {
		t.Errorf("the Logic Container has network %q, %d endpoints", h.NetworkMode, len(logic.Container.NetworkSettings.Networks))
	}
	if !slices.Equal(h.CapDrop, []string{"ALL"}) || !slices.Contains(h.SecurityOpt, "no-new-privileges") {
		t.Errorf("the Logic Container keeps privileges: cap_drop %v, security_opt %v", h.CapDrop, h.SecurityOpt)
	}
	if !h.ReadonlyRootfs || len(h.Tmpfs) != 1 || h.Tmpfs["/tmp"] != "" {
		t.Errorf("the Logic Container's filesystem: read-only %v, tmpfs %v", h.ReadonlyRootfs, h.Tmpfs)
	}
	if len(logic.Container.Mounts) != 1 || logic.Container.Mounts[0].Name != node.Shared || logic.Container.Mounts[0].Destination != sidecar.SharedDir {
		t.Errorf("the Logic Container mounts %+v", logic.Container.Mounts)
	}
	if h.Memory != 64<<20 || h.MemorySwap != 64<<20 {
		t.Errorf("the Logic Container's memory %d, swap %d", h.Memory, h.MemorySwap)
	}
	if h.RestartPolicy.Name != container.RestartPolicyAlways {
		t.Errorf("the Logic Container restarts %q", h.RestartPolicy.Name)
	}
	if logic.Container.Config.User != "65532:65532" {
		t.Errorf("the Logic Container runs as %q", logic.Container.Config.User)
	}
	l := logic.Container.Config.Labels
	for k, want := range map[string]string{
		labelNode: node.ID, labelTopic: "echo", labelModelID: node.ModelID, labelSignerID: f.iss.ID,
		labelSimilarityFloor: "0.6", labelMemory: "64", labelEgressGrants: "[]", labelIngressGrants: "[]",
	} {
		if l[k] != want {
			t.Errorf("label %s = %q, want %q", k, l[k], want)
		}
	}

	sc, err := f.e.c.ContainerInspect(ctx, node.Sidecar, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := sc.Container.NetworkSettings.Networks[f.n.dockerNetwork()]; !ok {
		t.Errorf("the Integration Sidecar is not on %s", f.n.dockerNetwork())
	}
	var mounted []string
	for _, m := range sc.Container.Mounts {
		mounted = append(mounted, m.Name+":"+m.Destination)
	}
	slices.Sort(mounted)
	if want := []string{node.Private + ":" + sidecar.StateDir, node.Shared + ":" + sidecar.SharedDir}; !slices.Equal(mounted, want) {
		t.Errorf("the Integration Sidecar mounts %v, want %v", mounted, want)
	}
	if sc.Container.HostConfig.Memory != 32<<20 || sc.Container.Config.Labels[labelMemory] != "32" {
		t.Errorf("the Integration Sidecar's memory %d, label %q", sc.Container.HostConfig.Memory, sc.Container.Config.Labels[labelMemory])
	}

	// What the Spawner hands over verifies against the root, through its certificate.
	var st sidecar.Startup
	if err := json.Unmarshal(f.readFile(t, ctx, node.Sidecar, sidecar.StartupPath), &st); err != nil {
		t.Fatal(err)
	}
	if st.Root != f.n.root || st.NRIVersion != "1.0" {
		t.Errorf("startup root %q, nri_version %q", st.Root, st.NRIVersion)
	}
	cred, err := signing.VerifyCapability(st.Credential, f.n.root)
	if err != nil {
		t.Fatal(err)
	}
	c := cred.Claims
	if c.Sub != node.ID || c.Topic != "echo" || c.ArtifactRef != f.logic || c.ModelID != node.ModelID || c.SimilarityFloor != 0.6 || c.AttemptWindow == nil || *c.AttemptWindow.From != 1 {
		t.Errorf("credential claims %+v", c)
	}
	if v, err := embedding.Decode(c.CapabilityEmbedding); err != nil || len(v) != embeddingtest.Dimensions {
		t.Errorf("capability_embedding: %d dimensions, %v", len(v), err)
	}
	kp, err := nkeys.FromSeed([]byte(st.Identity))
	if err != nil {
		t.Fatal(err)
	}
	if pub, _ := kp.PublicKey(); pub != node.ID {
		t.Error("the startup identity is not the node's")
	}
	uc, err := jwt.DecodeUserClaims(st.BusJWT)
	if err != nil {
		t.Fatal(err)
	}
	if uc.Subject != node.ID || uc.Issuer != f.signer || uc.IssuerAccount != f.iss.Account || time.Until(time.Unix(uc.Expires, 0)) > NodeIdentityLifetime {
		t.Errorf("the node's JWT: %+v", uc.ClaimsData)
	}

	// The shared volume belongs to the UID both halves run under, and to no other.
	out, err := f.e.RunEphemeral(ctx, Spec{
		Name: f.n.container("stat"), Network: f.n.dockerNetwork(), Labels: f.n.labels(), Image: testBase,
		Cmd:    []string{"stat", "-c", "%u %a", sidecar.SharedDir, sidecar.StateDir},
		Mounts: sc.Container.HostConfig.Mounts,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Fields(string(out)); !slices.Equal(got, []string{"65532", "700", "65532", "700"}) {
		t.Errorf("the volumes' owner and mode: %q", out)
	}

	cons, err := f.js.Consumer(ctx, "task", node.ID)
	if err != nil {
		t.Fatal(err)
	}
	if fs := cons.CachedInfo().Config.FilterSubjects; !slices.Contains(fs, "task.announced.echo") || !slices.Contains(fs, "task.assigned."+node.ID) {
		t.Errorf("the node's consumer filters %v", fs)
	}
	// zobik init leaves it alone.
	if err := bus.EnsureConsumers(ctx, f.js, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := f.js.Consumer(ctx, "task", node.ID); err != nil {
		t.Errorf("zobik init deleted the node's consumer: %v", err)
	}
}

// A precondition the act does not meet refuses it with its failure_code, and
// leaves nothing behind.
func TestProvisionRefused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	f := newProvisionFixture(t, ctx)
	cases := []struct {
		name string
		b    Blueprint
		code string
	}{
		{"an egress slot no entry implements", Blueprint{Requires: map[string]json.RawMessage{"egress_required": json.RawMessage(`[{"name":"api","interface":` + iface + `,"operations":["read"]}]`)}}, codeEgressSlotUnbound},
		{"a malformed egress slot", Blueprint{Requires: map[string]json.RawMessage{"egress_required": json.RawMessage(`[{"name":"model"}]`)}}, codeRequestInvalid},
		{"an artifact_ref that is not a digest", Blueprint{ArtifactRef: f.sidecar[len("sha256:"):]}, codeRequestInvalid},
		{"an unknown precondition", Blueprint{Requires: map[string]json.RawMessage{"gpu_required": json.RawMessage(`1`)}}, codeRequestInvalid},
		{"an image that declares a volume", Blueprint{ArtifactRef: f.artifact(t, ctx, "LABEL "+LabelNRIVersion+"=1.0", "VOLUME /data")}, codeRequestInvalid},
		{"memory under the engine's limit", Blueprint{Requires: map[string]json.RawMessage{"memory_required": json.RawMessage(`1`)}}, codeRequestInvalid},
		{"an image without its NRI version", Blueprint{ArtifactRef: f.unnamed}, codeRuntimeUnsupported},
		{"an image of a minor the sidecar does not speak", Blueprint{ArtifactRef: f.artifact(t, ctx, "LABEL "+LabelNRIVersion+"=1.1", `CMD ["sleep", "3600"]`)}, codeRuntimeUnsupported},
		{"an image of another major", Blueprint{ArtifactRef: f.artifact(t, ctx, "LABEL "+LabelNRIVersion+"=2.0", `CMD ["sleep", "3600"]`)}, codeRuntimeUnsupported},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Provision(ctx, f.e, f.js, f.iss, f.request(tc.b))
			var pe *ProvisionError
			if !errors.As(err, &pe) || pe.Code != tc.code {
				t.Fatalf("got %v, want %s", err, tc.code)
			}
			if n := f.resources(t, ctx); n != 0 {
				t.Errorf("%d resources left behind", n)
			}
		})
	}

	// An act that fails halfway removes what it had created.
	req := f.request(Blueprint{})
	req.SidecarImage = f.image(t, ctx, `CMD ["/absent"]`)
	if _, err := Provision(ctx, f.e, f.js, f.iss, req); err == nil {
		t.Fatal("provisioned with an Integration Sidecar that does not start")
	}
	if n := f.resources(t, ctx); n != 0 {
		t.Errorf("%d resources left behind", n)
	}
	stream, err := f.js.Stream(ctx, "task")
	if err != nil {
		t.Fatal(err)
	}
	if info, err := stream.Info(ctx); err != nil || info.State.Consumers != 0 {
		t.Errorf("consumers left behind: %v", err)
	}
}

func (f *provisionFixture) readFile(t *testing.T, ctx context.Context, name, path string) []byte {
	t.Helper()
	res, err := f.e.c.CopyFromContainer(ctx, name, client.CopyFromContainerOptions{SourcePath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer res.Content.Close()
	tr := tar.NewReader(res.Content)
	if _, err := tr.Next(); err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(tr)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
