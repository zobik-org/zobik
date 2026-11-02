package deploy

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/egress"
	"zobik.org/zobik/internal/role"
	"zobik.org/zobik/internal/role/catalog"
	"zobik.org/zobik/internal/role/secrets"
	"zobik.org/zobik/internal/role/spawner"
	"zobik.org/zobik/internal/sidecar"
)

// engineSocket is where the container engine's socket is, on the host and in
// the container that mounts it.
const engineSocket = "/var/run/docker.sock"

// NewIssuer loads the Spawner's material from its role's runtime: its signing
// key, its account, the tenant CA and its connection to the secrets role.
func NewIssuer(ctx context.Context, rt *role.Runtime, js jetstream.JetStream) (*Issuer, error) {
	seed, err := os.ReadFile(filepath.Join(rt.Dir(), role.FileSigningKey))
	if err != nil {
		return nil, fmt.Errorf("deploy: the spawner's signing key: %w", err)
	}
	key, err := nkeys.FromSeed([]byte(strings.TrimSpace(string(seed))))
	if err != nil {
		return nil, fmt.Errorf("deploy: the spawner's signing key: %w", err)
	}
	account, err := rt.Account()
	if err != nil {
		return nil, err
	}
	tenantCA, err := rt.TenantCA()
	if err != nil {
		return nil, err
	}
	roots, err := egress.Roots(tenantCA)
	if err != nil {
		return nil, err
	}
	// The Spawner resolves the embedding model's credential and its Catalog
	// credential, and reads the rate its embeddings are valued with
	// (implementation §1.2.11, §1.2.15, §8.5).
	sc, closeSecrets, err := secrets.Dial(role.AddrSecrets, rt.RPCCredentials(), "")
	if err != nil {
		return nil, err
	}
	return &Issuer{
		ID: rt.ID, Sign: rt.Sign, Certificate: rt.Certificate, SigningKey: key,
		Account: account.Subject, Root: rt.Root, TenantCA: string(tenantCA),
		Egress:  &egress.Executor{HTTP: egress.HTTPClient(roots), Credential: sc.Resolve, Rates: egress.BucketRates(ctx, js)},
		Resolve: sc.Resolve,
		close:   closeSecrets,
	}, nil
}

// SpawnerCatalog returns the Spawner's client of the tenant's Catalog, with the
// network's spawner credential from the Secret Store, or spawner.ErrNoCatalog
// for a network that has none (implementation §1.2.11).
func SpawnerCatalog(iss *Issuer) func(ctx context.Context) (spawner.Catalog, error) {
	var mu sync.Mutex
	var c *catalog.Client
	return func(ctx context.Context) (spawner.Catalog, error) {
		mu.Lock()
		defer mu.Unlock()
		if c != nil {
			return c, nil
		}
		cred, err := iss.Resolve(ctx, secrets.HandleCatalog)
		if status.Code(err) == codes.NotFound {
			return nil, spawner.ErrNoCatalog
		}
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(iss.TenantCA)) {
			return nil, errors.New("deploy: tenant CA: no certificate")
		}
		tr := http.DefaultTransport.(*http.Transport).Clone()
		tr.Proxy = nil
		tr.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS13}
		c = &catalog.Client{
			URL:        "https://" + CatalogAlias,
			Credential: string(cred),
			HTTP:       &http.Client{Transport: tr, Timeout: time.Minute},
		}
		return c, nil
	}
}

// Driver is the Spawner's deployment platform on the container engine: the act
// of provisioning, the fleet as the Active Node Registry's labels list it and
// the renewal of each node's user JWT (implementation §1.2.4, §1.2.14).
type Driver struct {
	Engine  *Engine
	JS      jetstream.JetStream
	Issuer  *Issuer
	Network string
	// SidecarImage is the Integration Sidecar's image, and Registry the address
	// the engine pulls the artifacts from.
	SidecarImage, Registry string
	FleetCapacityMB        int64
	IdentityLifetime       time.Duration
}

// Provision executes the act of provisioning of p.
func (d *Driver) Provision(ctx context.Context, p spawner.Provisioning) (*spawner.Node, error) {
	e := p.Entry
	node, err := Provision(ctx, d.Engine, d.JS, d.Issuer, ProvisionRequest{
		Network: d.Network, Topic: p.Topic,
		Blueprint: Blueprint{
			ArtifactRef: e.ArtifactRef, CapabilityText: e.CapabilityText, AuthorID: e.AuthorID,
			AttemptWindow: e.AttemptWindow, Requires: e.Requires,
		},
		SidecarImage: d.SidecarImage, Registry: d.Registry,
		Associations: p.Associations, Proof: p.Proof, ProvisionGrants: p.ProvisionGrants,
		MemoryMB: p.MemoryMB, FleetCapacityMB: d.FleetCapacityMB, IdentityLifetime: d.IdentityLifetime,
	})
	if err != nil {
		return nil, err
	}
	return &spawner.Node{ID: node.ID, ModelID: node.ModelID, Embedding: node.Embedding, MemoryMB: node.MemoryRequiredMB}, nil
}

// Fleet lists the network's nodes by their Logic Containers, the ones that
// carry the topic label (architecture §3.11).
func (d *Driver) Fleet(ctx context.Context) ([]spawner.Running, error) {
	cs, err := d.Engine.Containers(ctx, map[string]string{labelNetwork: d.Network, labelTopic: ""})
	if err != nil {
		return nil, err
	}
	var out []spawner.Running
	for _, c := range cs {
		// The engine runs the artifact by digest (implementation §1.2.16).
		_, digest, ok := strings.Cut(c.Image, "@")
		if !ok {
			continue
		}
		out = append(out, spawner.Running{NodeID: c.Labels[labelNode], Topic: c.Labels[labelTopic], ArtifactRef: digest})
	}
	return out, nil
}

// Renew writes the node's startup configuration again with a new user JWT;
// the Integration Sidecar reads it on each connection (implementation §1.2.4).
func (d *Driver) Renew(ctx context.Context, n spawner.Running) error {
	name := d.Engine.nodeSidecar(ctx, d.Network, n.NodeID)
	raw, err := d.Engine.ReadFile(ctx, name, sidecar.StartupPath)
	if err != nil {
		return err
	}
	var st sidecar.Startup
	if err := json.Unmarshal(raw, &st); err != nil {
		return fmt.Errorf("deploy: %s's startup configuration: %w", name, err)
	}
	lifetime := d.IdentityLifetime
	if lifetime <= 0 {
		lifetime = NodeIdentityLifetime
	}
	if st.BusJWT, err = bus.IssueUser(d.Issuer.SigningKey, d.Issuer.Account, n.NodeID, n.Topic, time.Now().Add(lifetime)); err != nil {
		return err
	}
	if raw, err = json.Marshal(st); err != nil {
		return err
	}
	return d.Engine.WriteFiles(ctx, name, []File{{Path: sidecar.StartupPath, Mode: 0o400, UID: nodeUID, Data: raw}})
}

// nodeSidecar names the Integration Sidecar of a node of network.
func (e *Engine) nodeSidecar(_ context.Context, network, id string) string {
	return names{network: network}.container("node-"+strings.ToLower(id[:12])) + "-sidecar"
}

// fleetMemory adds up the reservations the network's fleet carries in its
// labels, in MB: both halves of every node (implementation §1.2.4).
func (e *Engine) fleetMemory(ctx context.Context, network string) (int64, error) {
	cs, err := e.Containers(ctx, map[string]string{labelNetwork: network, labelNode: ""})
	if err != nil {
		return 0, err
	}
	var sum int64
	for _, c := range cs {
		mb, err := strconv.ParseInt(c.Labels[labelMemory], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("deploy: %s carries no %s: %w", c.Name, labelMemory, err)
		}
		sum += mb
	}
	return sum, nil
}

// Close closes the issuer's connection to the secrets role.
func (iss *Issuer) Close() {
	if iss.close != nil {
		_ = iss.close()
	}
}
