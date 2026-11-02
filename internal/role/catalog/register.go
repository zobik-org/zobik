package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"zobik.org/zobik/internal/digest"
	scannerv1 "zobik.org/zobik/proto/zobik/scanner/v1"
)

// Scanner is the Admission Scanner's service, which the act of registration
// calls (implementation §1.2.19).
type Scanner interface {
	Scan(ctx context.Context, in *scannerv1.ScanRequest, opts ...grpc.CallOption) (*scannerv1.ScanResult, error)
}

// The tags of implementation §1.2.16 the role reads and writes: a digest enters
// a tag as sha256-<hex>.
const (
	tagBase  = "base-"
	tagEntry = "entry-"
)

func entryTag(d string) string { return tagEntry + strings.Replace(d, ":", "-", 1) }

// registry is the tenant's image registry as the role reaches it: it resolves
// a digest, measures what the artifact adds to the base images and tags it.
type registry struct {
	repo name.Repository
	opts []remote.Option

	mu sync.Mutex
	// baseLayers caches the layers of each base-* tag, which names its digest
	// and so never changes.
	baseLayers map[string][]v1.Hash
}

func newRegistry(repository string, rt http.RoundTripper) (*registry, error) {
	repo, err := name.NewRepository(repository)
	if err != nil {
		return nil, err
	}
	return &registry{repo: repo, opts: []remote.Option{remote.WithTransport(rt)}, baseLayers: map[string][]v1.Hash{}}, nil
}

// errNotFound is a digest that does not resolve in the registry.
var errNotFound = errors.New("catalog: the digest does not resolve")

func (r *registry) resolve(ctx context.Context, d string) (*remote.Descriptor, error) {
	desc, err := remote.Get(r.repo.Digest(d), append(r.opts, remote.WithContext(ctx))...)
	var te *transport.Error
	if errors.As(err, &te) && te.StatusCode == http.StatusNotFound {
		return nil, errNotFound
	}
	return desc, err
}

// size is the artifact_size: the sum of the layers of the manifest that are in
// no base-* image's, since the base images of earlier versions stay in the
// registry (implementation §1.2.16).
func (r *registry) size(ctx context.Context, desc *remote.Descriptor) (int64, error) {
	img, err := desc.Image()
	if err != nil {
		return 0, err
	}
	m, err := img.Manifest()
	if err != nil {
		return 0, err
	}
	base, err := r.bases(ctx)
	if err != nil {
		return 0, err
	}
	var n int64
	for _, l := range m.Layers {
		if !base[l.Digest] {
			n += l.Size
		}
	}
	return n, nil
}

func (r *registry) bases(ctx context.Context) (map[v1.Hash]bool, error) {
	tags, err := remote.List(r.repo, append(r.opts, remote.WithContext(ctx))...)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := map[v1.Hash]bool{}
	for _, tag := range tags {
		if !strings.HasPrefix(tag, tagBase) {
			continue
		}
		layers, ok := r.baseLayers[tag]
		if !ok {
			img, err := remote.Image(r.repo.Tag(tag), append(r.opts, remote.WithContext(ctx))...)
			if err != nil {
				return nil, err
			}
			m, err := img.Manifest()
			if err != nil {
				return nil, err
			}
			for _, l := range m.Layers {
				layers = append(layers, l.Digest)
			}
			r.baseLayers[tag] = layers
		}
		for _, l := range layers {
			out[l] = true
		}
	}
	return out, nil
}

// tag puts the entry-* tag on the manifest, which makes the registry retain it
// (implementation §1.2.16).
func (r *registry) tag(ctx context.Context, desc *remote.Descriptor) error {
	return remote.Tag(r.repo.Tag(entryTag(desc.Digest.String())), desc, append(r.opts, remote.WithContext(ctx))...)
}

// result is what a request answers: its HTTP status, its body and, for a
// record, its revision as ETag.
type result struct {
	status int
	body   any
	etag   string
}

func respond(status int, body any) result { return result{status: status, body: body} }

func failure(f Failure) result { return respond(http.StatusUnprocessableEntity, f) }

// register is the act of registration (architecture §3.13.3, implementation
// §1.2.11), on behalf of network: the image has to resolve, pass the analysis,
// declare its NRI version and fit in the capacity. Registering the same half
// again returns the entry and removes its deletion mark.
func (c *Catalog) register(ctx context.Context, network string, body []byte) (result, error) {
	var reg Registration
	if err := json.Unmarshal(body, &reg); err != nil {
		return respond(http.StatusBadRequest, errorBody(err.Error())), nil
	}
	if err := reg.validate(); err != nil {
		return respond(http.StatusBadRequest, errorBody(err.Error())), nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return respond(http.StatusBadRequest, errorBody(err.Error())), nil
	}
	delete(fields, "acquisition")
	half, err := json.Marshal(fields)
	if err != nil {
		return result{}, err
	}
	halfDigest, err := digest.JSON(half)
	if err != nil {
		return respond(http.StatusBadRequest, errorBody(err.Error())), nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	existing, err := c.store.entry(ctx, reg.ArtifactRef)
	if err != nil {
		return result{}, err
	}
	if existing != nil && existing.HalfDigest != halfDigest {
		return respond(http.StatusConflict, errorBody("another half is registered under "+reg.ArtifactRef)), nil
	}
	desc, err := c.registry.resolve(ctx, reg.ArtifactRef)
	if errors.Is(err, errNotFound) {
		return failure(Failure{Reason: ReasonUnresolvable}), nil
	}
	if err != nil {
		return result{}, fmt.Errorf("resolving %s: %w", reg.ArtifactRef, err)
	}

	if existing != nil {
		if existing.DeletionMark {
			// It counts again toward what is occupied.
			if full, err := c.full(ctx, existing.ArtifactSize); err != nil || full {
				return failure(Failure{Reason: ReasonStorageFull}), err
			}
		}
		if err := c.registry.tag(ctx, desc); err != nil {
			return result{}, fmt.Errorf("tagging %s: %w", reg.ArtifactRef, err)
		}
		if existing.DeletionMark {
			if _, err := c.store.setDeletionMark(ctx, reg.ArtifactRef, false); err != nil {
				return result{}, err
			}
			existing.DeletionMark = false
		}
		if err := c.acquire(ctx, network, reg); err != nil {
			return result{}, err
		}
		v, err := existing.view()
		return respond(http.StatusOK, v), err
	}

	scan, err := c.scanner.Scan(ctx, &scannerv1.ScanRequest{ArtifactRef: reg.ArtifactRef})
	if status.Code(err) == codes.NotFound {
		return failure(Failure{Reason: ReasonUnresolvable}), nil
	}
	if err != nil {
		return result{}, fmt.Errorf("analyzing %s: %w", reg.ArtifactRef, err)
	}
	if scan.Verdict != verdictPass {
		return failure(Failure{Reason: ReasonScanFailed, RulesetVersion: scan.RulesetVersion, Findings: scan.Findings}), nil
	}
	if scan.NriVersion == "" {
		return failure(Failure{Reason: ReasonNRIVersionMissing}), nil
	}
	size, err := c.registry.size(ctx, desc)
	if err != nil {
		return result{}, fmt.Errorf("measuring %s: %w", reg.ArtifactRef, err)
	}
	if full, err := c.full(ctx, size); err != nil || full {
		return failure(Failure{Reason: ReasonStorageFull}), err
	}
	if err := c.registry.tag(ctx, desc); err != nil {
		return result{}, fmt.Errorf("tagging %s: %w", reg.ArtifactRef, err)
	}
	e := &entry{
		Digest:       reg.ArtifactRef,
		Half:         half,
		HalfDigest:   halfDigest,
		NRIVersion:   scan.NriVersion,
		ArtifactSize: size,
		Scan:         ScanResult{Verdict: scan.Verdict, RulesetVersion: scan.RulesetVersion, AsOf: c.store.stamp()},
	}
	if err := c.store.insertEntry(ctx, e); err != nil {
		return result{}, err
	}
	if err := c.acquire(ctx, network, reg); err != nil {
		return result{}, err
	}
	v, err := e.view()
	return respond(http.StatusCreated, v), err
}

// verdictPass is the Admission Scanner's verdict that admits an artifact.
const verdictPass = "pass"

// full says whether size more does not fit in the capacity.
func (c *Catalog) full(ctx context.Context, size int64) (bool, error) {
	occupied, err := c.store.occupied(ctx)
	if err != nil {
		return false, err
	}
	return occupied+size > c.capacity, nil
}

// acquire writes the registration's acquisition row as the record
// acquisition:<digest> of network, the only qualified record a register
// credential writes. It is a fact of how the entry reached the network, so a
// row already written stays (implementation §1.2.11).
func (c *Catalog) acquire(ctx context.Context, network string, reg Registration) error {
	if reg.Acquisition == nil {
		return nil
	}
	value, err := json.Marshal(reg.Acquisition)
	if err != nil {
		return err
	}
	_, err = c.store.putRecord(ctx, network, "acquisition:"+reg.ArtifactRef, value, condition{absent: true}, 0)
	if errors.Is(err, errPrecondition) {
		return nil
	}
	return err
}

// registerBundle registers a bundle whose members are registered entries; its
// bundle_id is the digest of its content, members in order.
func (c *Catalog) registerBundle(ctx context.Context, body []byte) (result, error) {
	var b Bundle
	if err := json.Unmarshal(body, &b); err != nil {
		return respond(http.StatusBadRequest, errorBody(err.Error())), nil
	}
	if b.PurposeText == "" || len(b.Members) == 0 {
		return respond(http.StatusBadRequest, errorBody("a bundle carries its purpose_text and its members")), nil
	}
	for _, m := range b.Members {
		if !digestPattern.MatchString(m) {
			return respond(http.StatusBadRequest, errorBody(fmt.Sprintf("member %q is not a sha256 digest", m))), nil
		}
		e, err := c.store.entry(ctx, m)
		if err != nil {
			return result{}, err
		}
		if e == nil {
			return failure(Failure{Reason: ReasonUnregisteredMember, Member: m}), nil
		}
	}
	doc, err := json.Marshal(Bundle{PurposeText: b.PurposeText, Members: b.Members})
	if err != nil {
		return result{}, err
	}
	if b.BundleID, err = digest.JSON(doc); err != nil {
		return result{}, err
	}
	if err := c.store.putBundle(ctx, &b); err != nil {
		return result{}, err
	}
	return respond(http.StatusOK, b), nil
}
