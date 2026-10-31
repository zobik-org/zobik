package deploy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/moby/moby/api/types/mount"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/egress"
	"zobik.org/zobik/internal/embedding"
	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/internal/sidecar"
	"zobik.org/zobik/internal/signing"
)

// The act of provisioning (architecture §3.13.1): it creates one node with
// everything it is born with, and evaluates the preconditions against the state
// the network has at that moment. The Spawner executes it.

// Blueprint is what the act reads from a Catalog entry: the shape of the act of
// registration (implementation §1.2.11).
type Blueprint struct {
	ArtifactRef    string                     `json:"artifact_ref"`
	CapabilityText string                     `json:"capability_text"`
	AuthorID       string                     `json:"author_id,omitempty"`
	AttemptWindow  *signing.AttemptWindow     `json:"attempt_window,omitempty"`
	Requires       map[string]json.RawMessage `json:"requires"`
}

// Issuer is the Spawner's material: the key it signs credentials with, its
// role certificate, and the scoped signing key it issues the fleet's platform
// identities with (implementation §1.2.4).
type Issuer struct {
	// ID is the Spawner's public key: the kid of what it signs, which the
	// registry carries as signer_id (implementation §9.1).
	ID          string
	Sign        func(typ signing.Type, claims any, signerCredential string) (string, error)
	Certificate string
	SigningKey  nkeys.KeyPair
	// Account is the network's account public key, and Root the root's.
	Account, Root string
	// TenantCA is the tenant CA's certificate, which the Integration Sidecar
	// receives in its startup configuration (implementation §1.2.4).
	TenantCA string
	// Egress executes the embeddings' calls, with the credential the secrets
	// role resolves for the Spawner (implementation §1.2.15, §1.2.18).
	Egress *egress.Executor
	close  func() error
}

// ProvisionRequest is one act: a blueprint on a topic, in a network.
type ProvisionRequest struct {
	Network      string    `json:"network"`
	Topic        string    `json:"topic"`
	Blueprint    Blueprint `json:"blueprint"`
	SidecarImage string    `json:"sidecar_image"`
	// Registry is the address the engine pulls the artifact from, the link's on
	// 127.0.0.1 (implementation §6, The tenant CA, the link and the join code).
	Registry string `json:"registry"`
}

// Node is what an act brought up.
type Node struct {
	// ID is the node's platform identity.
	ID               string
	Logic, Sidecar   string
	Shared, Private  string
	ModelID          string
	SimilarityFloor  float64
	MemoryRequiredMB int64
	EgressGrants     []signing.Grant
}

// ProvisionError is a precondition the act does not meet, with the failure_code
// a vacant task is closed with (implementation §10.1).
type ProvisionError struct {
	Code   string
	Reason string
}

func (e *ProvisionError) Error() string { return e.Code + ": " + e.Reason }

func refuse(code, format string, a ...any) error {
	return &ProvisionError{Code: code, Reason: fmt.Sprintf(format, a...)}
}

const (
	// nodeUID runs both halves of every node, and owns the shared volume and the
	// socket in it: the file's permission is the only thing that governs who
	// connects (implementation §2.1).
	nodeUID = 65532
	// sidecarMemory is the Integration Sidecar's fixed reservation (implementation §1.2.4).
	sidecarMemory = 32 << 20
	// minMemoryMB is the engine's smallest limit, and maxMemoryMB keeps the
	// limit in bytes within an int64.
	minMemoryMB, maxMemoryMB = 6, 1 << 40
	// NodeIdentityLifetime is how long a node's user JWT is valid; the Spawner
	// renews it while the deployment lives (implementation §1.2.4).
	NodeIdentityLifetime = time.Hour
	// LabelNRIVersion is the NRI version an image declares (implementation §3,
	// The interface version).
	LabelNRIVersion = "io.zobik.nri.version"
)

// The Active Node Registry's labels (architecture §3.11, implementation §1.2.14).
// Every resource of a node carries labelNode; the Logic Container carries the rest.
const (
	labelNode            = "org.zobik.node"
	labelTopic           = "org.zobik.topic"
	labelModelID         = "org.zobik.model_id"
	labelSignerID        = "org.zobik.signer_id"
	labelSimilarityFloor = "org.zobik.similarity_floor"
	labelEgressGrants    = "org.zobik.egress_grants"
	labelIngressGrants   = "org.zobik.ingress_grants"
	// labelMemory is the reservation in MB, on both halves: admission adds up
	// the fleet's, Integration Sidecar included (implementation §1.2.4).
	labelMemory = "org.zobik.memory_required"
)

// The failure codes of the act's preconditions (implementation §10.1).
const (
	codeRequestInvalid       = "request_invalid"
	codeRuntimeUnsupported   = "runtime_interface_unsupported"
	codeEgressSlotUnbound    = "egress_slot_unbound"
	codeIngressSlotUnbound   = "ingress_slot_unbound"
	codeEmbeddingUnavailable = "embedding_unavailable"
)

// Provision executes the act. A failed act removes what it created.
func Provision(ctx context.Context, e *Engine, js jetstream.JetStream, iss *Issuer, req ProvisionRequest) (node *Node, err error) {
	b := req.Blueprint
	if req.Topic == "" || strings.ContainsAny(req.Topic, ".*> \t\r\n") {
		return nil, refuse(codeRequestInvalid, "topic %q is not a single term", req.Topic)
	}
	if b.ArtifactRef == "" || b.CapabilityText == "" {
		return nil, refuse(codeRequestInvalid, "the blueprint needs artifact_ref and capability_text")
	}
	if !IsDigest(b.ArtifactRef) {
		return nil, refuse(codeRequestInvalid, "artifact_ref %q is not a digest", b.ArtifactRef)
	}
	artifact := PullRef(req.Registry, b.ArtifactRef)
	memoryMB, slots, ifaces, err := requires(b.Requires)
	if err != nil {
		return nil, err
	}

	head, _, err := globalconfig.Head(ctx, js)
	if err != nil {
		return nil, fmt.Errorf("deploy: reading the configuration: %w", err)
	}
	if memoryMB == 0 {
		if memoryMB, err = intValue(head.Values, "default_memory_required"); err != nil {
			return nil, err
		}
	}
	// An entry with no floor history receives task_similarity_floor (architecture §3.13.3).
	var floor float64
	if err := json.Unmarshal(head.Values["task_similarity_floor"], &floor); err != nil {
		return nil, fmt.Errorf("deploy: task_similarity_floor: %w", err)
	}
	registry := map[string]*egress.Entry{}
	if raw, ok := head.Values["egress_registry"]; ok {
		if registry, err = egress.ParseRegistry(raw); err != nil {
			return nil, fmt.Errorf("deploy: egress_registry: %w", err)
		}
	}
	grants, err := associate(slots, ifaces, registry, req.Topic)
	if err != nil {
		return nil, err
	}
	space, err := embedding.Resolve(head.Values)
	if err != nil {
		return nil, refuse(codeEmbeddingUnavailable, "%v", err)
	}
	vector, err := space.Document(ctx, iss.Egress, b.CapabilityText)
	if errors.Is(err, embedding.ErrTooLong) {
		return nil, refuse(codeRequestInvalid, "%v", err)
	}
	if err != nil {
		return nil, refuse(codeEmbeddingUnavailable, "%v", err)
	}

	// The engine verifies the artifact it pulls by digest (implementation §1.2.16).
	for _, img := range []string{artifact, req.SidecarImage} {
		if err := e.EnsureImage(ctx, img); err != nil {
			return nil, fmt.Errorf("deploy: image %s: %w", img, err)
		}
	}
	img, err := e.InspectImage(ctx, artifact)
	if err != nil {
		return nil, err
	}
	// A declared volume would be a second mount, writable and kept across
	// relaunches (implementation §2.2, §2.4).
	if len(img.Volumes) > 0 {
		return nil, refuse(codeRequestInvalid, "the image declares volumes %v", img.Volumes)
	}
	version, err := sidecar.ParseVersion(img.Labels[LabelNRIVersion])
	if err != nil {
		return nil, refuse(codeRuntimeUnsupported, "the image's %s: %v", LabelNRIVersion, err)
	}
	supported, err := sidecar.ReadRange(head.Values["runtime_interface_range"])
	if err != nil {
		return nil, fmt.Errorf("deploy: %w", err)
	}
	if !supported.Fits(version) {
		return nil, refuse(codeRuntimeUnsupported, "the image declares NRI %s, outside runtime_interface_range %v", version, supported)
	}

	user, err := nkeys.CreateUser()
	if err != nil {
		return nil, err
	}
	id, err := user.PublicKey()
	if err != nil {
		return nil, err
	}
	seed, err := user.Seed()
	if err != nil {
		return nil, err
	}
	token, err := bus.IssueUser(iss.SigningKey, iss.Account, id, req.Topic, time.Now().Add(NodeIdentityLifetime))
	if err != nil {
		return nil, err
	}
	credential, err := iss.Sign(signing.TypeCapability, signing.Capability{
		Sub:                 id,
		Iat:                 time.Now().Unix(),
		Topic:               req.Topic,
		ArtifactRef:         b.ArtifactRef,
		ModelID:             space.ModelID,
		CapabilityEmbedding: embedding.Encode(vector),
		SimilarityFloor:     floor,
		AttemptWindow:       b.AttemptWindow,
		EgressGrants:        grants,
	}, iss.Certificate)
	if err != nil {
		return nil, err
	}
	startup, err := json.Marshal(sidecar.Startup{
		Identity:   string(seed),
		BusJWT:     token,
		Credential: credential,
		Root:       iss.Root,
		NRIVersion: version.String(),
		TenantCA:   iss.TenantCA,
	})
	if err != nil {
		return nil, err
	}

	n := names{req.Network, iss.Root}
	base := n.container("node-" + strings.ToLower(id[:12]))
	node = &Node{
		ID: id, Logic: base + "-logic", Sidecar: base + "-sidecar",
		Shared: base + "-shared", Private: base + "-private",
		ModelID: space.ModelID, SimilarityFloor: floor, MemoryRequiredMB: memoryMB,
		EgressGrants: grants,
	}
	defer func() {
		if err == nil {
			return
		}
		cleanup := context.WithoutCancel(ctx)
		err = errors.Join(err,
			e.Remove(cleanup, []string{node.Logic, node.Sidecar}, []string{node.Shared, node.Private}),
			bus.DeleteNodeConsumer(cleanup, js, id))
		node = nil
	}()

	common := n.labels()
	common[labelNode] = id
	for _, v := range []string{node.Shared, node.Private} {
		if _, err := e.EnsureVolume(ctx, v, common); err != nil {
			return node, fmt.Errorf("deploy: volume %s: %w", v, err)
		}
	}
	if err := bus.NodeConsumer(ctx, js, id, req.Topic); err != nil {
		return node, err
	}

	uid := strconv.Itoa(nodeUID) + ":" + strconv.Itoa(nodeUID)
	sidecarLabels := with(common, labelMemory, strconv.Itoa(sidecarMemory>>20))
	err = e.EnsureRunning(ctx, Spec{
		Name:    node.Sidecar,
		Network: n.dockerNetwork(),
		Labels:  sidecarLabels,
		Image:   req.SidecarImage,
		Mounts: []mount.Mount{
			{Type: mount.TypeVolume, Source: node.Shared, Target: sidecar.SharedDir},
			{Type: mount.TypeVolume, Source: node.Private, Target: sidecar.StateDir},
		},
		Files: []File{
			{Path: sidecar.SharedDir, Dir: true, Mode: 0o700, UID: nodeUID},
			{Path: sidecar.StateDir, Dir: true, Mode: 0o700, UID: nodeUID},
			{Path: sidecar.StartupPath, Mode: 0o400, UID: nodeUID, Data: startup},
		},
		Restart: true,
		User:    uid,
		Memory:  sidecarMemory,
	})
	if err != nil {
		return node, fmt.Errorf("deploy: Integration Sidecar: %w", err)
	}

	grantsLabel, err := json.Marshal(grants)
	if err != nil {
		return node, err
	}
	if grants == nil {
		grantsLabel = []byte("[]")
	}
	logicLabels := with(common, labelMemory, strconv.FormatInt(memoryMB, 10))
	for k, v := range map[string]string{
		labelTopic:           req.Topic,
		labelModelID:         space.ModelID,
		labelSignerID:        iss.ID,
		labelSimilarityFloor: strconv.FormatFloat(floor, 'f', -1, 64),
		labelEgressGrants:    string(grantsLabel),
		labelIngressGrants:   "[]",
	} {
		logicLabels[k] = v
	}
	// The Logic Container mounts only the shared volume (implementation §2.2).
	err = e.EnsureRunning(ctx, Spec{
		Name:     node.Logic,
		Labels:   logicLabels,
		Image:    artifact,
		Mounts:   []mount.Mount{{Type: mount.TypeVolume, Source: node.Shared, Target: sidecar.SharedDir}},
		User:     uid,
		Memory:   memoryMB << 20,
		Confined: true,
	})
	if err != nil {
		return node, fmt.Errorf("deploy: Logic Container: %w", err)
	}
	return node, nil
}

// requires evaluates the provisioning preconditions the entry declares, and
// returns its memory_required, or zero if it declares none, and its egress
// slots (implementation §1.2.11). No ingress slot can be associated yet: an
// entry that requires one is not provisioned.
func requires(r map[string]json.RawMessage) (int64, []slot, []*egress.Interface, error) {
	var memoryMB int64
	var slots []slot
	var ifaces []*egress.Interface
	// In order, so the same entry is always refused with the same failure_code.
	for _, key := range slices.Sorted(maps.Keys(r)) {
		raw := r[key]
		switch key {
		case "memory_required":
			if err := json.Unmarshal(raw, &memoryMB); err != nil || memoryMB < minMemoryMB || memoryMB > maxMemoryMB {
				return 0, nil, nil, refuse(codeRequestInvalid, "memory_required is not an integer from %d to %d MB", minMemoryMB, maxMemoryMB)
			}
		case "egress_required":
			var err error
			if slots, ifaces, err = parseSlots(raw); err != nil {
				return 0, nil, nil, err
			}
		case "ingress_required":
			var list []json.RawMessage
			if err := json.Unmarshal(raw, &list); err != nil {
				return 0, nil, nil, refuse(codeRequestInvalid, "%s is not a list", key)
			}
			if len(list) > 0 {
				return 0, nil, nil, refuse(codeIngressSlotUnbound, "a slot of %s is not associated", key)
			}
		default:
			// A key of requires the Spawner does not know prevents provisioning (implementation §1.2.11).
			return 0, nil, nil, refuse(codeRequestInvalid, "requires.%s is not a precondition this Spawner knows", key)
		}
	}
	return memoryMB, slots, ifaces, nil
}

func intValue(v globalconfig.Values, key string) (int64, error) {
	var n int64
	if err := json.Unmarshal(v[key], &n); err != nil {
		return 0, fmt.Errorf("deploy: %s: %w", key, err)
	}
	return n, nil
}

func with(labels map[string]string, k, v string) map[string]string {
	out := map[string]string{k: v}
	for lk, lv := range labels {
		out[lk] = lv
	}
	return out
}
