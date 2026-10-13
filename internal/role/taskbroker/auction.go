package taskbroker

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"

	"zobik.org/zobik/internal/embedding"
	"zobik.org/zobik/internal/signing"
)

// announced is what an auction judges its proposals against: the task as the
// Task Broker holds it (architecture §3.1).
type announced struct {
	topic, taskID string
	// embedding is nil when the announcement carries no task_embedding.
	embedding []float32
	modelID   string
	// decimals is similarity_precision_decimals of the snapshot the trace pinned.
	decimals int
}

// proposal is a task.proposed: the identity the subject attributes to it and
// what its data carries.
type proposal struct {
	identity   string
	declared   *float64
	credential string
}

// verdict is a proposal judged by step 4 of architecture §3.7.5.
type verdict struct {
	identity string
	// score is the rounded s_effective, in units of 10^-decimals; a structural
	// proposal that declares nothing ranks lowest.
	score int64
	// similarity is the value published in task.assigned or task.rejected;
	// absent for a structural proposal that declares nothing.
	similarity *float64
	// admissible is false when s_effective does not exceed the credential's floor.
	admissible bool
}

// errDiscarded is a proposal the Task Broker drops without answering it.
var errDiscarded = errors.New("discarded")

func discard(format string, a ...any) error {
	return fmt.Errorf("%w: %s", errDiscarded, fmt.Sprintf(format, a...))
}

// credentials caches verified credentials by identity: the credential is static
// and travels in every proposal, so verifying its signature once is enough; each
// proposal still has to arrive under the identity it names (architecture §3.7.5, step 4).
type credentials struct {
	root string
	mu   sync.Mutex
	byID map[string]cached
}

type cached struct {
	token string
	v     *signing.Verified[signing.Capability]
}

func newCredentials(root string) *credentials {
	return &credentials{root: root, byID: map[string]cached{}}
}

func (c *credentials) verify(identity, token string) (*signing.Verified[signing.Capability], error) {
	c.mu.Lock()
	hit, ok := c.byID[identity]
	c.mu.Unlock()
	if ok && hit.token == token {
		return hit.v, nil
	}
	v, err := signing.VerifyCapability(token, c.root)
	if err != nil {
		return nil, err
	}
	if v.Claims.Sub != identity {
		return nil, errors.New("the credential's subject is not the publisher")
	}
	c.mu.Lock()
	c.byID[identity] = cached{token: token, v: v}
	c.mu.Unlock()
	return v, nil
}

// judge applies step 4 of architecture §3.7.5 to p. The class of the proposer
// comes from the depth of its credential's chain: a structural one verifies
// directly against the root, an emergent one through a spawner certificate.
func judge(creds *credentials, t *announced, p proposal) (verdict, error) {
	cred, err := creds.verify(p.identity, p.credential)
	if err != nil {
		return verdict{}, discard("credential: %v", err)
	}
	structural := cred.Header.SignerCredential == ""
	switch {
	case isHITL(t.topic):
		// Only the channel credentials of the audience the topic names are
		// admitted, and an emergent one with the proof that brought it up
		// (architecture §3.14.2).
		if cred.Claims.Topic != t.topic {
			return verdict{}, discard("not a channel of %s", t.topic)
		}
		if !structural {
			if err := checkChannelProof(creds.root, t.topic, &cred.Claims); err != nil {
				return verdict{}, discard("%v", err)
			}
		}
		return byDeclared(p, t.decimals), nil
	case structuralTopics[t.topic]:
		if !structural {
			return verdict{}, discard("%s is claimed by a structural component", t.topic)
		}
		return byDeclared(p, t.decimals), nil
	case structural:
		// The Spawner's sentinel proposal on a vacant emergent topic (architecture §3.13.2).
		return byDeclared(p, t.decimals), nil
	}

	if t.embedding == nil {
		return verdict{}, discard("the announcement carries no task_embedding")
	}
	if cred.Claims.ModelID != t.modelID {
		return verdict{}, discard("model_id %q against the task's %q", cred.Claims.ModelID, t.modelID)
	}
	capability, err := embedding.Decode(cred.Claims.CapabilityEmbedding)
	if err != nil {
		return verdict{}, discard("capability_embedding: %v", err)
	}
	sCalc, err := embedding.Similarity(capability, t.embedding)
	if err != nil {
		return verdict{}, discard("%v", err)
	}
	// The declared value is a ceiling: it can lower the recomputed one, never
	// raise it. Rounding goes after the min (architecture §3.7.5, step 4).
	sEff := sCalc
	if p.declared != nil {
		sEff = math.Min(*p.declared, sCalc)
	}
	score := embedding.Round(sEff, t.decimals)
	sim := embedding.Scaled(score, t.decimals)
	return verdict{
		identity:   p.identity,
		score:      score,
		similarity: &sim,
		admissible: score > embedding.Round(cred.Claims.SimilarityFloor, t.decimals),
	}, nil
}

// byDeclared ranks a structural proposal by the similarity it declares, with no
// floor (architecture §3.7.5, step 4).
func byDeclared(p proposal, decimals int) verdict {
	v := verdict{identity: p.identity, score: math.MinInt64, admissible: true}
	if p.declared != nil {
		v.score = embedding.Round(*p.declared, decimals)
		sim := embedding.Scaled(v.score, decimals)
		v.similarity = &sim
	}
	return v
}

// checkChannelProof requires the embedded proof of an emergent channel to be
// that of a node_provision for the topic's audience and for the artifact the
// credential runs (architecture §3.14.2).
func checkChannelProof(root, topic string, cred *signing.Capability) error {
	proof, err := signing.VerifyOperatorProof(cred.OperatorProof, root)
	if err != nil {
		return fmt.Errorf("operator_proof: %w", err)
	}
	switch {
	case proof.Claims.Act != signing.ActNodeProvision:
		return fmt.Errorf("the proof is of %q, not of a node_provision", proof.Claims.Act)
	case proof.Claims.Audience != strings.TrimPrefix(topic, signing.HITLPrefix):
		return fmt.Errorf("the proof authorizes audience %q", proof.Claims.Audience)
	case proof.Claims.ArtifactRef != cred.ArtifactRef:
		return fmt.Errorf("the proof authorizes artifact %q", proof.Claims.ArtifactRef)
	}
	return nil
}

// award picks the highest admissible score; a tie is drawn by the highest
// hash(TaskID, node_id), which keeps the award recomputable from the trace
// (architecture §3.7.5, step 4, implementation §1.2.7).
func award(taskID string, vs []verdict) (winner int, ok bool) {
	winner = -1
	var best []byte
	for i, v := range vs {
		if !v.admissible {
			continue
		}
		h := drawHash(taskID, v.identity)
		if winner < 0 || v.score > vs[winner].score || v.score == vs[winner].score && bytes.Compare(h, best) > 0 { //nolint:gosec // vs[winner] is read only when winner >= 0
			winner, best = i, h
		}
	}
	return winner, winner >= 0
}

// drawHash is SHA-256 over the TaskID, a zero byte and the node's platform identity.
func drawHash(taskID, identity string) []byte {
	h := sha256.New()
	h.Write([]byte(taskID))
	h.Write([]byte{0})
	h.Write([]byte(identity))
	return h.Sum(nil)
}
