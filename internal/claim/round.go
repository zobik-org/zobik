package claim

import (
	"encoding/json"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/embedding"
	"zobik.org/zobik/internal/signing"
)

// Round is where in its lineage an announcement stands (architecture §3.3, §3.5.2).
type Round struct {
	CurrentAttempts *int   `json:"current_attempts"`
	FinalRound      bool   `json:"final_round"`
	OriginAudience  string `json:"origin_audience"`
}

// ReadRound reads the round from an announcement's data.
func ReadRound(ann *bus.Event) Round {
	var r Round
	_ = json.Unmarshal(ann.Data, &r) // a field of another type reads as absent
	return r
}

// Attempts is the attempt number, 1 when the announcement carries none.
func (r Round) Attempts() int {
	if r.CurrentAttempts == nil {
		return 1
	}
	return *r.CurrentAttempts
}

// Admits reports whether the attempt_window a credential carries admits the
// round (architecture §3.5.2). A credential without one admits every round.
func Admits(w *signing.AttemptWindow, r Round) bool {
	if w == nil {
		return true
	}
	if r.FinalRound {
		return w.FinalRound == nil || *w.FinalRound
	}
	a := r.Attempts()
	return (w.From == nil || a >= *w.From) && (w.To == nil || a <= *w.To)
}

// ReservedTo reports whether the round is round 1 of the trace's origin
// audience, in which only the channel of that audience that owns the TraceID
// proposes (architecture §3.5.2).
func (r Round) ReservedTo(audience string) bool {
	return !r.FinalRound && r.Attempts() == 1 && r.OriginAudience == audience
}

// Similarity is the cosine between the credential's capability_embedding and
// the announcement's task_embedding, and whether, rounded to decimals, it
// exceeds the credential's similarity_floor (architecture §3.7.5, step 3). It is
// false for an announcement without a task_embedding or from another vector
// space: no vector is compared against one of another model_id (architecture §3.7.3).
func Similarity(cred *signing.Capability, ann *bus.Event, decimals int) (float64, bool) {
	var d struct {
		TaskEmbedding string `json:"task_embedding"`
		ModelID       string `json:"model_id"`
	}
	if err := json.Unmarshal(ann.Data, &d); err != nil || d.TaskEmbedding == "" || d.ModelID != cred.ModelID {
		return 0, false
	}
	task, err := embedding.Decode(d.TaskEmbedding)
	if err != nil {
		return 0, false
	}
	capability, err := embedding.Decode(cred.CapabilityEmbedding)
	if err != nil {
		return 0, false
	}
	s, err := embedding.Similarity(capability, task)
	if err != nil {
		return 0, false
	}
	return s, embedding.Round(s, decimals) > embedding.Round(cred.SimilarityFloor, decimals)
}
