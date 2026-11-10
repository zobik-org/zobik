package spawner

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"time"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/claim"
	"zobik.org/zobik/internal/role/catalog"
	"zobik.org/zobik/internal/signing"
)

// audiencePrefix prefixes the key of an audience without a channel
// (architecture §3.13.3, implementation §1.2.11).
const audiencePrefix = "audience:"

// codeUndelivered is the failure_code of a query that did not reach the person
// (architecture §3.5.2).
const codeUndelivered = "undelivered"

// The audiences whose channel the deployment brings up (architecture §3.5.3).
var deploymentAudiences = map[string]bool{"operator": true, "user": true}

// audienceKey is the value of the key of an audience without a channel: since
// when the condition holds (architecture §3.13.2).
type audienceKey struct {
	Since time.Time `json:"since"`
}

// closeVacantQuery closes, on its first vacancy, a hitl_contact_* query no
// channel claimed: task.failed with undelivered and vacant, without having it
// assigned, addressed to the requester its announcement declares. It is
// published when the re-announcement shows the round went without a proposal,
// so the re-announced round is already open (architecture §3.13.2,
// implementation §1.2.4).
func (s *spawner) closeVacantQuery(ctx context.Context, ann *bus.Event, topic string) error {
	var d struct {
		Requester string `json:"requester_identity"`
	}
	_ = json.Unmarshal(ann.Data, &d) // a field of another type reads as absent
	if d.Requester == "" {
		return errors.New("the announcement declares no requester_identity")
	}
	data := (&claim.Assignment{Announcement: ann}).Inherited()
	data["topic"] = topic
	data["failure_code"] = codeUndelivered
	data["failure_reason"] = "no channel of the audience claimed the query in its round"
	data["vacant"] = true
	data["metrics"] = map[string]any{"tokens_in": 0, "tokens_out": 0, "cost": 0, "duration_ms": 0}
	e, err := bus.NewEvent(bus.TypeFailed, s.rt.ID, ann.Subject, data)
	if err != nil {
		return err
	}
	e.TraceParent, e.ConfigVersion = ann.TraceParent, ann.ConfigVersion
	if err := bus.Publish(ctx, s.js, bus.SubjectTerminal(bus.TypeFailed, d.Requester, topic), e); err != nil {
		return err
	}
	log.Printf("spawner: %s on %s vacant: closed undelivered", ann.Subject, topic)
	s.unreachable(ctx, ann, topic)
	return nil
}

// unreachable stamps the key of the audience without a channel when what the
// vacant round lacked is the channel: no row of the Active Node Registry
// carries its topic or, in operator and user, the vacant round is the final
// one, the round their deployment channel waits for the person in. Its notice
// enters with La contención (architecture §3.13.2).
func (s *spawner) unreachable(ctx context.Context, ann *bus.Event, topic string) {
	audience := strings.TrimPrefix(topic, signing.HITLPrefix)
	if deploymentAudiences[audience] {
		if !claim.ReadRound(ann).FinalRound {
			return
		}
	} else {
		fleet, err := s.o.Driver.Fleet(ctx)
		if err != nil {
			log.Printf("spawner: reading the fleet for %s: %v", topic, err)
			return
		}
		for _, n := range fleet {
			if n.Topic == topic {
				return
			}
		}
	}
	s.vmu.Lock()
	known := s.audiences[audience]
	s.audiences[audience] = true
	s.vmu.Unlock()
	if known {
		return
	}
	cat, err := s.o.Catalog(ctx)
	if err != nil {
		log.Printf("spawner: stamping the audience %s: %v", audience, err)
		return
	}
	err = cat.PutRecord(ctx, s.o.Network, audiencePrefix+audience, audienceKey{Since: time.Now().UTC()}, catalog.Write{IfAbsent: true})
	if err != nil && !errors.Is(err, catalog.ErrPrecondition) {
		log.Printf("spawner: stamping the audience %s: %v", audience, err)
		s.vmu.Lock()
		delete(s.audiences, audience)
		s.vmu.Unlock()
	}
}

// reached deletes the key of the audience of topic, a query of which stopped
// being left vacant: a channel was awarded it (architecture §3.13.2).
func (s *spawner) reached(ctx context.Context, topic string) {
	audience := strings.TrimPrefix(topic, signing.HITLPrefix)
	s.vmu.Lock()
	known := s.audiences[audience]
	delete(s.audiences, audience)
	s.vmu.Unlock()
	if !known {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), opTimeout)
		defer cancel()
		cat, err := s.o.Catalog(ctx)
		if err != nil {
			return
		}
		deleteKey(ctx, cat, s.o.Network, audiencePrefix+audience)
	}()
}

// loadAudiences reads the audiences the network has without a channel, so an
// award after a restart deletes their keys.
func (s *spawner) loadAudiences(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	cat, err := s.o.Catalog(ctx)
	if err != nil {
		return
	}
	records, err := cat.Records(ctx, audiencePrefix, s.o.Network)
	if err != nil {
		log.Printf("spawner: reading the audiences without a channel: %v", err)
		return
	}
	s.vmu.Lock()
	defer s.vmu.Unlock()
	for _, r := range records {
		s.audiences[strings.TrimPrefix(r.Key, audiencePrefix)] = true
	}
}
