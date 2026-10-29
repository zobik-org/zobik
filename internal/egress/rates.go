package egress

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"maps"
	"math"
	"regexp"
	"slices"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"zobik.org/zobik/internal/bus"
)

// Table is a rate table: the unit price of each component, by key, in
// cost_unit, with the date of its source and the charge of a call whose cost
// cannot be evaluated (implementation §8.5).
type Table struct {
	AsOf       string                        `json:"as_of"`
	MaxPerCall float64                       `json:"max_per_call"`
	Rates      map[string]map[string]float64 `json:"rates"`
}

// rateKey is the form of a rate_source, a key of the rates bucket.
var rateKey = regexp.MustCompile(`^[-/_=A-Za-z0-9]+([.][-/_=A-Za-z0-9]+)*$`).MatchString

// ValidRateSource reports whether name can name a table in the bucket.
func ValidRateSource(name string) bool { return rateKey(name) }

// ParseTable reads a table and checks it against its format: as_of is a date,
// and max_per_call and each price are non-negative numbers (implementation §8.5).
func ParseTable(raw []byte) (*Table, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var t Table
	if err := dec.Decode(&t); err != nil {
		return nil, fmt.Errorf("rate table: %w", err)
	}
	if dec.More() {
		return nil, errors.New("rate table: trailing data")
	}
	if _, err := time.Parse(time.DateOnly, t.AsOf); err != nil {
		return nil, fmt.Errorf("rate table: as_of %q is not a date", t.AsOf)
	}
	if !nonNegative(t.MaxPerCall) {
		return nil, errors.New("rate table: max_per_call is a non-negative number")
	}
	for _, key := range slices.Sorted(maps.Keys(t.Rates)) {
		for component, price := range t.Rates[key] {
			if !nonNegative(price) {
				return nil, fmt.Errorf("rate table: the price of %s.%s is a non-negative number", key, component)
			}
		}
	}
	return &t, nil
}

func nonNegative(x float64) bool { return x >= 0 && !math.IsInf(x, 0) }

// rateMap is the table's rates as the billing_rule reads them, empty without a table.
func (t *Table) rateMap() map[string]map[string]float64 {
	if t == nil || t.Rates == nil {
		return map[string]map[string]float64{}
	}
	return t.Rates
}

// RateSource returns the current table of a rate_source, and whether the bucket has it.
type RateSource func(name string) (*Table, bool)

// Rates keeps the rates bucket's tables in memory: a watch delivers each key's
// value on opening and then each change (implementation §8.5).
type Rates struct {
	mu     sync.RWMutex
	tables map[string]*Table
}

// WatchRates watches the whole bucket until ctx ends, reopening the watch
// whenever it ends: the reader tolerates the NATS server's absence.
func WatchRates(ctx context.Context, js jetstream.JetStream) *Rates {
	r := &Rates{tables: map[string]*Table{}}
	go func() {
		for ctx.Err() == nil {
			if err := r.watch(ctx, js); err != nil && ctx.Err() == nil {
				log.Printf("egress: watching the rates: %v", err)
			}
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
		}
	}()
	return r
}

func (r *Rates) watch(ctx context.Context, js jetstream.JetStream) error {
	kv, err := js.KeyValue(ctx, bus.BucketRates)
	if err != nil {
		return err
	}
	w, err := kv.WatchAll(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = w.Stop() }()
	for {
		select {
		case <-ctx.Done():
			return nil
		case u, ok := <-w.Updates():
			if !ok {
				return errors.New("the watch ended")
			}
			if u == nil {
				continue // the initial values are delivered
			}
			r.apply(u)
		}
	}
}

func (r *Rates) apply(u jetstream.KeyValueEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if u.Operation() != jetstream.KeyValuePut {
		delete(r.tables, u.Key())
		return
	}
	// The table arrives validated: the console checks it before writing it.
	t, err := ParseTable(u.Value())
	if err != nil {
		log.Printf("egress: the rate table %s: %v", u.Key(), err)
		delete(r.tables, u.Key())
		return
	}
	r.tables[u.Key()] = t
}

// Table returns the current table of name.
func (r *Rates) Table(name string) (*Table, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.tables[name]
	return t, ok
}

// ReadRate reads one table from the bucket, for an act that makes a single call.
func ReadRate(ctx context.Context, js jetstream.JetStream, name string) (*Table, bool, error) {
	kv, err := js.KeyValue(ctx, bus.BucketRates)
	if err != nil {
		return nil, false, err
	}
	u, err := kv.Get(ctx, name)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	t, err := ParseTable(u.Value())
	if err != nil {
		return nil, false, err
	}
	return t, true, nil
}
