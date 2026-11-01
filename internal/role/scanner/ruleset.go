package scanner

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"

	v1 "github.com/google/go-containerregistry/pkg/v1"
)

// TrivyVersion is the Trivy the role image brings, whose built-in rules come
// compiled into the binary (implementation §1.2.19). It is the version
// images/zobik.Dockerfile copies, and the role refuses to serve with another.
const TrivyVersion = "0.76.1"

// patterns are the own content patterns, in Trivy's secret configuration.
//
//go:embed patterns.yaml
var patterns []byte

// A Check is a structural check: a rule over the manifest, the configuration
// and the metadata of the layers' files, for what a pattern over text does not
// express (implementation §1.2.19).
type Check struct {
	// ID is what findings carries when an image breaks it; it starts with zobik-.
	ID string
	// Revision changes whenever what the check finds changes, so that the
	// ruleset_version changes with it.
	Revision int
	// Breaks says whether img breaks the check.
	Breaks func(ctx context.Context, img v1.Image) (bool, error)
}

// Checks are the structural checks the analysis applies. A new one is added
// here with its test image.
var Checks []Check

// RulesetVersion is the SHA-256 of the ruleset: the Trivy version, the own
// patterns and the identifier and revision of each structural check
// (implementation §1.2.19). The catalog role reports it as the current one.
func RulesetVersion() string { return rulesetVersion(Checks) }

func rulesetVersion(checks []Check) string {
	type check struct {
		ID       string `json:"id"`
		Revision int    `json:"revision"`
	}
	ruleset := struct {
		Trivy    string  `json:"trivy"`
		Patterns string  `json:"patterns"`
		Checks   []check `json:"checks"`
	}{Trivy: TrivyVersion, Patterns: string(patterns), Checks: []check{}}
	for _, c := range checks {
		ruleset.Checks = append(ruleset.Checks, check{c.ID, c.Revision})
	}
	b, err := json.Marshal(ruleset)
	if err != nil {
		panic(err) // strings and integers always marshal
	}
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}
