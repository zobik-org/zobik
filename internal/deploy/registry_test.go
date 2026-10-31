package deploy

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"
)

func TestTag(t *testing.T) {
	d := "sha256:" + strings.Repeat("ab", 32)
	if !IsDigest(d) {
		t.Fatalf("%s is not a digest", d)
	}
	tag := Tag(TagEntry, d)
	// The distribution specification's tag grammar.
	if !regexp.MustCompile(`^[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}$`).MatchString(tag) {
		t.Errorf("%s is not a valid tag", tag)
	}
	if tag != "entry-sha256-"+d[len("sha256:"):] {
		t.Errorf("tag %s", tag)
	}
	for _, ref := range []string{"zobik-echo:dev", d[:len(d)-1], "sha512:" + d[7:]} {
		if IsDigest(ref) {
			t.Errorf("%s taken as a digest", ref)
		}
	}
}

// The retention keeps the base-* and entry-* tags without limit and a pending-*
// one for 72 hours (implementation §1.2.16).
func TestRegistryConfig(t *testing.T) {
	var c struct {
		Storage struct {
			RootDirectory string
			Retention     struct {
				Policies []struct {
					Repositories []string
					KeepTags     []struct {
						Patterns     []string
						PushedWithin string
					}
				}
			}
		}
		HTTP struct {
			Port string
			TLS  struct{ Cert, Key string }
		}
	}
	if err := json.Unmarshal(registryConfigJSON(), &c); err != nil {
		t.Fatal(err)
	}
	if c.HTTP.Port != RegistryPort || c.HTTP.TLS.Cert == "" || c.Storage.RootDirectory != registryDataDir {
		t.Errorf("config %+v", c)
	}
	p := c.Storage.Retention.Policies
	if len(p) != 1 || len(p[0].KeepTags) != 2 {
		t.Fatalf("retention %+v", p)
	}
	kept := map[string]string{}
	for _, k := range p[0].KeepTags {
		for _, pattern := range k.Patterns {
			kept[pattern] = k.PushedWithin
		}
	}
	want := map[string]string{"^base-": "", "^entry-": "", "^pending-": "72h"}
	for pattern, within := range want {
		if got, ok := kept[pattern]; !ok || got != within {
			t.Errorf("%s kept for %q, want %q", pattern, got, within)
		}
	}
}
