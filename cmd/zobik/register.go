//go:build dev

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"zobik.org/zobik/internal/deploy"
	"zobik.org/zobik/internal/role/catalog"
)

// zobik register executes the act of registration of a blueprint, as the
// deployment's seeding does, while the console has no form for it: it pushes
// the image the engine has and registers it with the network's register
// credential, with provenance seeded or, with --provenance imported, as an
// entry that purge can delete (architecture §3.13.8). An emergent topic the
// Topic Registry does not have needs --topic-description (architecture §3.7.6).
func init() {
	devCommands["register"] = registerCmd
}

func registerCmd(ctx context.Context, args []string) error {
	const usage = "usage: zobik register --topic <topic> [--topic-description <text>] [--provenance seeded|imported] <blueprint.json>"
	fs := flag.NewFlagSet("register", flag.ContinueOnError)
	var n networkFlags
	n.register(fs)
	topic := fs.String("topic", "", "the topic the blueprint claims")
	description := fs.String("topic-description", "", "the description of the topic, which registers it if the Topic Registry does not have it")
	author := fs.String("author", "seeding", "the author_id, when the blueprint does not carry one")
	provenance := fs.String("provenance", catalog.ProvenanceSeeded, "the entry's provenance in the network: seeded, or imported")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 || *topic == "" || (*provenance != catalog.ProvenanceSeeded && *provenance != catalog.ProvenanceImported) {
		return errors.New(usage)
	}
	if err := n.resolve(); err != nil {
		return err
	}
	raw, err := os.ReadFile(fs.Arg(0))
	if err != nil {
		return err
	}
	var reg catalog.Registration
	if err := json.Unmarshal(raw, &reg); err != nil {
		return fmt.Errorf("%s: %w", fs.Arg(0), err)
	}
	reg.Topic = *topic
	if *description != "" {
		reg.TopicDescription = *description
	}
	if reg.AuthorID == "" {
		reg.AuthorID = *author
	}
	reg.Acquisition = &catalog.Acquisition{Provenance: *provenance, AsOf: time.Now().UTC()}

	if !deploy.IsDigest(reg.ArtifactRef) {
		e, err := deploy.NewEngine(ctx)
		if err != nil {
			return err
		}
		defer e.Close()
		// The registration puts the tag that retains it (implementation §1.2.16).
		if reg.ArtifactRef, err = deploy.PushImage(ctx, e, n.dir, n.network, image, reg.ArtifactRef, deploy.TagPending); err != nil {
			return fmt.Errorf("artifact_ref: %w", err)
		}
	}
	cred, err := deploy.ReadSecret(n.dir, deploy.HandleCatalogRegister)
	if err != nil {
		return err
	}
	if cred == nil {
		return errors.New("the network has no Catalog credential: only the network that created its tenant receives them")
	}
	c, err := deploy.CatalogClient(n.dir, string(cred))
	if err != nil {
		return err
	}
	entry, err := c.Register(ctx, reg)
	if err != nil {
		return err
	}
	fmt.Println(string(entry))
	return nil
}
