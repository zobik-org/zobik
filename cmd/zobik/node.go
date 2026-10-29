//go:build dev

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"zobik.org/zobik/internal/deploy"
	"zobik.org/zobik/internal/role"
)

// zobik node up provisions a node with the act of provisioning, signed with the
// spawner role's material, while the network has no Spawner to do it. It runs
// as an ephemeral container that mounts the spawner role's volume and the
// container engine's socket.
func init() {
	devCommands["node"] = nodeCmd
}

const provisionPath = "/run/zobik/provision.json"

func nodeCmd(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] != "up" {
		return errors.New("usage: zobik node up --topic <topic> <blueprint.json>")
	}
	fs := flag.NewFlagSet("node up", flag.ContinueOnError)
	var n networkFlags
	n.register(fs)
	inside := fs.Bool("inside", false, "run inside the network (the ephemeral container does)")
	topic := fs.String("topic", "", "the topic the node claims")
	sidecarImage := fs.String("sidecar-image", "zobik-sidecar:dev", "the Integration Sidecar's image")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if *inside {
		return nodeUpInside(ctx)
	}
	if fs.NArg() != 1 {
		return errors.New("usage: zobik node up --topic <topic> <blueprint.json>")
	}
	if err := n.resolve(); err != nil {
		return err
	}
	raw, err := os.ReadFile(fs.Arg(0))
	if err != nil {
		return err
	}
	var b deploy.Blueprint
	if err := json.Unmarshal(raw, &b); err != nil {
		return fmt.Errorf("%s: %w", fs.Arg(0), err)
	}

	e, err := deploy.NewEngine(ctx)
	if err != nil {
		return err
	}
	defer e.Close()
	// Without a registry, an image the engine has stands for its digest.
	if !strings.HasPrefix(b.ArtifactRef, "sha256:") {
		img, err := e.InspectImage(ctx, b.ArtifactRef)
		if err != nil {
			return fmt.Errorf("artifact_ref: %w", err)
		}
		b.ArtifactRef = img.ID
	}
	req, err := json.Marshal(deploy.ProvisionRequest{Network: n.network, Topic: *topic, Blueprint: b, SidecarImage: *sidecarImage})
	if err != nil {
		return err
	}
	spec, err := deploy.SpawnerActSpec(n.dir, n.network, image, []string{"node", "up", "--inside"},
		deploy.File{Path: provisionPath, Mode: 0o600, Data: req})
	if err != nil {
		return err
	}
	out, err := e.RunEphemeral(ctx, spec)
	os.Stdout.Write(out)
	return err
}

func nodeUpInside(ctx context.Context) error {
	raw, err := os.ReadFile(provisionPath)
	if err != nil {
		return err
	}
	var req deploy.ProvisionRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	iss, nc, err := deploy.SpawnerIssuer(ctx, role.Dir)
	if err != nil {
		return err
	}
	defer nc.Close()
	defer iss.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	e, err := deploy.NewEngine(ctx)
	if err != nil {
		return err
	}
	defer e.Close()
	node, err := deploy.Provision(ctx, e, js, iss, req)
	if err != nil {
		return err
	}
	fmt.Printf("provisioned node %s on %s: %s and %s, similarity_floor %g, %d MB, model_id %s, egress_grants %v\n",
		node.ID, req.Topic, node.Logic, node.Sidecar, node.SimilarityFloor, node.MemoryRequiredMB, node.ModelID, node.EgressGrants)
	return nil
}
