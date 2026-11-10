//go:build e2e

package deploy

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"

	"zobik.org/zobik/internal/role/dockerreader"
)

// A docker_reader does not return another network's containers, even to a
// request that asks for them by label; the census through it lists only its
// network's fleet (implementation §6, The roles of the binary).
func TestDockerReaderNetwork(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e, err := NewEngine(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if err := e.EnsureImage(ctx, testBase); err != nil {
		t.Fatal(err)
	}
	mine, theirs := "dr-"+uuid.NewString()[:8], "dr-"+uuid.NewString()[:8]
	for _, nw := range []string{mine, theirs} {
		c, err := e.c.ContainerCreate(ctx, client.ContainerCreateOptions{Config: &container.Config{
			Image: testBase,
			Labels: map[string]string{
				labelNetwork: nw, labelNode: "U" + nw, labelTopic: "echo", labelModelID: "m", labelSignerID: "S",
				labelSimilarityFloor: "0.6", labelEgressGrants: "[]", labelIngressGrants: "[]", labelMemory: "64",
			},
		}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { e.discard(context.Background(), c.ID) })
	}

	dir, err := os.MkdirTemp("", "dr")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "docker.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: dockerreader.Handler(mine, dockerreader.Engine(EngineSocket))}
	go srv.Serve(ln)
	defer srv.Close()

	r, err := NewEngineAt(ctx, socket)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	nodes, err := r.Census(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 || nodes[0].NodeID != "U"+mine || nodes[0].SimilarityFloor != 0.6 || nodes[0].MemoryRequired != 64 {
		t.Errorf("census %+v", nodes)
	}

	// Asking for the other network by label, in either form, returns nothing.
	hc := &http.Client{Transport: dockerreader.Engine(socket)}
	for _, filters := range []string{
		`{"label":["` + labelNetwork + "=" + theirs + `"]}`,
		`{"label":{"` + labelNetwork + "=" + theirs + `":true}}`,
	} {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://docker/containers/json?all=1&filters="+url.QueryEscape(filters), nil)
		resp, err := hc.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var cs []json.RawMessage
		if resp.StatusCode != http.StatusOK || json.Unmarshal(body, &cs) != nil || len(cs) != 0 {
			t.Errorf("%s: %s %s", filters, resp.Status, body)
		}
	}
	// A read of a container's content does not pass.
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://docker/containers/x/json", nil)
	resp, err := hc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("inspect: %s", resp.Status)
	}
}
