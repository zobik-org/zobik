package deploy

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"

	"zobik.org/zobik/internal/role/dockerreader"
	"zobik.org/zobik/internal/role/networkmonitor"
)

// unitDockerReader is the docker_reader role's unit: it runs without the Bus,
// so it has no identity and zobik init brings it up before the roles that read
// through it (implementation §6, The roles of the binary).
const unitDockerReader = "docker_reader"

// readerSocket is the volume of docker_reader's socket, which only the roles
// that read through it mount.
func (n names) readerSocket() string { return n.volume(unitDockerReader + "_socket") }

// ensureDockerReader brings up the network's docker_reader with the engine's
// socket and the volume of its own.
func ensureDockerReader(ctx context.Context, e *Engine, n names, image string) error {
	if _, err := e.EnsureVolume(ctx, n.readerSocket(), n.labels()); err != nil {
		return fmt.Errorf("deploy: volume %s: %w", n.readerSocket(), err)
	}
	err := e.EnsureCurrent(ctx, Spec{
		Name:    n.container(unitDockerReader),
		Network: n.dockerNetwork(),
		Labels:  n.labels(),
		Image:   image,
		Cmd:     []string{"run", "--role=" + unitDockerReader, "--network=" + n.network},
		Mounts: []mount.Mount{
			{Type: mount.TypeBind, Source: EngineSocket, Target: EngineSocket},
			{Type: mount.TypeVolume, Source: n.readerSocket(), Target: dockerreader.SocketDir},
		},
		Restart:     true,
		Healthcheck: roleHealthcheck,
	})
	if err != nil {
		return fmt.Errorf("deploy: %s: %w", unitDockerReader, err)
	}
	return nil
}

// readerMount is the socket volume of docker_reader, mounted where a role
// that reads through it finds the socket.
func (n names) readerMount() mount.Mount {
	return mount.Mount{Type: mount.TypeVolume, Source: n.readerSocket(), Target: dockerreader.SocketDir}
}

// NewEngineAt reaches an engine API served on the Unix socket at path: what a
// role that reads through docker_reader uses.
func NewEngineAt(ctx context.Context, path string) (*Engine, error) {
	c, err := client.New(client.WithHost("unix://" + path))
	if err != nil {
		return nil, err
	}
	if _, err := c.Ping(ctx, client.PingOptions{}); err != nil {
		c.Close()
		return nil, fmt.Errorf("deploy: %s unreachable: %w", path, err)
	}
	return &Engine{c: c}, nil
}

// Census reads the Active Node Registry from the labels of the Logic
// Containers, running or stopped: the fleet the Spawner provisioned
// (architecture §3.11, §3.12.3, implementation §1.2.14). Through docker_reader
// it sees only its network's.
func (e *Engine) Census(ctx context.Context) ([]networkmonitor.Node, error) {
	cs, err := e.Containers(ctx, map[string]string{labelTopic: ""})
	if err != nil {
		return nil, err
	}
	out := make([]networkmonitor.Node, 0, len(cs))
	for _, c := range cs {
		// The engine runs the artifact by digest (implementation §1.2.16).
		_, digest, ok := strings.Cut(c.Image, "@")
		if !ok {
			continue
		}
		floor, err := strconv.ParseFloat(c.Labels[labelSimilarityFloor], 64)
		if err != nil {
			return nil, fmt.Errorf("deploy: %s carries no %s: %w", c.Name, labelSimilarityFloor, err)
		}
		memory, err := strconv.ParseInt(c.Labels[labelMemory], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("deploy: %s carries no %s: %w", c.Name, labelMemory, err)
		}
		node := networkmonitor.Node{
			NodeID: c.Labels[labelNode], ArtifactRef: digest, Topic: c.Labels[labelTopic],
			ModelID: c.Labels[labelModelID], SignerID: c.Labels[labelSignerID], SimilarityFloor: floor,
			EgressGrants:   json.RawMessage(c.Labels[labelEgressGrants]),
			IngressGrants:  json.RawMessage(c.Labels[labelIngressGrants]),
			MemoryRequired: memory,
		}
		for label, g := range map[string]json.RawMessage{labelEgressGrants: node.EgressGrants, labelIngressGrants: node.IngressGrants} {
			if !json.Valid(g) {
				return nil, fmt.Errorf("deploy: %s carries a %s that is not JSON", c.Name, label)
			}
		}
		out = append(out, node)
	}
	return out, nil
}
