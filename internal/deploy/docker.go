package deploy

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"time"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
)

// Engine is the container engine driver the console brings up the structural
// components with (implementation §6, The operator console).
type Engine struct {
	c *client.Client
}

// labelNetwork marks every resource of a network with its name.
const labelNetwork = "org.zobik.network"

// NewEngine connects to the engine named by the environment (DOCKER_HOST), or the default socket.
func NewEngine(ctx context.Context) (*Engine, error) {
	c, err := client.New(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, err
	}
	if _, err := c.Ping(ctx, client.PingOptions{}); err != nil {
		c.Close()
		return nil, fmt.Errorf("deploy: container engine unreachable: %w", err)
	}
	return &Engine{c: c}, nil
}

func (e *Engine) Close() error { return e.c.Close() }

// File is a file copied into a container before it starts. Copying it through
// the engine needs no host path (implementation §1.2.4).
type File struct {
	Path string
	Mode int64
	Data []byte
}

// EnsureNetwork creates the network's Docker bridge network if it does not exist.
func (e *Engine) EnsureNetwork(ctx context.Context, name, netName string) error {
	if _, err := e.c.NetworkInspect(ctx, name, client.NetworkInspectOptions{}); err == nil {
		return nil
	} else if !cerrdefs.IsNotFound(err) {
		return err
	}
	_, err := e.c.NetworkCreate(ctx, name, client.NetworkCreateOptions{
		Driver: "bridge",
		Labels: map[string]string{labelNetwork: netName},
	})
	return err
}

// EnsureVolume creates a named volume if it does not exist.
func (e *Engine) EnsureVolume(ctx context.Context, name, netName string) error {
	if _, err := e.c.VolumeInspect(ctx, name, client.VolumeInspectOptions{}); err == nil {
		return nil
	} else if !cerrdefs.IsNotFound(err) {
		return err
	}
	_, err := e.c.VolumeCreate(ctx, client.VolumeCreateOptions{Name: name, Labels: map[string]string{labelNetwork: netName}})
	return err
}

// EnsureImage pulls ref if the engine does not have it. A ref pinned by digest is
// verified by the engine on pull (implementation §1.2.1).
func (e *Engine) EnsureImage(ctx context.Context, ref string) error {
	if _, err := e.c.ImageInspect(ctx, ref); err == nil {
		return nil
	} else if !cerrdefs.IsNotFound(err) {
		return err
	}
	resp, err := e.c.ImagePull(ctx, ref, client.ImagePullOptions{})
	if err != nil {
		return err
	}
	defer resp.Close()
	return resp.Wait(ctx)
}

// Spec is a container of the network.
type Spec struct {
	Name    string
	Alias   string
	Network string // Docker network
	NetName string // Zobik network
	Image   string
	Cmd     []string
	Mounts  []mount.Mount
	Files   []File
	// Restart is unless-stopped for the long-lived components; an ephemeral one has none.
	Restart bool
}

// EnsureRunning creates and starts the container if it does not exist, and
// starts it if it exists stopped. An existing container is left as it is.
func (e *Engine) EnsureRunning(ctx context.Context, s Spec) error {
	info, err := e.c.ContainerInspect(ctx, s.Name, client.ContainerInspectOptions{})
	switch {
	case err == nil:
		if info.Container.State != nil && info.Container.State.Running {
			return nil
		}
		_, err = e.c.ContainerStart(ctx, s.Name, client.ContainerStartOptions{})
		return err
	case !cerrdefs.IsNotFound(err):
		return err
	}
	id, err := e.create(ctx, s)
	if err != nil {
		return err
	}
	_, err = e.c.ContainerStart(ctx, id, client.ContainerStartOptions{})
	return err
}

// RunEphemeral runs a container to completion, returns what it wrote and
// removes it. It fails if the process exits with a status other than zero.
func (e *Engine) RunEphemeral(ctx context.Context, s Spec) ([]byte, error) {
	// One left behind by an interrupted run would hold the name.
	if _, err := e.c.ContainerRemove(ctx, s.Name, client.ContainerRemoveOptions{Force: true}); err != nil && !cerrdefs.IsNotFound(err) {
		return nil, err
	}
	id, err := e.create(ctx, s)
	if err != nil {
		return nil, err
	}
	defer e.c.ContainerRemove(context.WithoutCancel(ctx), id, client.ContainerRemoveOptions{Force: true})

	wait := e.c.ContainerWait(ctx, id, client.ContainerWaitOptions{Condition: container.WaitConditionNextExit})
	if _, err := e.c.ContainerStart(ctx, id, client.ContainerStartOptions{}); err != nil {
		return nil, err
	}
	var status int64
	select {
	case r := <-wait.Result:
		status = r.StatusCode
	case err := <-wait.Error:
		return nil, err
	}
	out, err := e.output(ctx, id)
	if err != nil {
		return nil, err
	}
	if status != 0 {
		return out, fmt.Errorf("deploy: %s exited with %d: %s", s.Name, status, bytes.TrimSpace(out))
	}
	return out, nil
}

func (e *Engine) create(ctx context.Context, s Spec) (string, error) {
	host := &container.HostConfig{
		Mounts: s.Mounts,
		// implementation §6, The off-Bus interfaces: no container of the network has NET_RAW.
		CapDrop: []string{"NET_RAW"},
	}
	if s.Restart {
		host.RestartPolicy = container.RestartPolicy{Name: container.RestartPolicyUnlessStopped}
	}
	endpoint := &network.EndpointSettings{}
	if s.Alias != "" {
		endpoint.Aliases = []string{s.Alias}
	}
	res, err := e.c.ContainerCreate(ctx, client.ContainerCreateOptions{
		Name: s.Name,
		Config: &container.Config{
			Image:  s.Image,
			Cmd:    s.Cmd,
			Labels: map[string]string{labelNetwork: s.NetName},
		},
		HostConfig:       host,
		NetworkingConfig: &network.NetworkingConfig{EndpointsConfig: map[string]*network.EndpointSettings{s.Network: endpoint}},
	})
	if err != nil {
		return "", err
	}
	if len(s.Files) > 0 {
		archive, err := tarFiles(s.Files)
		if err != nil {
			return "", err
		}
		if _, err := e.c.CopyToContainer(ctx, res.ID, client.CopyToContainerOptions{DestinationPath: "/", Content: archive}); err != nil {
			e.c.ContainerRemove(context.WithoutCancel(ctx), res.ID, client.ContainerRemoveOptions{Force: true})
			return "", err
		}
	}
	return res.ID, nil
}

func (e *Engine) output(ctx context.Context, id string) ([]byte, error) {
	logs, err := e.c.ContainerLogs(ctx, id, client.ContainerLogsOptions{ShowStdout: true, ShowStderr: true})
	if err != nil {
		return nil, err
	}
	defer logs.Close()
	var out bytes.Buffer
	if _, err := stdcopy.StdCopy(&out, &out, logs); err != nil && err != io.EOF {
		return nil, err
	}
	return out.Bytes(), nil
}

// tarFiles builds the archive CopyToContainer extracts at /, with the parent
// directories of each file.
func tarFiles(files []File) (io.Reader, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	dirs := map[string]bool{}
	now := time.Now()
	for _, f := range files {
		for d := path.Dir(f.Path); d != "/" && d != "." && !dirs[d]; d = path.Dir(d) {
			dirs[d] = true
		}
	}
	for d := range dirs {
		if err := tw.WriteHeader(&tar.Header{Typeflag: tar.TypeDir, Name: d[1:] + "/", Mode: 0o755, ModTime: now}); err != nil {
			return nil, err
		}
	}
	for _, f := range files {
		if err := tw.WriteHeader(&tar.Header{Typeflag: tar.TypeReg, Name: f.Path[1:], Mode: f.Mode, Size: int64(len(f.Data)), ModTime: now}); err != nil {
			return nil, err
		}
		if _, err := tw.Write(f.Data); err != nil {
			return nil, err
		}
	}
	return &buf, tw.Close()
}
