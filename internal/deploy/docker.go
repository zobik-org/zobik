package deploy

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/netip"
	"path"
	"strings"
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

// CheckOwner fails if the engine holds a resource of n's network that n's root
// did not create: zobik init does not converge over another root's network, and
// never removes it on its own. With no root yet, any resource is foreign.
func (e *Engine) CheckOwner(ctx context.Context, n names) error {
	foreign := func(labels map[string]string) bool {
		return n.root == "" || labels[labelRoot] != n.root
	}
	fail := func(what string) error {
		return fmt.Errorf("deploy: the network %q on this engine belongs to another root (%s): remove it or choose another network name", n.network, what)
	}
	if net, err := e.c.NetworkInspect(ctx, n.dockerNetwork(), client.NetworkInspectOptions{}); err == nil {
		if foreign(net.Network.Labels) {
			return fail("Docker network " + n.dockerNetwork())
		}
	} else if !cerrdefs.IsNotFound(err) {
		return err
	}
	byNetwork := client.Filters{}.Add("label", labelNetwork+"="+n.network)
	cs, err := e.c.ContainerList(ctx, client.ContainerListOptions{All: true, Filters: byNetwork})
	if err != nil {
		return err
	}
	for _, c := range cs.Items {
		if foreign(c.Labels) {
			return fail("container " + strings.TrimPrefix(c.Names[0], "/"))
		}
	}
	vs, err := e.c.VolumeList(ctx, client.VolumeListOptions{Filters: byNetwork})
	if err != nil {
		return err
	}
	for _, v := range vs.Items {
		if foreign(v.Labels) {
			return fail("volume " + v.Name)
		}
	}
	return nil
}

// File is a file copied into a container before it starts. Copying it through
// the engine needs no host path (implementation §1.2.4).
type File struct {
	Path string
	Mode int64
	Data []byte
}

// EnsureNetwork creates the network's Docker bridge network if it does not exist.
func (e *Engine) EnsureNetwork(ctx context.Context, name string, labels map[string]string) error {
	if _, err := e.c.NetworkInspect(ctx, name, client.NetworkInspectOptions{}); err == nil {
		return nil
	} else if !cerrdefs.IsNotFound(err) {
		return err
	}
	_, err := e.c.NetworkCreate(ctx, name, client.NetworkCreateOptions{
		Driver: "bridge",
		Labels: labels,
	})
	return err
}

// EnsureVolume creates a named volume if it does not exist, and says whether it
// created it.
func (e *Engine) EnsureVolume(ctx context.Context, name string, labels map[string]string) (bool, error) {
	if _, err := e.c.VolumeInspect(ctx, name, client.VolumeInspectOptions{}); err == nil {
		return false, nil
	} else if !cerrdefs.IsNotFound(err) {
		return false, err
	}
	_, err := e.c.VolumeCreate(ctx, client.VolumeCreateOptions{Name: name, Labels: labels})
	return err == nil, err
}

// WriteVolume copies files into a volume through a container of image that is
// created with the volume mounted at dir and never started. Each file's path is
// relative to dir.
func (e *Engine) WriteVolume(ctx context.Context, volume, image, dir string, files []File) error {
	res, err := e.c.ContainerCreate(ctx, client.ContainerCreateOptions{
		Config: &container.Config{Image: image},
		HostConfig: &container.HostConfig{
			Mounts: []mount.Mount{{Type: mount.TypeVolume, Source: volume, Target: dir}},
		},
	})
	if err != nil {
		return err
	}
	defer e.c.ContainerRemove(context.WithoutCancel(ctx), res.ID, client.ContainerRemoveOptions{Force: true})
	rooted := make([]File, len(files))
	for i, f := range files {
		rooted[i] = File{Path: path.Join(dir, f.Path), Mode: f.Mode, Data: f.Data}
	}
	archive, err := tarFiles(rooted)
	if err != nil {
		return err
	}
	_, err = e.c.CopyToContainer(ctx, res.ID, client.CopyToContainerOptions{DestinationPath: "/", Content: archive})
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
	Labels  map[string]string
	Image   string
	Cmd     []string
	Mounts  []mount.Mount
	Files   []File
	// Restart is unless-stopped for the long-lived components; an ephemeral one has none.
	Restart bool
	// Healthcheck reports whether a role is ready (implementation §6, The health of the roles).
	Healthcheck *container.HealthConfig
	// Publish maps a container port, like 7002/tcp, to a host port on
	// 127.0.0.1 (implementation §6, The off-Bus interfaces).
	Publish map[string]string
}

// bindings is s.Publish as the engine takes it.
func (s Spec) bindings() (network.PortSet, network.PortMap, error) {
	if len(s.Publish) == 0 {
		return nil, nil, nil
	}
	exposed, bound := network.PortSet{}, network.PortMap{}
	for in, host := range s.Publish {
		p, err := network.ParsePort(in)
		if err != nil {
			return nil, nil, err
		}
		exposed[p] = struct{}{}
		bound[p] = []network.PortBinding{{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: host}}
	}
	return exposed, bound, nil
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

// EnsureCurrent is EnsureRunning for a container whose image can change between
// runs of zobik init: one created from another image than the one s.Image names
// now is replaced.
func (e *Engine) EnsureCurrent(ctx context.Context, s Spec) error {
	info, err := e.c.ContainerInspect(ctx, s.Name, client.ContainerInspectOptions{})
	if err == nil {
		img, err := e.c.ImageInspect(ctx, s.Image)
		if err != nil {
			return err
		}
		_, want, err := s.bindings()
		if err != nil {
			return err
		}
		if info.Container.Image != img.ID || !samePorts(info.Container.HostConfig, want) {
			if _, err := e.c.ContainerRemove(ctx, s.Name, client.ContainerRemoveOptions{Force: true}); err != nil {
				return err
			}
		}
	} else if !cerrdefs.IsNotFound(err) {
		return err
	}
	return e.EnsureRunning(ctx, s)
}

// WaitHealthy waits until the container's health check reports it ready, and
// fails if the container stops or timeout passes.
func (e *Engine) WaitHealthy(ctx context.Context, name string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		info, err := e.c.ContainerInspect(ctx, name, client.ContainerInspectOptions{})
		if err != nil {
			return err
		}
		st := info.Container.State
		switch {
		case st == nil:
		case st.Health != nil && st.Health.Status == container.Healthy:
			return nil
		case !st.Running && !st.Restarting:
			out, _ := e.output(ctx, info.Container.ID)
			return fmt.Errorf("deploy: %s stopped with %d: %s", name, st.ExitCode, bytes.TrimSpace(out))
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("deploy: %s is not ready after %s", name, timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
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

// RunAttached runs a container and copies what it writes to out as it writes it,
// until the process exits or ctx ends; either way it removes the container.
func (e *Engine) RunAttached(ctx context.Context, s Spec, out io.Writer) error {
	id, err := e.create(ctx, s)
	if err != nil {
		return err
	}
	defer e.c.ContainerRemove(context.WithoutCancel(ctx), id, client.ContainerRemoveOptions{Force: true})
	if _, err := e.c.ContainerStart(ctx, id, client.ContainerStartOptions{}); err != nil {
		return err
	}
	logs, err := e.c.ContainerLogs(ctx, id, client.ContainerLogsOptions{ShowStdout: true, ShowStderr: true, Follow: true})
	if err != nil {
		return err
	}
	defer logs.Close()
	if _, err := stdcopy.StdCopy(out, out, logs); err != nil && ctx.Err() == nil {
		return err
	}
	if ctx.Err() != nil {
		return nil
	}
	info, err := e.c.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
	if err != nil {
		return err
	}
	if info.Container.State != nil && info.Container.State.ExitCode != 0 {
		return fmt.Errorf("deploy: %s exited with %d", s.Name, info.Container.State.ExitCode)
	}
	return nil
}

func (e *Engine) create(ctx context.Context, s Spec) (string, error) {
	exposed, bound, err := s.bindings()
	if err != nil {
		return "", err
	}
	host := &container.HostConfig{
		Mounts:       s.Mounts,
		PortBindings: bound,
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
			Image:        s.Image,
			Cmd:          s.Cmd,
			Labels:       s.Labels,
			Healthcheck:  s.Healthcheck,
			ExposedPorts: exposed,
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

// samePorts says whether a container publishes exactly the ports in want.
func samePorts(host *container.HostConfig, want network.PortMap) bool {
	var got network.PortMap
	if host != nil {
		got = host.PortBindings
	}
	if len(got) != len(want) {
		return false
	}
	for p, w := range want {
		g := got[p]
		if len(g) != 1 || g[0].HostPort != w[0].HostPort || g[0].HostIP != w[0].HostIP {
			return false
		}
	}
	return true
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
