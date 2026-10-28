package deploy

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/nats-io/nkeys"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/role"
	"zobik.org/zobik/internal/signing"
)

// The structural roles' material (implementation §6, The root and the split
// Operator Channel). Each role keeps its key and what the root signed for it in
// a volume of its own, mounted at role.Dir:
//
//	identity.nk           the platform identity's seed
//	bus.jwt               its user JWT, which the console renews
//	root.pub              the root's public key, to verify against (thesis 9)
//	credential.jws        a structural node's capability credential
//	role_certificate.jws  the role certificate of a role that signs
//	signing_key.nk        the scoped signing key of a role that issues identities
//
// The console keeps only each identity's public key, in identities/<unit>.pub.
const (
	dirIdentities = "identities"

	// roleIdentityLifetime is the year a structural component's platform identity
	// is valid for (implementation §6, The root and the split Operator Channel).
	roleIdentityLifetime = 365 * 24 * time.Hour
)

// Role is a structural role as zobik init provisions it.
type Role struct {
	// Unit names its container and its volume: zobik-<network>-<unit>.
	Unit  string
	Scope bus.Scope
	// Topic is what a structural node claims; its capability credential names it.
	Topic         string
	AttemptWindow *signing.AttemptWindow
	// Certificate is the role a role_certificate accredits it as, for a role that signs.
	Certificate string
	// Consumers are the durable consumers it pulls from, named by its identity
	// (implementation §1.2.1).
	Consumers func(id string) []bus.Consumer
	// Args are its invocation after zobik run.
	Args []string
	// NamesNetwork adds the network's name to Args, as --network.
	NamesNetwork bool
	// WaitsFor are the units it waits for besides the NATS server
	// (implementation §6, The roles of the binary).
	WaitsFor []string
	// Data gives it a second volume, <unit>_data, mounted at role.DataDir.
	Data bool
	// ServesConsole publishes its service toward the console on a port of the
	// host's 127.0.0.1, and gives it the console's identity, the only one it
	// admits (implementation §6, The service between the console and the channels).
	ServesConsole bool
	// Issues is the scope whose signing key the role keeps in its volume, to
	// issue identities without the console. Minting the role rotates the key, so
	// the identities a lost volume's key issued stop being admitted.
	Issues *bus.Scope
	// Unstarted roles receive their volume and their material, and no container:
	// their acts run in an ephemeral container that mounts the volume.
	Unstarted bool
}

// The defaults of the deployment parameters the context role enforces
// (implementation §6, The deployment parameters).
const (
	contextStoreSize = 4 << 30
	contextTraceSize = 1 << 30
)

// Roles are the structural roles this version brings up.
var Roles = []Role{
	{Unit: "config", Scope: bus.ConfigScope, Topic: "config_change", Consumers: claimant("config_change"),
		Args: []string{"--role=config"}},
	// The Task Broker receives the entire family: announcements and closes, and
	// every proposal (implementation §1.2.1).
	{Unit: "task_broker", Scope: bus.TaskBrokerScope, Certificate: signing.RoleTaskBroker,
		Args: []string{"--role=task_broker"}, WaitsFor: []string{"config"},
		Consumers: func(id string) []bus.Consumer {
			return []bus.Consumer{
				{Stream: "task", Name: id, Filters: []string{"task.announced.>", "task.completed.>", "task.failed.>", "task.aborted.>"}},
				{Stream: "prop", Name: id, Filters: []string{"prop.>"}},
			}
		}},
	// The Context Store observes the terminal events to purge on trace close (implementation §1.2.9).
	{Unit: "context", Scope: bus.ContextScope,
		Args:     []string{"--role=context", fmt.Sprintf("--store-size=%d", contextStoreSize), fmt.Sprintf("--trace-size=%d", contextTraceSize)},
		WaitsFor: []string{"config"}, Data: true,
		Consumers: func(id string) []bus.Consumer {
			return []bus.Consumer{{Stream: "task", Name: id, Filters: []string{"task.completed.>", "task.failed.>", "task.aborted.>"}}}
		}},
	// The Tracing Collector observes every event that carries traceparent, on
	// the three families, and keeps the live traces in its data volume
	// (architecture §3.2, implementation §1.2.6). Its consumer of the task
	// family also brings it the cycle of trace_query (architecture §3.10).
	{Unit: "tracing_collector", Scope: bus.TracingCollectorScope, Topic: "trace_query",
		Args: []string{"--role=tracing_collector"}, NamesNetwork: true, WaitsFor: []string{"config"}, Data: true,
		Consumers: func(id string) []bus.Consumer {
			return []bus.Consumer{
				{Stream: "task", Name: id, Filters: []string{"task.>"}},
				{Stream: "prop", Name: id, Filters: []string{"prop.>"}},
				{Stream: "notice", Name: id, Filters: []string{"notice.>"}},
			}
		}},
	// The Spawner's container comes with its decisions; until then the act of
	// provisioning runs with its material (Provision).
	{Unit: "spawner", Scope: bus.SpawnerScope, Certificate: signing.RoleSpawner, Issues: &bus.NodeScope, Unstarted: true},
	// The Secret Store's resolver, with the store and its key in its volume
	// (implementation §1.2.18). It uses the Bus only to read the configuration.
	{Unit: unitSecrets, Scope: bus.SecretsScope, Args: []string{"--role=secrets"}},
	// The panel's channels go first and last in their audience (implementation §6,
	// The operator console). The Entry Node resolves the embedding model's
	// credential (implementation §1.2.15).
	panelChannel("user", "config", unitSecrets),
	panelChannel("operator", "config"),
}

// panelChannel is the channel role of one of the panel's audiences. A channel
// also receives the closes of the tasks it opens. Its window has no upper limit
// until the Spawner closes the vacant rounds: without that close no query
// reaches the final round, and one let go in round 1 would stay vacant.
func panelChannel(audience string, waitsFor ...string) Role {
	topic := signing.HITLPrefix + audience
	return Role{Unit: "channel_" + audience, Scope: bus.ChannelScope, Topic: topic,
		AttemptWindow: &signing.AttemptWindow{From: ptr(1), FinalRound: ptr(true)},
		Args:          []string{"--role=channel", "--audience=" + audience}, WaitsFor: []string{"config"},
		ServesConsole: true,
		Consumers: func(id string) []bus.Consumer {
			c := claimant(topic)(id)
			c[0].Filters = append(c[0].Filters, "task.completed."+id+".>", "task.failed."+id+".>", "task.aborted."+id+".>")
			return c
		}}
}

// claimant listens to a topic's announcements and to the award and rejection
// addressed to its identity.
func claimant(topic string) func(id string) []bus.Consumer {
	return func(id string) []bus.Consumer {
		return []bus.Consumer{{Stream: "task", Name: id, Filters: []string{
			bus.SubjectAnnounced(topic),
			bus.SubjectAddressed(bus.TypeAssigned, id),
			bus.SubjectAddressed(bus.TypeRejected, id),
		}}}
	}
}

func ptr[T any](v T) *T { return &v }

// ensureRoles gives each role its volume and its material. A role that already
// has its identity keeps it and only receives a renewed JWT; one whose volume is
// new receives a new identity, and the one it replaces is revoked in the account.
func ensureRoles(ctx context.Context, e *Engine, n names, m *material, dir, image string) (map[string]string, error) {
	if err := os.MkdirAll(filepath.Join(dir, dirIdentities), 0o700); err != nil {
		return nil, err
	}
	ids := map[string]string{}
	files := map[string][]File{}
	var revoked []string
	account := m.account.JWT
	for _, r := range Roles {
		created, err := e.EnsureVolume(ctx, n.volume(r.Unit), n.labels())
		if err != nil {
			return nil, fmt.Errorf("deploy: %s volume: %w", r.Unit, err)
		}
		if r.Data {
			if _, err := e.EnsureVolume(ctx, n.volume(r.Unit+"_data"), n.labels()); err != nil {
				return nil, fmt.Errorf("deploy: %s data volume: %w", r.Unit, err)
			}
		}
		old, err := os.ReadFile(filepath.Join(dir, dirIdentities, r.Unit+".pub"))
		hasOld := err == nil
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}
		if hasOld && !created {
			pub := string(old)
			jwtFile, err := m.roleJWT(r, pub)
			if err != nil {
				return nil, err
			}
			ids[r.Unit], files[r.Unit] = pub, []File{jwtFile}
			continue
		}
		if hasOld {
			revoked = append(revoked, string(old))
		}
		if ids[r.Unit], files[r.Unit], err = m.mintRole(r, image); err != nil {
			return nil, fmt.Errorf("deploy: %s: %w", r.Unit, err)
		}
	}
	if len(revoked) > 0 {
		updated, err := bus.Revoke(m.root, m.account.JWT, revoked...)
		if err != nil {
			return nil, err
		}
		m.account.JWT = updated
	}
	if m.account.JWT != account {
		if err := writeFile(dir, fileAccount, []byte(m.account.JWT)); err != nil {
			return nil, err
		}
	}
	// Every role receives the account as it ends up, rotations and revocations included.
	accountFile := File{Path: role.FileAccount, Mode: 0o644, Data: []byte(m.account.JWT)}
	consolePub, err := m.console.PublicKey()
	if err != nil {
		return nil, err
	}
	for _, r := range Roles {
		write := append(files[r.Unit], accountFile)
		if r.ServesConsole {
			write = append(write, File{Path: role.FileConsolePub, Mode: 0o644, Data: []byte(consolePub)})
		}
		if r.Unit == unitSecrets {
			store, err := secretsFiles(dir, ids["channel_user"])
			if err != nil {
				return nil, err
			}
			write = append(write, store...)
		}
		if err := e.WriteVolume(ctx, n.volume(r.Unit), image, role.Dir, write); err != nil {
			return nil, fmt.Errorf("deploy: writing %s's material: %w", r.Unit, err)
		}
		if err := writeFile(dir, filepath.Join(dirIdentities, r.Unit+".pub"), []byte(ids[r.Unit])); err != nil {
			return nil, err
		}
	}
	return ids, nil
}

// mintRole mints a role's platform identity and what the root signs for it.
func (m *material) mintRole(r Role, image string) (string, []File, error) {
	user, err := nkeys.CreateUser()
	if err != nil {
		return "", nil, err
	}
	pub, err := user.PublicKey()
	if err != nil {
		return "", nil, err
	}
	seed, err := user.Seed()
	if err != nil {
		return "", nil, err
	}
	jwtFile, err := m.roleJWT(r, pub)
	if err != nil {
		return "", nil, err
	}
	rootPub, err := m.root.PublicKey()
	if err != nil {
		return "", nil, err
	}
	files := []File{
		{Path: role.FileIdentity, Mode: 0o600, Data: seed},
		jwtFile,
		{Path: role.FileRootPub, Mode: 0o644, Data: []byte(rootPub)},
	}
	now := time.Now().Unix()
	if r.Topic != "" {
		// A structural credential carries neither model_id nor capability_embedding (implementation §9.2).
		cred, err := signing.Sign(m.root, signing.TypeCapability, signing.Capability{
			Sub: pub, Iat: now, Topic: r.Topic, ArtifactRef: image, AttemptWindow: r.AttemptWindow,
		}, "")
		if err != nil {
			return "", nil, err
		}
		files = append(files, File{Path: role.FileCredential, Mode: 0o644, Data: []byte(cred)})
	}
	if r.Certificate != "" {
		cert, err := signing.Sign(m.root, signing.TypeRoleCertificate, signing.RoleCertificate{
			Sub: pub, Iat: now, Role: r.Certificate,
		}, "")
		if err != nil {
			return "", nil, err
		}
		files = append(files, File{Path: role.FileRoleCertificate, Mode: 0o644, Data: []byte(cert)})
	}
	if r.Issues != nil {
		updated, key, err := bus.RotateScope(m.root, m.account.JWT, *r.Issues)
		if err != nil {
			return "", nil, err
		}
		seed, err := key.Seed()
		if err != nil {
			return "", nil, err
		}
		m.account.JWT = updated
		files = append(files, File{Path: role.FileSigningKey, Mode: 0o600, Data: seed})
	}
	return pub, files, nil
}

// roleJWT issues the role's user JWT for a year under its scope's signing key.
func (m *material) roleJWT(r Role, pub string) (File, error) {
	key, ok := m.account.SigningKeys[r.Scope.Role]
	if !ok {
		return File{}, fmt.Errorf("deploy: the account has no %s scope", r.Scope.Role)
	}
	token, err := bus.IssueUser(key, m.account.Public, pub, r.Unit, time.Now().Add(roleIdentityLifetime))
	if err != nil {
		return File{}, err
	}
	return File{Path: role.FileBusJWT, Mode: 0o600, Data: []byte(token)}, nil
}

// Identities reads the roles' public keys, by unit, from the console's directory.
func Identities(dir string) (map[string]string, error) {
	ids := map[string]string{}
	for _, r := range Roles {
		pub, err := os.ReadFile(filepath.Join(dir, dirIdentities, r.Unit+".pub"))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		ids[r.Unit] = string(pub)
	}
	return ids, nil
}

// roleReadyTimeout bounds how long zobik init waits for a role to be ready.
const roleReadyTimeout = 2 * time.Minute

// roleHealthcheck runs zobik health against the role's own socket: every second
// while it starts, then every ten (implementation §6, The health of the roles).
var roleHealthcheck = &container.HealthConfig{
	Test:          []string{"CMD", "/zobik", "health"},
	Interval:      10 * time.Second,
	Timeout:       3 * time.Second,
	Retries:       3,
	StartPeriod:   time.Minute,
	StartInterval: time.Second,
}

// roleSpec is a role's container: the roles' image with another invocation, its
// volume, its alias on the network's Docker network and the unless-stopped
// restart (implementation §6, The roles of the binary).
func roleSpec(n names, r Role, image, port string) Spec {
	mounts := []mount.Mount{{Type: mount.TypeVolume, Source: n.volume(r.Unit), Target: role.Dir}}
	var publish map[string]string
	if port != "" {
		publish = map[string]string{role.PortChannel + "/tcp": port}
	}
	if r.Data {
		mounts = append(mounts, mount.Mount{Type: mount.TypeVolume, Source: n.volume(r.Unit + "_data"), Target: role.DataDir})
	}
	cmd := append([]string{"run"}, r.Args...)
	if r.NamesNetwork {
		cmd = append(cmd, "--network="+n.network)
	}
	return Spec{
		Name:        n.container(r.Unit),
		Alias:       r.Unit,
		Network:     n.dockerNetwork(),
		Labels:      n.labels(),
		Image:       image,
		Cmd:         cmd,
		Mounts:      mounts,
		Restart:     true,
		Healthcheck: roleHealthcheck,
		Publish:     publish,
	}
}

// startRoles brings up each role once what it waits for is ready, and returns
// when all of them are.
func startRoles(ctx context.Context, e *Engine, n names, dir, image string) error {
	for _, r := range Roles {
		if r.Unstarted {
			continue
		}
		for _, dep := range r.WaitsFor {
			if err := e.WaitHealthy(ctx, n.container(dep), roleReadyTimeout); err != nil {
				return err
			}
		}
		var port string
		if r.ServesConsole {
			var err error
			if port, err = ConsolePort(dir, r.Unit); err != nil {
				return err
			}
		}
		if err := e.EnsureCurrent(ctx, roleSpec(n, r, image, port)); err != nil {
			return fmt.Errorf("deploy: %s: %w", r.Unit, err)
		}
	}
	for _, r := range Roles {
		if r.Unstarted {
			continue
		}
		if err := e.WaitHealthy(ctx, n.container(r.Unit), roleReadyTimeout); err != nil {
			return err
		}
	}
	return nil
}

// dirPorts keeps, in the console's directory, the host port of each role that
// serves the console: zobik init chooses a free one once, and it stays fixed
// (implementation §6, The deployment parameters).
const dirPorts = "ports"

// ConsolePort is the host port on 127.0.0.1 where unit serves the console.
func ConsolePort(dir, unit string) (string, error) {
	path := filepath.Join(dir, dirPorts, unit)
	b, err := os.ReadFile(path)
	if err == nil {
		return strings.TrimSpace(string(b)), nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return "", err
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	port := strconv.Itoa(ln.Addr().(*net.TCPAddr).Port)
	ln.Close()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", err
	}
	return port, os.WriteFile(path, []byte(port), 0o600)
}

// unitPanel names, among the ports, the one where the console serves the panel
// and its API (implementation §6, The deployment parameters).
const unitPanel = "panel"

// PanelPort is the host port on 127.0.0.1 where the console serves the panel:
// the first zobik init chooses it, and it stays fixed.
func PanelPort(dir string) (string, error) { return ConsolePort(dir, unitPanel) }

// unitTraceUI names, among the ports, the one where the console serves the
// tracing backend's UI (implementation §1.2.17).
const unitTraceUI = "trace_ui"

// TraceUIPort is the host port on 127.0.0.1 where the console serves the
// tracing backend's UI: the first console that serves it chooses it, and it
// stays fixed.
func TraceUIPort(dir string) (string, error) { return ConsolePort(dir, unitTraceUI) }

// ErrNoPanel is a directory where zobik init has not chosen the panel's port.
var ErrNoPanel = errors.New("deploy: no network here: run zobik init")

// KnownPanelPort is the panel's port zobik init chose, without choosing one.
func KnownPanelPort(dir string) (string, error) {
	b, err := os.ReadFile(filepath.Join(dir, dirPorts, unitPanel))
	if errors.Is(err, fs.ErrNotExist) {
		return "", ErrNoPanel
	}
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}
