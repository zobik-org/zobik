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
	// WaitsFor are the units it waits for besides the NATS server
	// (implementation §6, The roles of the binary).
	WaitsFor []string
	// Data gives it a second volume, <unit>_data, mounted at role.DataDir.
	Data bool
	// ServesConsole publishes its service toward the console on a port of the
	// host's 127.0.0.1, and gives it the console's identity, the only one it
	// admits (implementation §6, The service between the console and the channels).
	ServesConsole bool
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
	// The panel's channels go first and last in their audience (implementation §6,
	// The operator console). A channel also receives the closes of the tasks it opens.
	{Unit: "channel_operator", Scope: bus.ChannelScope, Topic: "hitl_contact_operator",
		AttemptWindow: &signing.AttemptWindow{From: ptr(1), To: ptr(1), FinalRound: ptr(true)},
		Args:          []string{"--role=channel", "--audience=operator"}, WaitsFor: []string{"config"},
		ServesConsole: true,
		Consumers: func(id string) []bus.Consumer {
			c := claimant("hitl_contact_operator")(id)
			c[0].Filters = append(c[0].Filters, "task.completed."+id+".>", "task.failed."+id+".>", "task.aborted."+id+".>")
			return c
		}},
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
		if err := writeFile(dir, fileAccount, []byte(updated)); err != nil {
			return nil, err
		}
	}
	// Every role receives the account as it ends up, revocations included.
	account := File{Path: role.FileAccount, Mode: 0o644, Data: []byte(m.account.JWT)}
	consolePub, err := m.console.PublicKey()
	if err != nil {
		return nil, err
	}
	for _, r := range Roles {
		write := append(files[r.Unit], account)
		if r.ServesConsole {
			write = append(write, File{Path: role.FileConsolePub, Mode: 0o644, Data: []byte(consolePub)})
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
	return Spec{
		Name:        n.container(r.Unit),
		Alias:       r.Unit,
		Network:     n.dockerNetwork(),
		Labels:      n.labels(),
		Image:       image,
		Cmd:         append([]string{"run"}, r.Args...),
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
