package deploy

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/nats-io/nkeys"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/signing"
)

// The structural roles' material (implementation §6, The root and the split
// Operator Channel). Each role keeps its key and what the root signed for it in
// a volume of its own, mounted at RoleDir:
//
//	identity.nk           the platform identity's seed
//	bus.jwt               its user JWT, which the console renews
//	root.pub              the root's public key, to verify against (thesis 9)
//	credential.jws        a structural node's capability credential
//	role_certificate.jws  the role certificate of a role that signs
//
// The console keeps only each identity's public key, in identities/<unit>.pub.
const (
	RoleDir             = "/var/lib/zobik"
	FileIdentity        = "identity.nk"
	FileBusJWT          = "bus.jwt"
	FileRootPub         = "root.pub"
	FileCredential      = "credential.jws"
	FileRoleCertificate = "role_certificate.jws"

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
}

// Roles are the structural roles this version brings up.
var Roles = []Role{
	{Unit: "config", Scope: bus.ConfigScope, Topic: "config_change", Consumers: claimant("config_change")},
	// The Task Broker receives the entire family: announcements and closes, and
	// every proposal (implementation §1.2.1).
	{Unit: "task_broker", Scope: bus.TaskBrokerScope, Certificate: signing.RoleTaskBroker,
		Consumers: func(id string) []bus.Consumer {
			return []bus.Consumer{
				{Stream: "task", Name: id, Filters: []string{"task.announced.>", "task.completed.>", "task.failed.>", "task.aborted.>"}},
				{Stream: "prop", Name: id, Filters: []string{"prop.>"}},
			}
		}},
	// The Context Store observes the terminal events to purge on trace close (implementation §1.2.9).
	{Unit: "context", Scope: bus.ContextScope,
		Consumers: func(id string) []bus.Consumer {
			return []bus.Consumer{{Stream: "task", Name: id, Filters: []string{"task.completed.>", "task.failed.>", "task.aborted.>"}}}
		}},
	// The panel's channels go first and last in their audience (implementation §6,
	// The operator console). A channel also receives the closes of the tasks it opens.
	{Unit: "channel_operator", Scope: bus.ChannelScope, Topic: "hitl_contact_operator",
		AttemptWindow: &signing.AttemptWindow{From: ptr(1), To: ptr(1), FinalRound: ptr(true)},
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
	var revoked []string
	for _, r := range Roles {
		created, err := e.EnsureVolume(ctx, n.volume(r.Unit), n.labels())
		if err != nil {
			return nil, fmt.Errorf("deploy: %s volume: %w", r.Unit, err)
		}
		pubFile := filepath.Join(dirIdentities, r.Unit+".pub")
		old, err := os.ReadFile(filepath.Join(dir, pubFile))
		hasOld := err == nil
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}

		var files []File
		var pub string
		if hasOld && !created {
			pub = string(old)
			jwtFile, err := m.roleJWT(r, pub)
			if err != nil {
				return nil, err
			}
			files = []File{jwtFile}
		} else {
			if hasOld {
				revoked = append(revoked, string(old))
			}
			if pub, files, err = m.mintRole(r, image); err != nil {
				return nil, fmt.Errorf("deploy: %s: %w", r.Unit, err)
			}
		}
		if err := e.WriteVolume(ctx, n.volume(r.Unit), image, RoleDir, files); err != nil {
			return nil, fmt.Errorf("deploy: writing %s's material: %w", r.Unit, err)
		}
		if err := writeFile(dir, pubFile, []byte(pub)); err != nil {
			return nil, err
		}
		ids[r.Unit] = pub
	}
	if len(revoked) == 0 {
		return ids, nil
	}
	updated, err := bus.Revoke(m.root, m.account.JWT, revoked...)
	if err != nil {
		return nil, err
	}
	m.account.JWT = updated
	return ids, writeFile(dir, fileAccount, []byte(updated))
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
		{Path: FileIdentity, Mode: 0o600, Data: seed},
		jwtFile,
		{Path: FileRootPub, Mode: 0o644, Data: []byte(rootPub)},
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
		files = append(files, File{Path: FileCredential, Mode: 0o644, Data: []byte(cred)})
	}
	if r.Certificate != "" {
		cert, err := signing.Sign(m.root, signing.TypeRoleCertificate, signing.RoleCertificate{
			Sub: pub, Iat: now, Role: r.Certificate,
		}, "")
		if err != nil {
			return "", nil, err
		}
		files = append(files, File{Path: FileRoleCertificate, Mode: 0o644, Data: []byte(cert)})
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
	return File{Path: FileBusJWT, Mode: 0o600, Data: []byte(token)}, nil
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
