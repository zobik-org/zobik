package bus

import (
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

// The Bus runs in operator mode: operator → account → user (implementation §1.2.1).
// The operator's key is the root (implementation §6, The root and the split Operator
// Channel), so only an act with the session open signs the account's JWT.

// Scope is what a scoped signing key issues: every user it signs carries these
// permissions, and {{subject()}} expands to the user's public nkey (implementation §1.2.1).
// An empty list denies the whole direction.
type Scope struct {
	Role      string
	Publish   []string
	Subscribe []string
}

// Account is the material zobik init mints for a network's Bus.
type Account struct {
	// Operator is the operator's JWT, self-signed by the root.
	Operator string
	// System is the system account's JWT, through which the full resolver takes
	// account updates (implementation §1.2.1); SystemKey signs its users.
	System    string
	SystemKey nkeys.KeyPair
	// JWT is the network's account; Public is its identity.
	JWT    string
	Public string
	// SigningKeys holds the scoped signing key of each role, by Scope.Role.
	SigningKeys map[string]nkeys.KeyPair
}

// NewAccount mints the operator, the system account and the network's account
// with one scoped signing key per scope. The account's identity key is never
// returned: the root signs the account, and every user comes from a scoped key
// (implementation §6, The root and the split Operator Channel).
func NewAccount(root nkeys.KeyPair, network string, scopes []Scope) (*Account, error) {
	rootPub, err := root.PublicKey()
	if err != nil {
		return nil, err
	}
	if !nkeys.IsValidPublicOperatorKey(rootPub) {
		return nil, errors.New("bus: the root is not an operator key")
	}

	sysKey, sysPub, err := newAccountKey()
	if err != nil {
		return nil, err
	}
	sys := jwt.NewAccountClaims(sysPub)
	sys.Name = "SYS"
	sysJWT, err := sys.Encode(root)
	if err != nil {
		return nil, err
	}

	op := jwt.NewOperatorClaims(rootPub)
	op.Name = "zobik-" + network
	op.SystemAccount = sysPub
	opJWT, err := op.Encode(root)
	if err != nil {
		return nil, err
	}

	_, accPub, err := newAccountKey()
	if err != nil {
		return nil, err
	}
	acc := jwt.NewAccountClaims(accPub)
	acc.Name = network
	// The streams carry their own byte ceilings (implementation §1.2.1).
	acc.Limits.JetStreamLimits = jwt.JetStreamLimits{MemoryStorage: jwt.NoLimit, DiskStorage: jwt.NoLimit, Streams: jwt.NoLimit, Consumer: jwt.NoLimit}
	accJWT, err := acc.Encode(root)
	if err != nil {
		return nil, err
	}

	a := &Account{Operator: opJWT, System: sysJWT, SystemKey: sysKey, JWT: accJWT, Public: accPub}
	if a.JWT, a.SigningKeys, err = RegisterScopes(root, accJWT, scopes); err != nil {
		return nil, err
	}
	return a, nil
}

// RegisterScopes returns the account's JWT, signed again by the root, with a new
// scoped signing key for each scope whose role it does not have yet, and those keys.
// Running it again with the same scopes changes nothing, so zobik init converges.
func RegisterScopes(root nkeys.KeyPair, accountJWT string, scopes []Scope) (string, map[string]nkeys.KeyPair, error) {
	acc, err := jwt.DecodeAccountClaims(accountJWT)
	if err != nil {
		return "", nil, err
	}
	have := map[string]bool{}
	for _, k := range acc.SigningKeys.Keys() {
		if s, ok := acc.SigningKeys.GetScope(k); ok && s != nil {
			have[s.(*jwt.UserScope).Role] = true
		}
	}
	keys := map[string]nkeys.KeyPair{}
	for _, s := range scopes {
		if have[s.Role] {
			continue
		}
		kp, pub, err := newAccountKey()
		if err != nil {
			return "", nil, err
		}
		us := jwt.NewUserScope()
		us.Key = pub
		us.Role = s.Role
		us.Template.Pub = permission(s.Publish)
		us.Template.Sub = permission(s.Subscribe)
		acc.SigningKeys.AddScopedSigner(us)
		keys[s.Role] = kp
		have[s.Role] = true
	}
	if len(keys) == 0 {
		return accountJWT, keys, nil
	}
	signed, err := acc.Encode(root)
	return signed, keys, err
}

// IssueUser mints a user JWT for the platform identity user, signed by a scoped
// signing key of account. The user carries no permissions of its own: the scope's apply.
func IssueUser(signingKey nkeys.KeyPair, account, user, name string, expires time.Time) (string, error) {
	uc := jwt.NewUserClaims(user)
	uc.Name = name
	uc.IssuerAccount = account
	uc.Expires = expires.Unix()
	// The server rejects a scoped user that sets any permission or limit, the
	// default unlimited ones included.
	uc.UserPermissionLimits = jwt.UserPermissionLimits{}
	return uc.Encode(signingKey)
}

// IssueSystemUser mints a user of the system account, to push account updates.
func IssueSystemUser(systemKey nkeys.KeyPair, user string, expires time.Time) (string, error) {
	uc := jwt.NewUserClaims(user)
	uc.Name = "console"
	uc.Expires = expires.Unix()
	return uc.Encode(systemKey)
}

// permission allows subjects, or denies everything when there are none: NATS
// reads an empty permission as allowing every subject.
func permission(subjects []string) jwt.Permission {
	if len(subjects) == 0 {
		return jwt.Permission{Deny: jwt.StringList{">"}}
	}
	return jwt.Permission{Allow: jwt.StringList(subjects)}
}

func newAccountKey() (nkeys.KeyPair, string, error) {
	kp, err := nkeys.CreateAccount()
	if err != nil {
		return nil, "", err
	}
	pub, err := kp.PublicKey()
	if err != nil {
		return nil, "", fmt.Errorf("bus: %w", err)
	}
	return kp, pub, nil
}
