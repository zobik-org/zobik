// Package rpcauth authenticates the gRPC connections of the off-Bus interfaces
// with a handshake when opening them (implementation §6, The off-Bus interfaces):
// the role sends a nonce; the caller returns its user JWT and the nonce signed
// with its platform identity's key; the role verifies that this network's account
// issued the JWT, that it is current and that the signature is from the nkey the
// JWT names. The connection stays authenticated as that identity, and lives as
// long as the JWT.
package rpcauth

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

// handshakeTimeout bounds the exchange, so a caller that stalls holds nothing.
const handshakeTimeout = 10 * time.Second

// Info is what an authenticated connection carries.
type Info struct {
	credentials.CommonAuthInfo
	// Identity is the caller's platform identity, its public nkey.
	Identity string
	// Role is the role of the scoped signing key that issued its JWT: one per
	// role, and the console's own (implementation §6, The off-Bus interfaces).
	Role    string
	Expires time.Time
}

func (Info) AuthType() string { return "zobik" }

// FromContext returns the identity of the connection a call arrived over.
func FromContext(ctx context.Context) (Info, bool) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return Info{}, false
	}
	info, ok := p.AuthInfo.(Info)
	return info, ok
}

// hello is what the caller answers the nonce with.
type hello struct {
	JWT       string `json:"jwt"`
	Signature []byte `json:"signature"`
}

// verdict is the role's answer, so the caller learns why it was rejected.
type verdict struct {
	Error string `json:"error,omitempty"`
}

// Account returns this network's account JWT, already verified against the root.
type Account func() (*jwt.AccountClaims, error)

// AccountFromJWT verifies an account JWT against the root.
func AccountFromJWT(accountJWT, root string) (*jwt.AccountClaims, error) {
	ac, err := jwt.DecodeAccountClaims(accountJWT)
	if err != nil {
		return nil, err
	}
	if ac.Issuer != root {
		return nil, errors.New("rpcauth: the account is not the root's")
	}
	return ac, nil
}

// Server is the role's side of the handshake.
type Server struct {
	Account Account
	now     func() time.Time
}

func (s *Server) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

func (s *Server) ServerHandshake(raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	raw.SetDeadline(time.Now().Add(handshakeTimeout))
	nonce := make([]byte, 32)
	rand.Read(nonce)
	if err := writeLine(raw, nonce); err != nil {
		return nil, nil, err
	}
	var h hello
	r := bufio.NewReader(raw)
	if err := readLine(r, &h); err != nil {
		return nil, nil, err
	}
	info, err := s.verify(h, nonce)
	answer := verdict{}
	if err != nil {
		answer.Error = err.Error()
	}
	if werr := writeLine(raw, answer); werr != nil && err == nil {
		err = werr
	}
	if err != nil {
		raw.Close()
		return nil, nil, err
	}
	raw.SetDeadline(time.Time{})
	return expiring(&buffered{Conn: raw, r: r}, info.Expires), info, nil
}

func (s *Server) verify(h hello, nonce []byte) (Info, error) {
	uc, err := jwt.DecodeUserClaims(h.JWT)
	if err != nil {
		return Info{}, fmt.Errorf("rpcauth: %w", err)
	}
	ac, err := s.Account()
	if err != nil {
		return Info{}, err
	}
	// Comparing the account leaves out an identity of another network of the tenant.
	if uc.IssuerAccount != ac.Subject {
		return Info{}, errors.New("rpcauth: the JWT is not of this network's account")
	}
	scope, ok := ac.SigningKeys.GetScope(uc.Issuer)
	if !ok {
		return Info{}, errors.New("rpcauth: the JWT is not issued by a signing key of the account")
	}
	role := ""
	if us, ok := scope.(*jwt.UserScope); ok {
		role = us.Role
	}
	now := s.clock()
	if uc.Expires == 0 || now.Unix() >= uc.Expires {
		return Info{}, errors.New("rpcauth: the JWT has expired")
	}
	if ac.IsClaimRevoked(uc) {
		return Info{}, errors.New("rpcauth: the identity is revoked")
	}
	key, err := nkeys.FromPublicKey(uc.Subject)
	if err != nil {
		return Info{}, fmt.Errorf("rpcauth: %w", err)
	}
	if err := key.Verify(nonce, h.Signature); err != nil {
		return Info{}, errors.New("rpcauth: the nonce is not signed by the JWT's identity")
	}
	return Info{
		CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.NoSecurity},
		Identity:       uc.Subject,
		Role:           role,
		Expires:        time.Unix(uc.Expires, 0),
	}, nil
}

func (s *Server) ClientHandshake(context.Context, string, net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, errors.New("rpcauth: server credentials")
}
func (s *Server) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: "zobik"}
}
func (s *Server) Clone() credentials.TransportCredentials { c := *s; return &c }
func (s *Server) OverrideServerName(string) error         { return nil }

// Client is the caller's side: its user JWT, read on each connection so a
// reconnection takes the renewed one, and the key of its platform identity.
type Client struct {
	JWT func() (string, error)
	Key nkeys.KeyPair
}

func (c *Client) ClientHandshake(ctx context.Context, _ string, raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	deadline := time.Now().Add(handshakeTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	raw.SetDeadline(deadline)
	r := bufio.NewReader(raw)
	var nonce []byte
	if err := readLine(r, &nonce); err != nil {
		return nil, nil, err
	}
	token, err := c.JWT()
	if err != nil {
		return nil, nil, err
	}
	sig, err := c.Key.Sign(nonce)
	if err != nil {
		return nil, nil, err
	}
	if err := writeLine(raw, hello{JWT: token, Signature: sig}); err != nil {
		return nil, nil, err
	}
	var v verdict
	if err := readLine(r, &v); err != nil {
		return nil, nil, err
	}
	if v.Error != "" {
		raw.Close()
		return nil, nil, errors.New(v.Error)
	}
	raw.SetDeadline(time.Time{})
	return &buffered{Conn: raw, r: r}, Info{CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.NoSecurity}}, nil
}

func (c *Client) ServerHandshake(net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, errors.New("rpcauth: client credentials")
}
func (c *Client) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: "zobik"}
}
func (c *Client) Clone() credentials.TransportCredentials { d := *c; return &d }
func (c *Client) OverrideServerName(string) error         { return nil }

// The handshake travels as one JSON line per message.
func writeLine(w net.Conn, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = w.Write(append(b, '\n'))
	return err
}

func readLine(r *bufio.Reader, v any) error {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return fmt.Errorf("rpcauth: handshake: %w", err)
	}
	return json.Unmarshal(line, v)
}

// buffered keeps what the handshake's reader already took from the connection.
type buffered struct {
	net.Conn
	r *bufio.Reader
}

func (b *buffered) Read(p []byte) (int, error) { return b.r.Read(p) }

// expiring closes the connection when the JWT it was opened with expires, like
// the NATS server, so the extinction of an identity reaches outside the Bus too.
func expiring(c net.Conn, at time.Time) net.Conn {
	e := &expiringConn{Conn: c}
	e.timer = time.AfterFunc(time.Until(at), func() { c.Close() })
	return e
}

type expiringConn struct {
	net.Conn
	once  sync.Once
	timer *time.Timer
}

func (e *expiringConn) Close() error {
	e.once.Do(func() { e.timer.Stop() })
	return e.Conn.Close()
}
