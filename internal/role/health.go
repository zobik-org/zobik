package role

import (
	"bufio"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// HealthSocket is where a role answers zobik health, which the container's
// HEALTHCHECK runs: a Unix domain socket of the process itself, with no port
// (implementation §6, The health of the roles).
const HealthSocket = "/run/zobik/health.sock"

const (
	answerReady    = "ready"
	answerNotReady = "not ready"
)

// ServeHealth answers each connection to path with whether the role is ready,
// until ctx ends.
func (rt *Runtime) ServeHealth(ctx context.Context, path string) error {
	return ServeHealth(ctx, path, rt.Ready)
}

// ServeHealth answers each connection to path with what ready says, until ctx
// ends: the health of a role that runs without the Bus.
func ServeHealth(ctx context.Context, path string, ready func() bool) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	os.Remove(path) // one a previous process left
	ln, err := net.Listen("unix", path)
	if err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		ln.Close()
	}()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			answer := answerNotReady
			if ready() {
				answer = answerReady
			}
			// A checker that does not read in time reads nothing: it is not ready.
			_ = c.SetDeadline(time.Now().Add(time.Second))
			_, _ = c.Write([]byte(answer + "\n"))
			c.Close()
		}
	}()
	return nil
}

// ErrNotReady is a role that answers but does not serve yet.
var ErrNotReady = errors.New("role: not ready")

// CheckHealth asks the role at path whether it is ready.
func CheckHealth(path string, timeout time.Duration) error {
	c, err := net.DialTimeout("unix", path, timeout)
	if err != nil {
		return err
	}
	defer c.Close()
	if err := c.SetDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	line, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		return err
	}
	if strings.TrimSpace(line) != answerReady {
		return ErrNotReady
	}
	return nil
}
