package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"runtime"
	"time"

	"golang.org/x/term"

	"zobik.org/zobik/internal/console"
	"zobik.org/zobik/internal/deploy"
)

// zobik console serves the panel and the local API in the foreground
// (implementation §6, The operator console).
func consoleCmd(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("console", flag.ContinueOnError)
	var n networkFlags
	n.register(fs)
	idle := registerIdleTimeout(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := n.resolve(); err != nil {
		return err
	}
	port, err := deploy.KnownPanelPort(n.dir)
	if err != nil {
		return err
	}
	ln, err := listenPanel(n, port)
	if err != nil {
		return err
	}
	svc, err := newConsole(n.dir, *idle)
	if err != nil {
		return err
	}
	fmt.Printf("console: serving the panel at http://localhost:%s/\n", port)
	return svc.Serve(ctx, ln)
}

func registerIdleTimeout(fs *flag.FlagSet) *time.Duration {
	return fs.Duration("session-idle-timeout", 30*time.Minute, "session_idle_timeout: the time without activity after which the console closes a session")
}

// listenPanel takes the panel's port, which only one console of the network holds.
func listenPanel(n networkFlags, port string) (net.Listener, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:"+port)
	if err != nil {
		return nil, fmt.Errorf("the console of the network %s is already running on port %s, or another process holds it: %w", n.network, port, err)
	}
	return ln, nil
}

// newConsole is the console of the network in dir, with the panel's channels
// the network runs.
func newConsole(dir string, idle time.Duration) (*console.Service, error) {
	var channels []console.Channel
	for unit, audience := range map[string]string{"channel_operator": console.AudienceOperator, "channel_user": console.AudienceUser} {
		id, addr, err := deploy.Channel(dir, unit)
		if errors.Is(err, deploy.ErrNoChannel) {
			continue
		}
		if err != nil {
			return nil, err
		}
		channels = append(channels, console.Channel{Audience: audience, ID: id, Addr: addr})
	}
	return console.New(console.Options{
		Open: func(user string, password []byte) (*console.Keys, error) {
			s, err := deploy.OpenSession(dir, user, password)
			if err != nil {
				return nil, err
			}
			return &console.Keys{Root: s.Root, Console: s.Console, JWT: s.ConsoleJWT, Close: s.Close}, nil
		},
		Channels:    channels,
		IdleTimeout: idle,
	}), nil
}

// consoleClient signs in to the network's console with the operator's username
// and password.
func consoleClient(ctx context.Context, n networkFlags) (*console.Client, error) {
	port, err := deploy.KnownPanelPort(n.dir)
	if err != nil {
		return nil, err
	}
	user, password, err := readCredentials(false)
	if err != nil {
		return nil, err
	}
	c, err := console.SignIn(ctx, "http://127.0.0.1:"+port, user, password)
	if err != nil {
		return nil, fmt.Errorf("opening the session: %w", err)
	}
	return c, nil
}

// openBrowser opens url when the person is at a terminal; whatever fails, the
// link stays printed.
func openBrowser(url string) {
	if !term.IsTerminal(int(os.Stdin.Fd())) {
		return
	}
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", url) //nolint:gosec // url is the console's own link
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url) //nolint:gosec // url is the console's own link
	default:
		cmd = exec.Command("xdg-open", url) //nolint:gosec // url is the console's own link
	}
	if cmd.Start() == nil {
		go func() { _ = cmd.Wait() }()
	}
}
