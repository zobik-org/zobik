// Command zobik is the operator console, the command line and every
// structural role of a Zobik network, each run as zobik run --role=<role>.
package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/term"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/deploy"
)

// image is the zobik image the roles and the console's acts run; the release
// build pins it by digest.
var image = "zobik:dev"

// devScopes are the scopes only a development binary registers (dev.go).
var devScopes []bus.Scope

// devCommands are the subcommands only a development binary has (dev.go).
var devCommands = map[string]func(ctx context.Context, args []string) error{}

// devInitFlags register the flags of zobik init only a development binary has (dev.go).
var devInitFlags []func(fs *flag.FlagSet, o *deploy.Options)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "zobik:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: zobik <command>")
	}
	switch cmd, rest := args[0], args[1:]; cmd {
	case "init":
		return initCmd(ctx, rest)
	case "run":
		return runCmd(ctx, rest)
	case "health":
		return healthCmd(ctx, rest)
	case "config":
		return configCmd(ctx, rest)
	case "console":
		return consoleCmd(ctx, rest)
	case deploy.ActCommand:
		if len(rest) != 1 {
			return fmt.Errorf("usage: zobik %s <act>", deploy.ActCommand)
		}
		return deploy.RunAct(ctx, rest[0])
	default:
		if f, ok := devCommands[cmd]; ok {
			return f(ctx, rest)
		}
		return fmt.Errorf("unknown command %q", cmd)
	}
}

// networkFlags are the flags that select a network and its directory on the host.
type networkFlags struct {
	network, dir string
}

func (n *networkFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&n.network, "network", "main", "the network's name")
	fs.StringVar(&n.dir, "dir", "", "the console's directory for the network (default: the user config directory)")
}

func (n *networkFlags) resolve() error {
	if n.dir != "" {
		return nil
	}
	base, err := os.UserConfigDir()
	if err != nil {
		return err
	}
	n.dir = filepath.Join(base, "zobik", n.network)
	return nil
}

func initCmd(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	var n networkFlags
	n.register(fs)
	var o deploy.Options
	fs.StringVar(&o.Tenant, "tenant", "", "the identifier of a tenant on this host to join (default: a new tenant)")
	idle := registerIdleTimeout(fs)
	for _, f := range devInitFlags {
		f(fs, &o)
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := n.resolve(); err != nil {
		return err
	}
	// A console that already runs for the network stops zobik init before it
	// touches anything.
	port, err := deploy.PanelPort(n.dir)
	if err != nil {
		return err
	}
	ln, err := listenPanel(n, port)
	if err != nil {
		return err
	}
	defer ln.Close()
	_, statErr := os.Stat(filepath.Join(n.dir, "root.sealed"))
	user, password, err := readCredentials(os.IsNotExist(statErr))
	if err != nil {
		return err
	}
	e, err := deploy.NewEngine(ctx)
	if err != nil {
		return err
	}
	o.Network, o.Dir, o.User, o.Password, o.Image = n.network, n.dir, user, password, image
	o.Scopes = append(append([]bus.Scope{bus.ConsoleScope}, bus.RoleScopes...), devScopes...)
	err = deploy.Init(ctx, e, o)
	e.Close()
	if err != nil {
		return err
	}

	// It ends serving the console, with the session open and a single-use link
	// to it (implementation §6, Installation and zobik init).
	svc, err := newConsole(n.dir, *idle)
	if err != nil {
		return err
	}
	path, err := svc.Link(user, password)
	if err != nil {
		return err
	}
	url := "http://localhost:" + port + path
	fmt.Printf("console: the network is up; the panel is at %s\n", url)
	openBrowser(url)
	return svc.Serve(ctx, ln)
}

// readCredentials asks for the operator's username and password on the
// terminal, the password twice when it is new. Without a terminal it reads them
// from standard input, one per line.
func readCredentials(isNew bool) (string, []byte, error) {
	fd := int(os.Stdin.Fd())
	if !term.IsTerminal(fd) {
		in := bufio.NewReader(os.Stdin)
		user, err := in.ReadString('\n')
		if err != nil {
			return "", nil, errors.New("no username on standard input")
		}
		pw, err := in.ReadBytes('\n')
		if err != nil && len(pw) == 0 {
			return "", nil, errors.New("no password on standard input")
		}
		return strings.TrimRight(user, "\r\n"), bytes.TrimRight(pw, "\r\n"), nil
	}
	fmt.Fprint(os.Stderr, "Username: ")
	user, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		return "", nil, err
	}
	user = strings.TrimSpace(user)
	if user == "" {
		return "", nil, errors.New("empty username")
	}
	fmt.Fprint(os.Stderr, "Password: ")
	pw, err := term.ReadPassword(fd)
	fmt.Fprintln(os.Stderr)
	if err != nil {
		return "", nil, err
	}
	if len(pw) == 0 {
		return "", nil, errors.New("empty password")
	}
	if isNew {
		fmt.Fprint(os.Stderr, "Repeat it: ")
		again, err := term.ReadPassword(fd)
		fmt.Fprintln(os.Stderr)
		if err != nil {
			return "", nil, err
		}
		if !bytes.Equal(pw, again) {
			return "", nil, errors.New("the passwords do not match")
		}
	}
	return user, pw, nil
}
