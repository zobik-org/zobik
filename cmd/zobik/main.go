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
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := n.resolve(); err != nil {
		return err
	}
	_, statErr := os.Stat(filepath.Join(n.dir, "root.sealed"))
	user, password, err := readCredentials(os.IsNotExist(statErr))
	if err != nil {
		return err
	}
	e, err := deploy.NewEngine(ctx)
	if err != nil {
		return err
	}
	defer e.Close()
	return deploy.Init(ctx, e, deploy.Options{
		Network:  n.network,
		Dir:      n.dir,
		User:     user,
		Password: password,
		Image:    image,
		Scopes:   append(append([]bus.Scope{bus.ConsoleScope}, bus.RoleScopes...), devScopes...),
	})
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
