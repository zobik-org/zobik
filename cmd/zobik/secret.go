package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"golang.org/x/term"
)

// maxSecretInput bounds what zobik secret set reads from standard input.
const maxSecretInput = 64 << 10

// zobik secret set and zobik secret delete write the Secret Store through the
// console: an act of deployment, with the session open (implementation §6, The
// operator console, §1.2.18). The value never goes on the command line: at a
// terminal it is asked without echo; otherwise it is what follows the username
// and the password on standard input.
func secretCmd(ctx context.Context, args []string) error {
	usage := errors.New("usage: zobik secret set <handle> | delete <handle>")
	if len(args) == 0 || (args[0] != "set" && args[0] != "delete") {
		return usage
	}
	fs := flag.NewFlagSet("secret "+args[0], flag.ContinueOnError)
	var n networkFlags
	n.register(fs)
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return usage
	}
	handle := fs.Arg(0)
	if err := n.resolve(); err != nil {
		return err
	}

	var (
		user          string
		password, val []byte
		err           error
	)
	if term.IsTerminal(int(os.Stdin.Fd())) {
		if user, password, err = readCredentials(false); err != nil {
			return err
		}
		if args[0] == "set" {
			fmt.Fprint(os.Stderr, "Value: ")
			val, err = term.ReadPassword(int(os.Stdin.Fd()))
			fmt.Fprintln(os.Stderr)
			if err != nil {
				return err
			}
		}
	} else {
		in, err := io.ReadAll(io.LimitReader(os.Stdin, maxSecretInput+1))
		if err != nil {
			return err
		}
		if len(in) > maxSecretInput {
			return fmt.Errorf("the input exceeds %d bytes", maxSecretInput)
		}
		lines := bytes.SplitN(in, []byte("\n"), 3)
		if len(lines) < 2 {
			return errors.New("no username and password on standard input")
		}
		user, password = string(bytes.TrimRight(lines[0], "\r")), bytes.TrimRight(lines[1], "\r")
		if len(lines) == 3 {
			// Only the line's own end: the rest of the value is kept as is.
			val = bytes.TrimSuffix(bytes.TrimSuffix(lines[2], []byte("\n")), []byte("\r"))
		}
	}
	if args[0] == "set" && len(val) == 0 {
		return errors.New("no value")
	}

	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	c, err := signInAs(ctx, n, user, password)
	if err != nil {
		return err
	}
	defer func() { _ = c.SignOut(context.WithoutCancel(ctx)) }()
	if args[0] == "delete" {
		return c.DeleteSecret(ctx, handle)
	}
	return c.SetSecret(ctx, handle, val)
}
