//go:build e2e

// Package e2e holds one end-to-end test per development stage, each against a
// real container engine. Run with: go test -tags e2e ./test/e2e/
package e2e

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"zobik.org/zobik/internal/deploy"
)

// The Bus: an event published by hand appears in zobik tap.
func TestBus(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	z := newNetwork(t, ctx)

	_, stop := z.init(t, ctx)
	stop()
	z.init(t, ctx) // converges

	lines := z.start(t, ctx, "tap")
	waitFor(t, lines, regexp.MustCompile(`^tap: listening`))

	out := z.run(t, ctx, "", "publish", "--topic", "echo", "--data", `{"text":"hola"}`)
	id := regexp.MustCompile(`published task\.announced (\S+) on task\.announced\.echo`).FindStringSubmatch(out)
	if id == nil {
		t.Fatalf("publish said: %s", out)
	}
	waitFor(t, lines, regexp.MustCompile(`"id": "`+regexp.QuoteMeta(id[1])+`"`))

	if out, err := z.cmd(ctx, "", "publish", "--type", "task.failed", "--identity", "UX", "--topic", "echo").CombinedOutput(); err == nil {
		t.Errorf("a task.failed without failure_reason was published: %s", out)
	}
}

type network struct {
	name, home, bin string
	dir             string // the network's directory in home
	root            string // the repository, where images are built from
}

func newNetwork(t *testing.T, ctx context.Context) *network {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	suffix := make([]byte, 4)
	rand.Read(suffix)
	name, home := "e2e"+hex.EncodeToString(suffix), t.TempDir()
	z := &network{name: name, home: home, dir: deploy.NetworkDir(home, name), bin: filepath.Join(t.TempDir(), "zobik"), root: root}

	z.build(t, ctx, "images/zobik.Dockerfile", "zobik:dev", "--build-arg", "TAGS=dev")
	gobuild := exec.CommandContext(ctx, "go", "build", "-tags", "dev", "-o", z.bin, "./cmd/zobik")
	gobuild.Dir = root
	if out, err := gobuild.CombinedOutput(); err != nil {
		t.Fatalf("building zobik: %v\n%s", err, out)
	}
	t.Cleanup(func() { z.remove(t) })
	return z
}

// build builds an image from a Dockerfile of the repository.
func (z *network) build(t *testing.T, ctx context.Context, dockerfile, tag string, args ...string) {
	t.Helper()
	build := exec.CommandContext(ctx, "docker", append(append([]string{"build", "-q", "-f", dockerfile, "-t", tag}, args...), ".")...)
	build.Dir = z.root
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building %s: %v\n%s", tag, err, out)
	}
}

// cmd runs a subcommand on the test's network. The network's flags go right
// after the command's words, before the arguments the command parses itself.
func (z *network) cmd(ctx context.Context, stdin string, args ...string) *exec.Cmd {
	words := 1
	if args[0] == "config" || args[0] == "node" || args[0] == "secret" || args[0] == "rates" {
		words = 2
	}
	full := append(append([]string{}, args[:words]...), "--network", z.name, "--home", z.home)
	full = append(full, args[words:]...)
	c := exec.CommandContext(ctx, z.bin, full...)
	c.Stdin = strings.NewReader(stdin)
	return c
}

func (z *network) run(t *testing.T, ctx context.Context, stdin string, args ...string) string {
	t.Helper()
	out, err := z.cmd(ctx, stdin, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("zobik %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

// init runs zobik init, which ends serving the console in the foreground, and
// returns once the network is up, with the panel's single-use link; stop stops
// that console.
func (z *network) init(t *testing.T, ctx context.Context) (link string, stop func()) {
	t.Helper()
	lines, stop := z.startWith(t, ctx, credentials, "init")
	m := waitWithin(t, lines, regexp.MustCompile(`the panel is at (\S+)`), 8*time.Minute)
	return m[1], stop
}

// start runs a long-lived subcommand and streams its output lines; the test's
// end interrupts it, which removes its container.
func (z *network) start(t *testing.T, ctx context.Context, args ...string) <-chan string {
	t.Helper()
	lines, _ := z.startWith(t, ctx, "", args...)
	return lines
}

// startWith is start with stdin, and returns too a stop that interrupts the
// subcommand and waits for it.
func (z *network) startWith(t *testing.T, ctx context.Context, stdin string, args ...string) (<-chan string, func()) {
	t.Helper()
	c := z.cmd(ctx, stdin, args...)
	c.Cancel = func() error { return c.Process.Signal(os.Interrupt) }
	c.WaitDelay = 15 * time.Second
	stdout, err := c.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	c.Stderr = c.Stdout
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	lines := make(chan string, 256)
	go func() {
		defer close(lines)
		s := bufio.NewScanner(stdout)
		for s.Scan() {
			t.Log(s.Text())
			lines <- s.Text()
		}
	}()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			c.Process.Signal(syscall.SIGINT)
			c.Wait()
		})
	}
	t.Cleanup(stop)
	return lines, stop
}

func waitFor(t *testing.T, lines <-chan string, re *regexp.Regexp) {
	t.Helper()
	waitWithin(t, lines, re, time.Minute)
}

// waitWithin waits for a line matching re and returns its submatches.
func waitWithin(t *testing.T, lines <-chan string, re *regexp.Regexp, within time.Duration) []string {
	t.Helper()
	timeout := time.After(within)
	for {
		select {
		case l, ok := <-lines:
			if !ok {
				t.Fatalf("output ended before %s", re)
			}
			if m := re.FindStringSubmatch(l); m != nil {
				return m
			}
		case <-timeout:
			t.Fatalf("no line matching %s", re)
		}
	}
}

// remove takes down what the test brought up, found by the network's label,
// and the tenant init created for it.
func (z *network) remove(t *testing.T) {
	if tenant, err := os.ReadFile(filepath.Join(z.dir, "tenant")); err == nil {
		tracing := "zobik-tenant-" + string(tenant) + "-tracing"
		exec.Command("docker", "rm", "-f", tracing).Run()
		exec.Command("docker", "volume", "rm", tracing).Run()
	}
	label := "label=org.zobik.network=" + z.name
	ids, _ := exec.Command("docker", "ps", "-aq", "--filter", label).Output()
	if f := strings.Fields(string(ids)); len(f) > 0 {
		exec.Command("docker", append([]string{"rm", "-f"}, f...)...).Run()
	}
	vols, _ := exec.Command("docker", "volume", "ls", "-q", "--filter", label).Output()
	if f := strings.Fields(string(vols)); len(f) > 0 {
		exec.Command("docker", append([]string{"volume", "rm"}, f...)...).Run()
	}
	exec.Command("docker", "network", "rm", "zobik-"+z.name).Run()
}
