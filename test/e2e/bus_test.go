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
	"syscall"
	"testing"
	"time"
)

// The Bus: an event published by hand appears in zobik tap.
func TestBus(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	z := newNetwork(t, ctx)

	z.run(t, ctx, credentials, "init")
	z.run(t, ctx, credentials, "init") // converges

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
	name, dir, bin string
	root           string // the repository, where images are built from
}

func newNetwork(t *testing.T, ctx context.Context) *network {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	suffix := make([]byte, 4)
	rand.Read(suffix)
	z := &network{name: "e2e" + hex.EncodeToString(suffix), dir: t.TempDir(), bin: filepath.Join(t.TempDir(), "zobik"), root: root}

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
	if args[0] == "config" || args[0] == "node" {
		words = 2
	}
	full := append(append([]string{}, args[:words]...), "--network", z.name, "--dir", z.dir)
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

// start runs a long-lived subcommand and streams its output lines; the test's
// end interrupts it, which removes its container.
func (z *network) start(t *testing.T, ctx context.Context, args ...string) <-chan string {
	t.Helper()
	c := z.cmd(ctx, "", args...)
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
	t.Cleanup(func() {
		c.Process.Signal(syscall.SIGINT)
		c.Wait()
	})
	return lines
}

func waitFor(t *testing.T, lines <-chan string, re *regexp.Regexp) {
	t.Helper()
	timeout := time.After(time.Minute)
	for {
		select {
		case l, ok := <-lines:
			if !ok {
				t.Fatalf("output ended before %s", re)
			}
			if re.MatchString(l) {
				return
			}
		case <-timeout:
			t.Fatalf("no line matching %s", re)
		}
	}
}

// remove takes down what the test brought up, found by the network's label.
func (z *network) remove(t *testing.T) {
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
