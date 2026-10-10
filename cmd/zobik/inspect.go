//go:build dev

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/deploy"
	"zobik.org/zobik/internal/globalconfig"
)

// zobik inspect shows what zobik init created on the Bus: the streams, each
// role's consumers, the buckets and the head of the Global Configuration. It runs
// with an identity of the console's scope, which reads JetStream's API.
func init() {
	devCommands["inspect"] = inspectCmd
}

const identitiesPath = "/run/zobik/identities.json"

func inspectCmd(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("inspect", flag.ContinueOnError)
	var n networkFlags
	n.register(fs)
	inside := fs.Bool("inside", false, "run inside the network (the ephemeral container does)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *inside {
		return inspectInside(ctx)
	}
	if err := n.resolve(); err != nil {
		return err
	}
	ids, err := deploy.Identities(n.dir)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(ids)
	if err != nil {
		return err
	}
	return runOnBus(ctx, n, bus.ConsoleScope.Role, []string{"inspect", "--inside"}, false,
		deploy.File{Path: identitiesPath, Mode: 0o600, Data: raw})
}

func inspectInside(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var units map[string]string // unit by identity
	if raw, err := os.ReadFile(identitiesPath); err == nil {
		var ids map[string]string
		if err := json.Unmarshal(raw, &ids); err != nil {
			return err
		}
		units = map[string]string{}
		for unit, id := range ids {
			units[id] = unit
		}
	}
	nc, err := bus.Connect(ctx, bus.URL, deploy.CredsPath)
	if err != nil {
		return err
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}

	fmt.Println("Streams")
	var streams []*jetstream.StreamInfo
	for s := range js.ListStreams(ctx).Info() {
		streams = append(streams, s)
	}
	sort.Slice(streams, func(i, j int) bool { return streams[i].Config.Name < streams[j].Config.Name })
	for _, s := range streams {
		retention := "forever"
		if s.Config.MaxAge > 0 {
			retention = s.Config.MaxAge.String()
		}
		fmt.Printf("  %-16s %-22s %4d msgs  retains %s\n", s.Config.Name, strings.Join(s.Config.Subjects, " "), s.State.Msgs, retention)
	}

	fmt.Println("\nConsumers")
	for _, stream := range []string{"task", "prop"} {
		st, err := js.Stream(ctx, stream)
		if err != nil {
			return err
		}
		for c := range st.ListConsumers(ctx).Info() {
			who := units[c.Name]
			if who == "" {
				who = "?"
			}
			fmt.Printf("  %s/%s… (%s)  pending %d\n", stream, c.Name[:8], who, c.NumPending)
			for _, f := range c.Config.FilterSubjects {
				fmt.Printf("      %s\n", f)
			}
		}
	}

	fmt.Println("\nBuckets")
	for _, bucket := range []string{bus.BucketConfig, bus.BucketTasks} {
		kv, err := js.KeyValue(ctx, bucket)
		if err != nil {
			fmt.Printf("  %-8s missing: %v\n", bucket, err)
			continue
		}
		st, err := kv.Status(ctx)
		if err != nil {
			return err
		}
		ttl := "none"
		if st.TTL() > 0 {
			ttl = st.TTL().String()
		}
		fmt.Printf("  %-8s %d keys  ttl %s\n", bucket, st.Values(), ttl)
	}

	head, _, err := globalconfig.Head(ctx, js)
	if err != nil {
		return err
	}
	fmt.Printf("\nGlobal Configuration: head %s, published %s by %s\n", head.ConfigVersion, head.PublishedAt.Format(time.RFC3339), head.AuthorizedBy)
	keys := make([]string, 0, len(head.Values))
	for k := range head.Values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("  %-30s %s\n", k, head.Values[k])
	}
	return nil
}
