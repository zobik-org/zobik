package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"

	"zobik.org/zobik/internal/deploy"
	"zobik.org/zobik/internal/role"
	"zobik.org/zobik/internal/role/channel"
	"zobik.org/zobik/internal/role/config"
	"zobik.org/zobik/internal/role/contextstore"
	"zobik.org/zobik/internal/role/taskbroker"
	"zobik.org/zobik/internal/role/tracingcollector"
)

// zobik run --role=<role> is what each role container runs; the invocation uses
// the role's identifier (implementation §6, The roles of the binary).
func runCmd(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	name := fs.String("role", "", "the role: config, task_broker, context, channel or tracing_collector")
	audience := fs.String("audience", "", "the audience a channel serves: operator or user")
	storeSize := fs.Int64("store-size", 0, "context: the ceiling of the content volume, in bytes (context_store_size)")
	traceSize := fs.Int64("trace-size", 0, "context: the ceiling of one trace, in bytes (context_trace_size)")
	network := fs.String("network", "", "tracing_collector: the network's name, the spans' service.name")
	if err := fs.Parse(args); err != nil {
		return err
	}
	var body role.Body
	switch *name {
	case "config":
		body = func(ctx context.Context, rt *role.Runtime, nc *nats.Conn) error {
			return config.Run(ctx, rt, nc, config.Options{ContextAddr: role.AddrContext})
		}
	case "task_broker":
		body = taskbroker.Run
	case "context":
		if *storeSize <= 0 || *traceSize <= 0 {
			return fmt.Errorf("the context role needs --store-size and --trace-size")
		}
		body = func(ctx context.Context, rt *role.Runtime, nc *nats.Conn) error {
			return contextstore.Run(ctx, rt, nc, contextstore.Options{StoreSize: *storeSize, TraceSize: *traceSize})
		}
	case "channel":
		if *audience != channel.AudienceOperator && *audience != channel.AudienceUser {
			return fmt.Errorf("a channel serves --audience=%s or %s", channel.AudienceOperator, channel.AudienceUser)
		}
		body = func(ctx context.Context, rt *role.Runtime, nc *nats.Conn) error {
			return channel.Run(ctx, rt, nc, channel.Options{
				Audience: *audience, Listen: ":" + role.PortChannel, ContextAddr: role.AddrContext,
			})
		}
	case "tracing_collector":
		if *network == "" {
			return fmt.Errorf("the tracing_collector role needs --network")
		}
		body = func(ctx context.Context, rt *role.Runtime, nc *nats.Conn) error {
			backend, err := tracingcollector.Dial(ctx, deploy.TracingAlias+":"+deploy.TracingOTLP)
			if err != nil {
				return err
			}
			return tracingcollector.Run(ctx, rt, nc, tracingcollector.Options{
				Network: *network, Backend: backend, DataDir: role.DataDir, ContextAddr: role.AddrContext,
			})
		}
	default:
		return fmt.Errorf("unknown role %q", *name)
	}
	return role.Main(ctx, *name, body)
}

// zobik health is the container's HEALTHCHECK: it asks the role in the same
// container whether it is ready (implementation §6, The health of the roles).
func healthCmd(context.Context, []string) error {
	return role.CheckHealth(role.HealthSocket, 2*time.Second)
}
