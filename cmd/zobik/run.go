package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"zobik.org/zobik/internal/deploy"
	"zobik.org/zobik/internal/role"
	"zobik.org/zobik/internal/role/catalog"
	"zobik.org/zobik/internal/role/channel"
	"zobik.org/zobik/internal/role/config"
	"zobik.org/zobik/internal/role/contextstore"
	"zobik.org/zobik/internal/role/scanner"
	"zobik.org/zobik/internal/role/secrets"
	"zobik.org/zobik/internal/role/spawner"
	"zobik.org/zobik/internal/role/taskbroker"
	"zobik.org/zobik/internal/role/tenantlink"
	"zobik.org/zobik/internal/role/tracingcollector"
)

// zobik run --role=<role> is what each role container runs; the invocation uses
// the role's identifier (implementation §6, The roles of the binary).
func runCmd(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	name := fs.String("role", "", "the role: config, task_broker, context, channel, tracing_collector, secrets, spawner, tenant_link, scanner or catalog")
	capacity := fs.Int64("storage-capacity", 0, "catalog: the artifact storage capacity, in bytes")
	audience := fs.String("audience", "", "the audience a channel serves: operator or user")
	storeSize := fs.Int64("store-size", 0, "context: the ceiling of the content volume, in bytes (context_store_size)")
	traceSize := fs.Int64("trace-size", 0, "context: the ceiling of one trace, in bytes (context_trace_size)")
	network := fs.String("network", "", "tracing_collector and spawner: the network's name")
	fleetCapacity := fs.Int64("fleet-capacity", 0, "spawner: fleet_capacity, in MB")
	sidecarImage := fs.String("sidecar-image", "", "spawner: the Integration Sidecar's image")
	registry := fs.String("registry", "", "spawner: the address the engine pulls the artifacts from")
	lifetime := fs.Duration("node-identity-lifetime", deploy.NodeIdentityLifetime, "spawner: the life of a node's user JWT")
	if err := fs.Parse(args); err != nil {
		return err
	}
	var body role.Body
	switch *name {
	case "tenant_link":
		// It runs without the Bus (implementation §6, The roles of the binary).
		ca, err := os.ReadFile(deploy.TenantCAPath)
		if err != nil {
			return err
		}
		return tenantlink.Run(ctx, tenantlink.Options{
			Listen: ":" + deploy.LinkPort, Registry: deploy.RegistryAlias + ":" + deploy.RegistryPort, CA: ca,
		})
	case "scanner":
		// It runs without the Bus, and only catalog reaches it (implementation §1.2.19).
		return scanner.Run(ctx, scanner.Options{
			Repository: deploy.RegistryAlias + ":" + deploy.RegistryPort + "/" + deploy.Repository,
			CAFile:     deploy.TenantCAPath,
			Trivy:      scanner.TrivyPath,
			WorkDir:    os.TempDir(),
		}, scanner.Socket)
	case "catalog":
		// It runs without the Bus, and reaches its scanner over the socket only
		// the two mount (implementation §1.2.11, §1.2.19).
		return catalog.Run(ctx, catalog.Options{
			Listen:     ":" + catalog.Port,
			CertFile:   catalog.CertPath,
			KeyFile:    catalog.KeyPath,
			CAFile:     deploy.TenantCAPath,
			AdminFile:  catalog.AdminPath,
			DataDir:    catalog.DataDir,
			Repository: deploy.RegistryAlias + ":" + deploy.RegistryPort + "/" + deploy.Repository,
			Scanner:    scanner.Socket,
			Capacity:   *capacity,
		})
	case "config":
		body = func(ctx context.Context, rt *role.Runtime, nc *nats.Conn) error {
			return config.Run(ctx, rt, nc, config.Options{ContextAddr: role.AddrContext})
		}
	case "task_broker":
		body = taskbroker.Run
	case "secrets":
		body = secrets.Run
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
			tenantCA, err := rt.TenantCA()
			if err != nil {
				return err
			}
			backend, err := tracingcollector.Dial(ctx, deploy.TracingAlias, deploy.TracingOTLP, tenantCA)
			if err != nil {
				return err
			}
			return tracingcollector.Run(ctx, rt, nc, tracingcollector.Options{
				Network: *network, Backend: backend, DataDir: role.DataDir, ContextAddr: role.AddrContext,
			})
		}
	case "spawner":
		if *network == "" || *fleetCapacity <= 0 || *sidecarImage == "" || *registry == "" {
			return fmt.Errorf("the spawner role needs --network, --fleet-capacity, --sidecar-image and --registry")
		}
		body = func(ctx context.Context, rt *role.Runtime, nc *nats.Conn) error {
			js, err := jetstream.New(nc)
			if err != nil {
				return err
			}
			iss, err := deploy.NewIssuer(ctx, rt, js)
			if err != nil {
				return err
			}
			defer iss.Close()
			e, err := deploy.NewEngine(ctx)
			if err != nil {
				return err
			}
			defer e.Close()
			return spawner.Run(ctx, rt, nc, spawner.Options{
				Network: *network,
				Driver: &deploy.Driver{
					Engine: e, JS: js, Issuer: iss, Network: *network,
					SidecarImage: *sidecarImage, Registry: *registry,
					FleetCapacityMB: *fleetCapacity, IdentityLifetime: *lifetime,
				},
				Catalog:          deploy.SpawnerCatalog(iss),
				IdentityLifetime: *lifetime,
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
