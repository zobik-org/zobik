package deploy

import (
	"fmt"
	"strings"

	"github.com/moby/moby/api/types/mount"
	"github.com/nats-io/jwt/v2"
)

// NATSImage is the server this version of zobik installs, pinned by the digest of
// the official image (implementation §1.2.1). It has to match the nats-server
// module in go.mod, which the tests run in process.
const NATSImage = "nats:2.15.0@sha256:cd3fcd4ecdda44e3a66728a5334af0a959bc3979b32810e033d1c547241cd0f4"

const (
	natsConfigPath = "/etc/nats/nats-server.conf"
	natsDataDir    = "/data"
)

// natsSpec is the NATS server's container: alias nats on the network's Docker
// network, no port published on the host, its state in its own volume.
func natsSpec(n names, config []byte) Spec {
	return Spec{
		Name:    n.container("nats"),
		Alias:   "nats",
		Network: n.dockerNetwork(),
		NetName: n.network,
		Image:   NATSImage,
		Cmd:     []string{"--config", natsConfigPath},
		Mounts:  []mount.Mount{{Type: mount.TypeVolume, Source: n.volume("nats"), Target: natsDataDir}},
		Files:   []File{{Path: natsConfigPath, Mode: 0o644, Data: config}},
		Restart: true,
	}
}

// natsConfig is the server's configuration in operator mode with the full
// resolver (implementation §1.2.1). The preload only seeds the resolver's
// directory: later account updates go through the system account.
func natsConfig(dataDir, operatorJWT string, accounts ...string) ([]byte, error) {
	op, err := jwt.DecodeOperatorClaims(operatorJWT)
	if err != nil {
		return nil, err
	}
	var b strings.Builder
	fmt.Fprintf(&b, "port: 4222\n")
	fmt.Fprintf(&b, "server_name: nats\n")
	fmt.Fprintf(&b, "operator: %q\n", operatorJWT)
	fmt.Fprintf(&b, "system_account: %q\n", op.SystemAccount)
	fmt.Fprintf(&b, "resolver: {\n  type: full\n  dir: %q\n  allow_delete: false\n}\n", dataDir+"/resolver")
	fmt.Fprintf(&b, "resolver_preload: {\n")
	for _, a := range accounts {
		ac, err := jwt.DecodeAccountClaims(a)
		if err != nil {
			return nil, err
		}
		fmt.Fprintf(&b, "  %s: %q\n", ac.Subject, a)
	}
	fmt.Fprintf(&b, "}\n")
	fmt.Fprintf(&b, "jetstream: {\n  store_dir: %q\n}\n", dataDir+"/jetstream")
	return []byte(b.String()), nil
}
