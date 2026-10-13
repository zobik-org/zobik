package sidecar

import (
	"encoding/json"
	"fmt"
	"os"
)

// The Integration Sidecar's paths, which the act of provisioning mounts and
// writes (implementation §1.2.4, §2.1).
const (
	// SharedDir is the volume shared with the Logic Container: the NRI socket
	// and the work areas.
	SharedDir = "/run/zobik"
	// StateDir is the private volume: the outbox and the deduplication
	// (architecture §3.6.4).
	StateDir = "/var/lib/zobik"
	// StartupPath is the startup configuration, which the Spawner copies into
	// the container before starting it.
	StartupPath = "/etc/zobik/startup.json"
)

// Startup is the startup configuration: what the Spawner hands over in the act
// of provisioning (implementation §1.2.4). It carries no address: the services
// the Integration Sidecar reaches have the same name in every network.
type Startup struct {
	// Identity is the platform identity's seed, and BusJWT its user JWT, which
	// the Spawner renews by writing the file again.
	Identity string `json:"identity"`
	BusJWT   string `json:"bus_jwt"`
	// Credential is the capability credential, in compact JWS.
	Credential string `json:"credential"`
	// Root is the root's public key, which the provision grants' proofs
	// verify against (implementation §1.2.4).
	Root string `json:"root"`
	// NRIVersion is the io.zobik.nri.version label of the Logic Container's
	// image (implementation §3, The interface version).
	NRIVersion string `json:"nri_version"`
}

// LoadStartup reads the startup configuration at path.
func LoadStartup(path string) (*Startup, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("sidecar: startup configuration: %w", err)
	}
	var s Startup
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("sidecar: startup configuration: %w", err)
	}
	return &s, nil
}
