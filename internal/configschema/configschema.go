// Package configschema assembles the Global Configuration schema this binary
// brings from the fragment each consumer declares next to its code
// (implementation §1.2.5). The schema's version is the binary's.
package configschema

import (
	"sync"

	"zobik.org/zobik/internal/egress"
	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/internal/role/channel"
	"zobik.org/zobik/internal/role/contextstore"
	"zobik.org/zobik/internal/role/spawner"
	"zobik.org/zobik/internal/role/taskbroker"
	"zobik.org/zobik/internal/sidecar"
)

// Load returns the assembled schema.
var Load = sync.OnceValues(func() (*globalconfig.Schema, error) {
	s, err := globalconfig.Assemble(
		taskbroker.ConfigSchema,
		contextstore.ConfigSchema,
		channel.ConfigSchema,
		spawner.ConfigSchema,
		sidecar.ConfigSchema,
	)
	if err != nil {
		return nil, err
	}
	// An egress entry's conditions and billing_rule compile against its
	// interface (architecture §3.14.4, implementation §8).
	if err := s.AddCheck(egress.CheckRegistry, "egress_registry"); err != nil {
		return nil, err
	}
	if err := s.AddCheck(egress.CheckSpace, "egress_registry", "embedding_model"); err != nil {
		return nil, err
	}
	return s, nil
})
