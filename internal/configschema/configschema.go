// Package configschema assembles the Global Configuration schema this binary
// brings from the fragment each consumer declares next to its code
// (implementation §1.2.5). The schema's version is the binary's.
package configschema

import (
	"sync"

	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/internal/role/channel"
	"zobik.org/zobik/internal/role/contextstore"
	"zobik.org/zobik/internal/role/taskbroker"
	"zobik.org/zobik/internal/sidecar"
)

// Load returns the assembled schema.
var Load = sync.OnceValues(func() (*globalconfig.Schema, error) {
	return globalconfig.Assemble(
		taskbroker.ConfigSchema,
		contextstore.ConfigSchema,
		channel.ConfigSchema,
		sidecar.ConfigSchema,
	)
})
