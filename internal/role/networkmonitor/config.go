package networkmonitor

import _ "embed"

// ConfigSchema is the fragment of the Global Configuration schema with the keys
// this package reads: those of the node lifecycle it measures (architecture
// §2.3) and those of its trigger logic (implementation §5).
//
//go:embed config.schema.json
var ConfigSchema []byte
