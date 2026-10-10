package contextstore

import _ "embed"

// ConfigSchema is the fragment of the Global Configuration schema with the keys
// this package reads (implementation §1.2.5).
//
//go:embed config.schema.json
var ConfigSchema []byte
