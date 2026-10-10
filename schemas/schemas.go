// Package schemas embeds the JSON Schemas the binary validates with, the same
// files each release publishes (implementation §10.2).
package schemas

import "embed"

//go:embed *.schema.json
var FS embed.FS
