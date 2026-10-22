// Package web is the panel: HTML and JavaScript without a build step, which
// the console serves (implementation §6, The operator console).
package web

import "embed"

// FS holds the panel's files.
//
//go:embed index.html app.js style.css
var FS embed.FS
