//go:build dev

package main

import "zobik.org/zobik/internal/bus"

// A development binary registers zobik tap's scope; a release binary never does.
func init() {
	devScopes = append(devScopes, bus.TapScope)
}
