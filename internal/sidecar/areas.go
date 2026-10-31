package sidecar

import (
	"fmt"
	"log"
	"path/filepath"
	"regexp"
)

// The directories of the shared volume the Integration Sidecar creates and
// deletes: each task's work area and each admitted message's area
// (implementation §2.1, §3).
const (
	tasksDir   = "tasks"
	inboundDir = "inbound"
)

// areaName is a TaskID that names a directory: a single path element.
var areaName = regexp.MustCompile(`^[A-Za-z0-9_-][A-Za-z0-9._-]{0,127}$`)

// newArea creates the task's work area, empty. Both halves run under the UID
// the Spawner sets, so it is the Logic Container's (implementation §2.1, §3).
func (n *node) newArea(taskID string) error {
	if !areaName.MatchString(taskID) {
		return fmt.Errorf("the TaskID %q does not name a work area", taskID)
	}
	dir := filepath.Join(tasksDir, taskID)
	if err := n.areas.RemoveAll(dir); err != nil {
		return err
	}
	return n.areas.MkdirAll(dir, 0o700)
}

// removeArea deletes the work area of a task that closed.
func (n *node) removeArea(taskID string) {
	if !areaName.MatchString(taskID) {
		return
	}
	if err := n.areas.RemoveAll(filepath.Join(tasksDir, taskID)); err != nil {
		log.Printf("sidecar: deleting the work area of %s: %v", taskID, err)
	}
}

// clearAreas deletes every work area and message area, when the session is
// cut and at startup (implementation §3).
func (n *node) clearAreas() {
	for _, d := range []string{tasksDir, inboundDir} {
		if err := n.areas.RemoveAll(d); err != nil {
			log.Printf("sidecar: deleting %s: %v", d, err)
		}
	}
}
