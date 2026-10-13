package sidecar

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
)

// Range is a set of NRI versions: for each major, the highest minor. Within a
// major the Integration Sidecar widens what it does and never what it demands,
// so it serves every minor up to that one (architecture §3.6.1). It is the
// shape of runtime_interface_range (architecture §2.3).
type Range map[string]uint32

// Speaks is what this binary speaks: the zobik.nri.v1 service, up to its
// minor 0. The default of runtime_interface_range in the configuration schema
// is this value (implementation §6, The initial Global Configuration).
var Speaks = Range{"1": 0}

// Version is an NRI version, <major>.<minor>, as io.zobik.nri.version declares it
// (implementation §3, The interface version).
type Version struct {
	Major, Minor uint32
}

func (v Version) String() string { return fmt.Sprintf("%d.%d", v.Major, v.Minor) }

var versionForm = regexp.MustCompile(`^([1-9][0-9]{0,8})\.(0|[1-9][0-9]{0,8})$`)

// ParseVersion reads a version in its label form.
func ParseVersion(s string) (Version, error) {
	m := versionForm.FindStringSubmatch(s)
	if m == nil {
		return Version{}, fmt.Errorf("sidecar: NRI version %q is not <major>.<minor>", s)
	}
	major, _ := strconv.ParseUint(m[1], 10, 32) // the form bounds it
	minor, _ := strconv.ParseUint(m[2], 10, 32)
	return Version{uint32(major), uint32(minor)}, nil
}

// Fits reports whether r includes v.
func (r Range) Fits(v Version) bool {
	max, ok := r[strconv.FormatUint(uint64(v.Major), 10)]
	return ok && v.Minor <= max
}

// ReadRange reads runtime_interface_range from a value of the configuration.
func ReadRange(raw json.RawMessage) (Range, error) {
	var r Range
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, fmt.Errorf("sidecar: runtime_interface_range: %w", err)
	}
	return r, nil
}
