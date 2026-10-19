package embedding

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"math"
)

// The wire form of an embedding: base64 over little-endian float32
// (implementation §1.2.1). The task_embedding and the capability_embedding of a
// credential share it (implementation §9.2).

// Room is what a task_embedding adds to an event: a 4096-dimension vector in
// base64, with its model_id.
const Room = 24 << 10

// Encode returns v in its wire form.
func Encode(v []float32) string {
	b := make([]byte, 4*len(v))
	for i, x := range v {
		binary.LittleEndian.PutUint32(b[4*i:], math.Float32bits(x))
	}
	return base64.StdEncoding.EncodeToString(b)
}

// ErrEncoding is a string that is not an embedding in its wire form.
var ErrEncoding = errors.New("embedding: not base64 over little-endian float32")

// Decode reads an embedding from its wire form.
func Decode(s string) ([]float32, error) {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil || len(b) == 0 || len(b)%4 != 0 {
		return nil, ErrEncoding
	}
	v := make([]float32, len(b)/4)
	for i := range v {
		v[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[4*i:]))
	}
	return v, nil
}

// ErrDimension is a pair of vectors of different length.
var ErrDimension = errors.New("embedding: vectors of different dimension")

// Similarity is the cosine of two normalized vectors, which reduces to their dot
// product (architecture §3.7.5, step 4).
func Similarity(a, b []float32) (float64, error) {
	if len(a) != len(b) {
		return 0, ErrDimension
	}
	var dot float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
	}
	return dot, nil
}

// Round expresses s in units of 10^-decimals, which is the resolution the
// network declares it distinguishes similarities at: whoever compares a
// similarity against a floor, or two similarities, compares them rounded
// (architecture §3.7.5, step 4).
func Round(s float64, decimals int) int64 {
	return int64(math.Round(s * math.Pow10(decimals)))
}

// Scaled is a rounded similarity back in its own units.
func Scaled(score int64, decimals int) float64 {
	return float64(score) / math.Pow10(decimals)
}
