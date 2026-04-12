// Package key provides helpers for building stable, collision-resistant
// deduplication keys from request components.
//
// All functions produce a lowercase hex-encoded SHA-256 digest, which is
// safe to use as a Redis or map key.
package key

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"
)

// FromParts returns SHA-256 digest of the concatenation of parts,
// each separated by a NUL byte to prevent cross-field collisions.
//
// Example – HTTP request:
//
//	k := key.FromParts(r.Method, r.URL.Path, r.URL.RawQuery, string(body))
func FromParts(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// FromJSON serialises v with encoding/json and returns a SHA-256 digest of
// the result. Useful when the request is already a typed struct.
//
// Example – gRPC request:
//
//	k := key.FromJSON(req)
func FromJSON(v any) string {
	b, _ := json.Marshal(v)
	return FromParts(string(b))
}

// FromMap builds a canonical string from m (keys sorted lexicographically)
// and returns its SHA-256 digest. Useful for query-parameter maps or
// header maps where order must not matter.
//
// Example:
//
//	k := key.FromMap(map[string]string{"user_id": "42", "action": "pay"})
func FromMap(m map[string]string) string {
	if len(m) == 0 {
		return FromParts()
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+m[k])
	}
	return FromParts(strings.Join(parts, "&"))
}
