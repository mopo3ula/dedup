package dedup

import "time"

// Envelope is the transport-agnostic response container shared between
// the original handler and all waiting duplicates.
//
// Fields:
//   - Payload  – raw response body (e.g. serialised JSON, protobuf bytes).
//   - Meta     – optional key-value metadata (HTTP status, Content-Type, gRPC
//     code, or any application-specific data).
//   - Err      – non-empty when the original call produced an error that must
//     be replicated to all duplicates.
//   - CreatedAt – stamped automatically by [Deduplicator.Do]; do not set it
//     manually.
type Envelope struct {
	Payload   []byte
	Meta      map[string]string
	Err       string
	CreatedAt time.Time
}

// clone returns a deep copy so that callers cannot mutate the cached value.
func (e *Envelope) clone() *Envelope {
	if e == nil {
		return nil
	}
	cp := &Envelope{
		Payload:   append([]byte(nil), e.Payload...),
		Meta:      make(map[string]string, len(e.Meta)),
		Err:       e.Err,
		CreatedAt: e.CreatedAt,
	}
	for k, v := range e.Meta {
		cp.Meta[k] = v
	}
	return cp
}
