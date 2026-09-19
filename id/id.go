// Package id provides canonical identifiers used across gospore.
//
// ActorID wraps spore's identity.CanonicalID; gospore owns the runtime
// generation lifecycle while spore owns the binary layout.
// CorID is the per-call correlation identifier (uint64, App-global).
// Identity is the external caller identity attached at transport handshake.
package id

import "github.com/qomos-w/spore/identity"

// ActorID is the canonical 128-bit gospore identifier for an actor.
// It wraps spore identity.CanonicalID so gospore does not duplicate
// snowflake layout. Comparable; the zero value is the null identity.
type ActorID struct{ inner identity.CanonicalID }

// From wraps a spore CanonicalID as an ActorID.
func From(c identity.CanonicalID) ActorID {
	return ActorID{inner: c}
}

// Parse decodes a 32-character lowercase hex string into an ActorID.
func Parse(s string) (ActorID, error) {
	cid, err := identity.ParseCanonicalID(s)
	if err != nil {
		return ActorID{}, err
	}
	return From(cid), nil
}

// Canonical returns the underlying spore CanonicalID.
func (a ActorID) Canonical() identity.CanonicalID {
	return a.inner
}

// IsZero reports whether this is the null ActorID.
func (a ActorID) IsZero() bool {
	return a.inner.IsZero()
}

// String returns the canonical 32-character lowercase hex form.
func (a ActorID) String() string {
	return a.inner.String()
}

// TimestampMS returns the 48-bit millisecond timestamp segment.
func (a ActorID) TimestampMS() uint64 {
	return a.inner.TimestampMS()
}

// RuntimeSlot returns the 16-bit origin runtime slot segment.
func (a ActorID) RuntimeSlot() uint16 {
	return a.inner.RuntimeSlot()
}

// Incarnation returns the 16-bit runtime incarnation segment.
func (a ActorID) Incarnation() uint16 {
	return a.inner.Incarnation()
}

// Sequence returns the 48-bit local sequence segment.
func (a ActorID) Sequence() uint64 {
	return a.inner.Sequence()
}
