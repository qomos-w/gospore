package service

// This file owns the cross-registry naming policy: which error (if
// any) applies when a service name is registered into the scoped or
// global registry. The checks depend on registry state that lives in
// the tree package; the DECISION of which sentinel applies is defined
// here, next to the sentinels and their diag mappings, so the policy
// has one authoritative home.

// ScopedRegistrationError decides the outcome of registering name as a
// scoped service of one owner:
//
//   - globalTaken: any actor already exposes the name as a global
//     service. A name is either global or scoped, never both →
//     ErrScopedConflict.
//   - ownerTaken: the owner already registered the same scoped name →
//     ErrScopedConflict.
//
// Both false → nil (registration may proceed).
func ScopedRegistrationError(globalTaken, ownerTaken bool) error {
	if globalTaken || ownerTaken {
		return ErrScopedConflict
	}
	return nil
}

// GlobalRegistrationError decides the outcome of registering name as a
// global service of owner:
//
//   - takenBySelf: owner already exposes the name globally → nil
//     (idempotent re-registration).
//   - takenByOther: a different actor exposes the name globally →
//     ErrServiceNameTaken.
//   - scopedTaken: any actor exposes the name as a scoped service →
//     ErrScopedConflict (the either-global-or-scoped rule, other side).
//
// takenBySelf takes precedence over takenByOther (a lookup that finds
// the name reports at most one global owner).
func GlobalRegistrationError(takenBySelf, takenByOther, scopedTaken bool) error {
	if takenBySelf {
		return nil
	}
	if takenByOther {
		return ErrServiceNameTaken
	}
	if scopedTaken {
		return ErrScopedConflict
	}
	return nil
}
