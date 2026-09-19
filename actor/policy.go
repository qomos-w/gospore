package actor

import (
	"strings"
	"sync/atomic"
)

// Policy is a single runtime permission rule.
// The scope is matched against the CallID prefix chain (most specific
// to least specific). The role is matched against Role.Effective().
type Policy struct {
	Scope string // e.g. "myxos.llm" or "myxos.llm.complete"
	Role  Role   // empty = match any role
	Allow bool   // true = allow, false = deny
}

// PolicyStore is the runtime policy evaluation interface.
// Implementations must be safe for concurrent use without locking
// during Evaluate.
type PolicyStore interface {
	// Evaluate reports whether the caller with the given role is allowed
	// to access the scope derived from callID.
	Evaluate(role Role, scope string) (allow bool, found bool)

	// Reload atomically replaces the current policy set.
	Reload(policies []Policy) error

	// Version returns the monotonically increasing version number of the
	// current policy snapshot. Useful for audit logging and cache invalidation.
	Version() uint64
}

// defaultPolicyStore is the default PolicyStore implementation using
// atomic snapshots. Evaluate is lock-free.
type defaultPolicyStore struct {
	atomic.Value // stores *policySnapshot
	version      atomic.Uint64
}

// policySnapshot is an immutable snapshot of policies.
type policySnapshot struct {
	version uint64
	// exact maps "scope:role" → allow value for exact matches.
	exact map[string]bool
	// prefix maps each scope prefix to the most specific rule for that scope.
	// The scope chain traversal (exact → parent → ...) is handled by Evaluate.
	scopeRules map[string]*policyRuleNode
}

// policyRuleNode holds the best matching rule for a given scope,
// keyed by role (or "" for wildcard).
type policyRuleNode struct {
	role       Role
	allow      bool
	explicit   bool // true if this is a direct match, not inherited
	parent     *policyRuleNode
}

// NewPolicyStore returns a default PolicyStore with a deny-all snapshot.
func NewPolicyStore() PolicyStore {
	s := &defaultPolicyStore{}
	s.Store(&policySnapshot{version: 0})
	return s
}

// Evaluate checks the policy for the given role and scope.
// Scope matching walks from the exact scope up to broader prefixes.
// Role matching walks from the exact role down the Effective chain.
// The first explicit match wins. If no match is found, (false, false)
// is returned, meaning the caller should default to deny.
func (s *defaultPolicyStore) Evaluate(role Role, scope string) (bool, bool) {
	snap := s.Load().(*policySnapshot)

	// Try exact scope:role first
	key := scopeKey(scope, string(role))
	if v, ok := snap.exact[key]; ok {
		return v, true
	}

	// Walk role effective chain for exact scope
	for _, r := range role.Effective() {
		key := scopeKey(scope, string(r))
		if v, ok := snap.exact[key]; ok {
			return v, true
		}
	}

	// Try wildcard role (empty) for exact scope
	key = scopeKey(scope, "")
	if v, ok := snap.exact[key]; ok {
		return v, true
	}

	// Walk scope chain (parent prefixes)
	parts := strings.Split(scope, ".")
	for i := len(parts) - 1; i >= 0; i-- {
		parentScope := strings.Join(parts[:i], ".")
		if parentScope == "" {
			continue
		}

		// Try exact role match on parent scope
		key := scopeKey(parentScope, string(role))
		if v, ok := snap.exact[key]; ok {
			return v, true
		}

		// Walk role effective chain on parent scope
		for _, r := range role.Effective() {
			key := scopeKey(parentScope, string(r))
			if v, ok := snap.exact[key]; ok {
				return v, true
			}
		}

		// Try wildcard role on parent scope
		key = scopeKey(parentScope, "")
		if v, ok := snap.exact[key]; ok {
			return v, true
		}
	}

	return false, false
}

func (s *defaultPolicyStore) Reload(policies []Policy) error {
	snap := buildSnapshot(policies, s.version.Add(1))
	s.Store(snap)
	return nil
}

func (s *defaultPolicyStore) Version() uint64 {
	return s.Load().(*policySnapshot).version
}

// scopeKey creates the exact lookup key for a scope+role pair.
func scopeKey(scope, role string) string {
	if role == "" {
		return scope + ":*"
	}
	return scope + ":" + role
}

// buildSnapshot creates an immutable snapshot from a policy list.
func buildSnapshot(policies []Policy, version uint64) *policySnapshot {
	snap := &policySnapshot{
		version: version,
		exact:   make(map[string]bool, len(policies)),
	}
	for _, p := range policies {
		key := scopeKey(p.Scope, string(p.Role))
		snap.exact[key] = p.Allow
	}
	return snap
}

// scopeChain returns the scope lookup chain for a callID,
// from most specific to least specific.
// e.g. "myxos.llm.complete" → ["myxos.llm.complete", "myxos.llm", "myxos"]
// PolicyCheckResult is the return value for script-side policy checks.
// Spore scripts cannot consume Go multi-return values directly, so
// Evaluate's (bool, bool) is packed into a struct for the binding layer.
type PolicyCheckResult struct {
	Allow bool `json:"allow"`
	Found bool `json:"found"`
}

func scopeChain(callID string) []string {
	parts := strings.Split(callID, ".")
	if len(parts) < 2 {
		return []string{callID}
	}
	result := make([]string, len(parts))
	for i := 0; i < len(parts); i++ {
		result[i] = strings.Join(parts[:len(parts)-i], ".")
	}
	return result
}
