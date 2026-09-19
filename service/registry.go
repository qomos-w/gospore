// Package service is the App-scoped name → actor table populated by
// ctx.Expose. Service names match `^[a-z][a-z0-9_-]*$` (single segment,
// no dots) so they are visually distinct from dotted Call IDs.
//
// Lifecycle: a service Register happens when the actor calls
// ctx.Expose during OnStart; Unregister happens automatically after
// the actor's OnStop. Actors do not need to clean up Expose calls.
package service

import (
	"errors"
	"regexp"
	"sync"

	"github.com/qomos-w/gospore/internal/collections"
	"github.com/qomos-w/gospore/ref"
)

// Registry is the service-name table for one App.
type Registry interface {
	// Register binds name to ref. ErrServiceNameTaken is returned if
	// name is already bound. ErrInvalidServiceName is returned if name
	// fails the format check.
	Register(name string, ref ref.Ref) error
	// Unregister removes name. No error if name was not bound.
	Unregister(name string)
	// Lookup resolves name to its current Ref.
	Lookup(name string) (ref.Ref, bool)
	// Names returns a snapshot of currently-registered service names,
	// sorted ascending for stable iteration.
	Names() []string
}

// ErrServiceNameTaken is returned by Register when the name is bound
// to another actor.
var ErrServiceNameTaken = errors.New("gospore/service: name already taken")

// ErrInvalidServiceName is returned when a name fails the
// `^[a-z][a-z0-9_-]*$` check (callers see this surfaced as
// gospore.service.invalid_name).
var ErrInvalidServiceName = errors.New("gospore/service: invalid name")

// ErrScopedConflict is returned by RegisterScopedService when a scoped
// service name conflicts with an existing global or scoped service in the
// same subtree.
var ErrScopedConflict = errors.New("gospore/service: scoped service conflicts with existing service")

// nameRE enforces single-segment names: leading lowercase letter, then
// any number of lowercase letters / digits / underscore / hyphen. No
// dots — dots distinguish Call IDs from service names.
var nameRE = regexp.MustCompile(`^[a-z][a-z0-9_-]*$`)

// ValidName reports whether name is a syntactically valid service name.
// Exported so the actor / cell layer can reject ctx.Expose calls
// upfront with a uniform error code.
func ValidName(name string) bool {
	return nameRE.MatchString(name)
}

type registry struct {
	mu sync.RWMutex
	m  map[string]ref.Ref
}

// New constructs an empty Registry.
func New() Registry {
	return &registry{m: make(map[string]ref.Ref)}
}

func (r *registry) Register(name string, ref ref.Ref) error {
	if !ValidName(name) {
		return ErrInvalidServiceName
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.m[name]; exists {
		return ErrServiceNameTaken
	}
	r.m[name] = ref
	return nil
}

func (r *registry) Unregister(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.m, name)
}

func (r *registry) Lookup(name string) (ref.Ref, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.m[name]
	return v, ok
}

func (r *registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return collections.SortedKeys(r.m)
}
