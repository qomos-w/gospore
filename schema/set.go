// Package schema is the App-scoped wire-type registry.
//
// Each App owns exactly one Set. The Set maps (namespace, uint64 id) to
// spore.TypeDesc; namespace + ID is the canonical wire key. Other Apps'
// types become accessible via Import; a Set Marshalled by App A can be
// Unmarshalled by App B and Imported there for cross-process decode.
package schema

import (
	"cmp"
	"encoding/json"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"sync"

	spore "github.com/qomos-w/spore/schema"
)

// Entry is a single registered schema record.
// Name is local-only (debug / logs / tooling) and never travels on the wire.
type Entry struct {
	Namespace string
	ID        uint64
	Name      string
	Desc      spore.TypeDesc
	Object    spore.ObjectDesc
}

// Reader is the read-only face of a Set, the form Context.Schemas() exposes.
// Handlers can look up types but cannot Register new ones nor Import.
type Reader interface {
	Namespace() string
	Lookup(id uint64) (Entry, bool)
	LookupByName(name string) (Entry, bool)
	LookupSchema(id uint64) (spore.TypeDesc, bool)
	LookupObject(id uint64) (spore.ObjectDesc, bool)
	Resolve(ns string, id uint64) (Entry, bool)
	ResolveByName(ns, name string) (Entry, bool)
	Namespaces() []string
	Len() int
}

// Set is the mutable schema registry held by an App.
type Set interface {
	Reader

	// Register binds (id, name, desc) within Set.Namespace().
	// id == 0 returns ErrSchemaIDZero; (id) collision within the namespace
	// returns ErrSchemaIDTaken; (name) collision returns ErrSchemaNameTaken.
	Register(id uint64, name string, desc spore.TypeDesc, object spore.ObjectDesc) error

	// RegisterAuto binds (name, desc) within Set.Namespace() and returns
	// the auto-assigned id. The id is chosen from `len(owner)+1` upward,
	// skipping any taken slots. Name collisions still return
	// ErrSchemaNameTaken; the registry stays read-only friendly otherwise.
	RegisterAuto(name string, desc spore.TypeDesc) (uint64, error)

	// Import takes a snapshot of foreign's namespace into this Set.
	// Re-importing the same namespace is idempotent when (id, desc) match
	// structurally; mismatches return ErrSchemaImportConflict. A Set may
	// not Import its own namespace.
	Import(foreign Set) error

	// Marshal serialises this Set's own namespace to bytes that another
	// Set can Unmarshal + Import. Only the owner namespace is emitted;
	// imported foreign namespaces are not included.
	Marshal() ([]byte, error)
}

// namespaceRE matches a single-segment namespace identifier per §4.6.
var namespaceRE = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// validateNamespace enforces the §4.6 namespace rules: regex match plus
// the two reserved prefixes ("app", "_gospore_").
func validateNamespace(ns string) error {
	if !namespaceRE.MatchString(ns) {
		return ErrSchemaInvalidNamespace
	}
	if ns == "app" || strings.HasPrefix(ns, "_gospore_") {
		return ErrSchemaInvalidNamespace
	}
	return nil
}

// New constructs a fresh, empty Set whose owner namespace is ns.
// ns must match `^[a-z][a-z0-9_]*$` and must not be a reserved name.
func New(ns string) (Set, error) {
	if err := validateNamespace(ns); err != nil {
		return nil, err
	}
	return newSet(ns, false), nil
}

// newSet is the package-internal constructor used by both New and Unmarshal.
// readonly == true blocks Register and Import.
func newSet(ns string, readonly bool) *set {
	return &set{
		namespace:     ns,
		readonly:      readonly,
		owner:         map[uint64]Entry{},
		ownerByName:   map[string]Entry{},
		imports:       map[string]map[uint64]Entry{},
		importsByName: map[string]map[string]Entry{},
	}
}

// set is the canonical Set implementation. Concurrency-safe.
type set struct {
	mu            sync.RWMutex
	namespace     string
	readonly      bool
	owner         map[uint64]Entry
	ownerByName   map[string]Entry
	imports       map[string]map[uint64]Entry
	importsByName map[string]map[string]Entry
}

func (s *set) Namespace() string {
	return s.namespace
}

func (s *set) Register(id uint64, name string, desc spore.TypeDesc, object spore.ObjectDesc) error {
	if s.readonly {
		return ErrSchemaReadOnly
	}
	if id == 0 {
		return ErrSchemaIDZero
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, taken := s.owner[id]; taken {
		return ErrSchemaIDTaken
	}
	if _, taken := s.ownerByName[name]; taken {
		return ErrSchemaNameTaken
	}
	e := Entry{Namespace: s.namespace, ID: id, Name: name, Desc: desc, Object: object}
	s.owner[id] = e
	s.ownerByName[name] = e
	return nil
}

// RegisterAuto allocates the next free uint64 id and binds (name, desc) to
// it. Allocation starts at BuiltinUserStart (128) so the universal builtin
// region 0-127 stays untouched; id == 0 is skipped to keep the zero-id
// reservation intact.
//
// RegisterAuto is idempotent on structural equality: when a prior entry
// already bears name and its desc deep-equals the incoming desc, the
// previously assigned id is returned with no mutation. A name re-use with
// a divergent desc still returns ErrSchemaNameTaken — handlers that share
// a return type therefore share a schema id without explicit caller dedup.
//
// If desc itself maps to a builtin (scalar, scalar-only container, void)
// the builtin id is returned without touching the registry; this keeps
// callers from minting a per-namespace id for a universal type.
func (s *set) RegisterAuto(name string, desc spore.TypeDesc) (uint64, error) {
	if s.readonly {
		return 0, ErrSchemaReadOnly
	}
	if id, ok := BuiltinIDFor(desc); ok {
		return id, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.ownerByName[name]; ok {
		if descMatch(existing.Desc, desc) {
			return existing.ID, nil
		}
		return 0, ErrSchemaNameTaken
	}
	id := BuiltinUserStart + uint64(len(s.owner))
	for {
		if id == 0 {
			return 0, ErrSchemaIDTaken
		}
		if _, taken := s.owner[id]; !taken {
			break
		}
		id++
	}
	e := Entry{Namespace: s.namespace, ID: id, Name: name, Desc: desc}
	s.owner[id] = e
	s.ownerByName[name] = e
	return id, nil
}

// descMatch reports whether two TypeDescs describe the same type.
// ClassID is ignored because runtime reflection cannot produce it —
// only manifest import sets it. Similarly, Name may differ ("struct"
// from reflection vs the real name from manifest) so only Kind and
// ClassName are compared for struct types.
func descMatch(a, b spore.TypeDesc) bool {
	if a.Kind != b.Kind {
		return false
	}
	if a.Kind == spore.TypeKindStruct {
		return a.ClassName == b.ClassName
	}
	return reflect.DeepEqual(a, b)
}

func (s *set) Lookup(id uint64) (Entry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.owner[id]
	return e, ok
}

func (s *set) LookupByName(name string) (Entry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.ownerByName[name]
	return e, ok
}

// LookupSchema is the codec.SchemaResolver shape: it returns the TypeDesc
// bound to id within the owner namespace, ignoring imports. Decoding paths
// use this to materialise typed Go values from raw Frame bytes.
func (s *set) LookupSchema(id uint64) (spore.TypeDesc, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if e, ok := s.owner[id]; ok {
		return e.Desc, true
	}
	return spore.TypeDesc{}, false
}

// LookupObject returns the ObjectDesc (field layout) for a registered schema.
// Returns (zero, false) when the ID is not found or the entry has no object.
func (s *set) LookupObject(id uint64) (spore.ObjectDesc, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if e, ok := s.owner[id]; ok && e.Object.Name != "" {
		return e.Object, true
	}
	return spore.ObjectDesc{}, false
}

func (s *set) Resolve(ns string, id uint64) (Entry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if ns == s.namespace {
		e, ok := s.owner[id]
		return e, ok
	}
	if m, ok := s.imports[ns]; ok {
		e, ok2 := m[id]
		return e, ok2
	}
	return Entry{}, false
}

func (s *set) ResolveByName(ns, name string) (Entry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if ns == s.namespace {
		e, ok := s.ownerByName[name]
		return e, ok
	}
	if m, ok := s.importsByName[ns]; ok {
		e, ok2 := m[name]
		return e, ok2
	}
	return Entry{}, false
}

func (s *set) Namespaces() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, 1+len(s.imports))
	out = append(out, s.namespace)
	for ns := range s.imports {
		out = append(out, ns)
	}
	slices.Sort(out)
	return out
}

// Len reports the number of entries registered in the owner namespace.
// Imports are not counted; the value is primarily diagnostic.
func (s *set) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.owner)
}

func (s *set) Import(foreign Set) error {
	if s.readonly {
		return ErrSchemaReadOnly
	}
	if foreign == nil {
		return ErrSchemaImportConflict
	}
	if foreign.Namespace() == s.namespace {
		return ErrSchemaImportConflict
	}
	fs, ok := foreign.(*set)
	if !ok {
		return ErrSchemaImportConflict
	}
	fs.mu.RLock()
	foreignNs := fs.namespace
	snapshot := make([]Entry, 0, len(fs.owner))
	for _, e := range fs.owner {
		snapshot = append(snapshot, e)
	}
	fs.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	existing := s.imports[foreignNs]
	existingByName := s.importsByName[foreignNs]
	if existing == nil {
		existing = map[uint64]Entry{}
		existingByName = map[string]Entry{}
	}
	for _, e := range snapshot {
		if prev, found := existing[e.ID]; found {
			if !entriesStructurallyEqual(prev, e) {
				return ErrSchemaImportConflict
			}
			continue
		}
		if prev, found := existingByName[e.Name]; found {
			if !entriesStructurallyEqual(prev, e) {
				return ErrSchemaImportConflict
			}
			continue
		}
		existing[e.ID] = e
		existingByName[e.Name] = e
	}
	s.imports[foreignNs] = existing
	s.importsByName[foreignNs] = existingByName
	return nil
}

// entriesStructurallyEqual compares two Entry values for cross-Import
// idempotency. Namespace and ID must match exactly; Desc is compared
// deeply because it carries pointer-typed sub-fields.
func entriesStructurallyEqual(a, b Entry) bool {
	if a.Namespace != b.Namespace || a.ID != b.ID || a.Name != b.Name {
		return false
	}
	return reflect.DeepEqual(a.Desc, b.Desc)
}

// marshalledSet is the on-the-wire shape produced by Set.Marshal and
// consumed by Unmarshal. Entries carry only the owner namespace.
type marshalledSet struct {
	Namespace string  `json:"ns"`
	Entries   []Entry `json:"entries"`
}

func (s *set) Marshal() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ms := marshalledSet{Namespace: s.namespace, Entries: make([]Entry, 0, len(s.owner))}
	for _, e := range s.owner {
		ms.Entries = append(ms.Entries, e)
	}
	slices.SortFunc(ms.Entries, func(a, b Entry) int { return cmp.Compare(a.ID, b.ID) })
	return json.Marshal(ms)
}

// Unmarshal decodes bytes produced by Set.Marshal into a read-only Set.
// The returned Set carries one namespace and is suitable as the argument
// to another Set's Import.
func Unmarshal(data []byte) (Set, error) {
	var ms marshalledSet
	if err := json.Unmarshal(data, &ms); err != nil {
		return nil, err
	}
	if !namespaceRE.MatchString(ms.Namespace) {
		return nil, ErrSchemaInvalidNamespace
	}
	out := newSet(ms.Namespace, true)
	for _, e := range ms.Entries {
		if e.ID == 0 {
			return nil, ErrSchemaIDZero
		}
		if _, taken := out.owner[e.ID]; taken {
			return nil, ErrSchemaIDTaken
		}
		if _, taken := out.ownerByName[e.Name]; taken {
			return nil, ErrSchemaNameTaken
		}
		e.Namespace = ms.Namespace
		out.owner[e.ID] = e
		out.ownerByName[e.Name] = e
	}
	return out, nil
}
