// Package projection exposes the State Channel: a per-actor, monotonic
// snapshot of every gospore:"component"-tagged field.
//
// The Cell layer maintains the projection automatically via Convention D
// (SnapshotOf → handler → Diff → ApplyTo). Version increments only when
// fingerprint comparison detects a real state change — stateless handlers
// never advance Version, and stateful handlers that produce no delta
// also do not.
//
// Watch returns a Subscription[Update] that emits full_snapshot,
// delta, or gap_too_large records depending on subscriber state.
package projection

import (
	"fmt"
	"strings"
	"reflect"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/spore/binding"
)

// Snapshot is the current projection of an actor.
type Snapshot struct {
	ActorID id.ActorID
	// Fields maps each gospore:"component" field's name to its spore
	// view (decoded with the field's registered TypeDesc).
	Fields map[string]binding.ViewProjection
	// Version is the count of state changes observed (NOT the count of
	// stateful invocations). Monotonic and unique per actor.
	Version uint64
}

// Store is the App-wide projection lookup and subscription surface.
type Store interface {
	// Get returns the actor's current snapshot. (zero, false) if the
	// actor is unknown or has no projected components.
	Get(actorID id.ActorID) (Snapshot, bool)
	// GetField returns one named field's projection.
	GetField(actorID id.ActorID, name string) (binding.ViewProjection, bool)
	// Watch subscribes to state updates. Use SinceVersion to reconnect
	// without losing intervening updates.
	Watch(actorID id.ActorID, opts ...WatchOption) (actor.Subscription[Update], error)
	// Has reports whether the actor has any projected components.
	Has(actorID id.ActorID) bool
	// HasSubscribers is the Tier-0 hint the Cell consults before
	// computing fingerprints — when there are no subscribers, the
	// projection path is skipped entirely.
	HasSubscribers(actorID id.ActorID) bool
}

// WatchOption configures a Watch call.
type WatchOption func(*watchConfig)

// watchConfig is the internal aggregation of WatchOptions.
type watchConfig struct {
	sinceVersion uint64
	batchTimeout time.Duration
	hasSinceSet  bool
}

// SinceVersion makes Watch replay every Update with Version > v before
// switching to live mode. If v is older than the delta ring's oldest
// retained version, the first delivered Update is gap_too_large
// followed by a full_snapshot.
func SinceVersion(v uint64) WatchOption {
	return func(c *watchConfig) {
		c.sinceVersion = v
		c.hasSinceSet = true
	}
}

// WithBatchTimeout coalesces multiple state changes within d into one
// delta delivery. Used as a backpressure / chattiness control.
func WithBatchTimeout(d time.Duration) WatchOption {
	return func(c *watchConfig) { c.batchTimeout = d }
}

// UpdateKind discriminates the three possible Update shapes.
type UpdateKind string

const (
	// UpdateFullSnapshot delivers the entire current Snapshot. Used for
	// the first message a subscriber receives, or to recover after
	// gap_too_large.
	UpdateFullSnapshot UpdateKind = "full_snapshot"
	// UpdateDelta delivers only changed fields, with PrevVersion naming
	// the version this delta extends.
	UpdateDelta UpdateKind = "delta"
	// UpdateGapTooLarge signals the subscriber's since cursor is older
	// than the delta ring; the next Update will be a full_snapshot.
	UpdateGapTooLarge UpdateKind = "gap_too_large"
)

// Update is the unified stream element produced by Watch.
type Update struct {
	Kind    UpdateKind
	Version uint64
	// PrevVersion is set on UpdateDelta.
	PrevVersion uint64
	// Snapshot is set on UpdateFullSnapshot and UpdateGapTooLarge.
	Snapshot Snapshot
	// Delta is set on UpdateDelta — only the changed fields.
	Delta map[string]binding.ViewProjection
}

// FieldSnapshot is the leaf snapshot shape Convention D operates on before
// Cell wraps fields into schema-aware binding.ViewProjection values.
type FieldSnapshot map[string]any

// SnapshotOfSlots copies only the ComponentSlot-named fields from src into a
// FieldSnapshot. It is used by Cell.applyProjectionIfObserved after
// ScanComponents has determined which fields carry the `gospore:"component"`
// tag. Passing nil slots returns an empty snapshot (no error).
func SnapshotOfSlots(src any, slots []ComponentSlot) (FieldSnapshot, error) {
	if len(slots) == 0 {
		return FieldSnapshot{}, nil
	}
	if src == nil {
		return nil, fmt.Errorf("SnapshotOfSlots: nil")
	}
	v := reflect.ValueOf(src)
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil, fmt.Errorf("SnapshotOfSlots: nil pointer")
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("SnapshotOfSlots: want struct or *struct, got %s", v.Kind())
	}
	out := make(FieldSnapshot, len(slots))
	for _, slot := range slots {
		name := lowerFirst(slot.Name)
		field := v.FieldByName(slot.Name)
		if !field.IsValid() {
			continue
		}
		val, err := snapshotValue(field)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", slot.Name, err)
		}
		out[name] = val
	}
	return out, nil
}

// SnapshotOf copies exported fields from a struct or *struct into a detached
// FieldSnapshot using lowerCamel field names.
func SnapshotOf(src any) (FieldSnapshot, error) {
	if src == nil {
		return nil, fmt.Errorf("SnapshotOf: nil")
	}
	v := reflect.ValueOf(src)
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil, fmt.Errorf("SnapshotOf: nil pointer")
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("SnapshotOf: want struct or *struct, got %s", v.Kind())
	}
	return snapshotStruct(v)
}

func snapshotStruct(v reflect.Value) (FieldSnapshot, error) {
	t := v.Type()
	out := make(FieldSnapshot, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		name, ok := wireFieldName(f)
		if !ok {
			continue
		}
		val, err := snapshotValue(v.Field(i))
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", f.Name, err)
		}
		out[name] = val
	}
	return out, nil
}

func snapshotValue(v reflect.Value) (out any, err error) {
	// projection runs on the actor's ownerLoop but the snapshot dereferences
	// slices/maps that may be mutated by goroutines the actor spawned. A
	// concurrent append (slice grew past the captured len) or truncate (slice
	// shrank below i) would otherwise panic and — without this recover —
	// propagate up to cell.recoverAndDecide and trigger a workspace-wide Restart.
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("snapshot: panic: %v", r)
			out = nil
		}
	}()
	if !v.IsValid() {
		return nil, nil
	}
	for v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface {
		if v.IsNil() {
			return nil, nil
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Struct:
		return snapshotStruct(v)
	case reflect.Slice:
		// Capture the length once. The loop bound must be the snapshot taken
		// at allocation time, not v.Len() re-read each iteration — otherwise a
		// concurrent append grows v.Len() past len(out) and out[i] = item panics
		// with "index out of range".
		n := v.Len()
		outSlice := make([]any, n)
		for i := 0; i < n; i++ {
			// If the slice shrank concurrently, v.Index(i) would panic.
			// Re-checking bounds here keeps the snapshot best-effort instead
			// of relying solely on the top-level recover.
			if i >= v.Len() {
				outSlice[i] = nil
				continue
			}
			item, err := snapshotValue(v.Index(i))
			if err != nil {
				return nil, err
			}
			outSlice[i] = item
		}
		return outSlice, nil
	case reflect.Array:
		n := v.Len()
		outSlice := make([]any, n)
		for i := 0; i < n; i++ {
			item, err := snapshotValue(v.Index(i))
			if err != nil {
				return nil, err
			}
			outSlice[i] = item
		}
		return outSlice, nil
	case reflect.Bool:
		return v.Bool(), nil
	case reflect.String:
		return v.String(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return int(v.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return int(v.Uint()), nil
	case reflect.Float32, reflect.Float64:
		return v.Float(), nil
	default:
		if v.CanInterface() {
			return v.Interface(), nil
		}
		return nil, fmt.Errorf("unsupported kind %s", v.Kind())
	}
}

// Diff returns only the fields whose post value differs from pre.
func Diff(pre, post FieldSnapshot) FieldSnapshot {
	out := FieldSnapshot{}
	for k, postVal := range post {
		if !reflect.DeepEqual(pre[k], postVal) {
			out[k] = postVal
		}
	}
	return out
}

// ApplyTo writes a FieldSnapshot back onto a struct or *struct.
func ApplyTo(dst any, snap FieldSnapshot) error {
	v := reflect.ValueOf(dst)
	if v.Kind() != reflect.Pointer || v.IsNil() {
		return fmt.Errorf("ApplyTo: want non-nil pointer")
	}
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return fmt.Errorf("ApplyTo: nil pointer")
		}
		v = v.Elem()
	}
	return applyStruct(v, snap)
}

func applyStruct(v reflect.Value, snap FieldSnapshot) error {
	if v.Kind() != reflect.Struct {
		return fmt.Errorf("applyStruct: want struct, got %s", v.Kind())
	}
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		value, ok := snap[wireName(f)]
		if !ok {
			continue
		}
		if err := applyValue(v.Field(i), value); err != nil {
			return fmt.Errorf("field %s: %w", f.Name, err)
		}
	}
	return nil
}

func applyValue(dst reflect.Value, value any) error {
	if !dst.CanSet() {
		return fmt.Errorf("cannot set")
	}
	if dst.Kind() == reflect.Pointer {
		if value == nil {
			dst.Set(reflect.Zero(dst.Type()))
			return nil
		}
		if dst.IsNil() {
			dst.Set(reflect.New(dst.Type().Elem()))
		}
		return applyValue(dst.Elem(), value)
	}
	if dst.Kind() == reflect.Struct {
		snap, ok := value.(FieldSnapshot)
		if !ok {
			m, ok := value.(map[string]any)
			if !ok {
				return fmt.Errorf("want nested map, got %T", value)
			}
			snap = FieldSnapshot(m)
		}
		return applyStruct(dst, snap)
	}
	if dst.Kind() == reflect.Slice {
		raw, ok := value.([]any)
		if !ok {
			return fmt.Errorf("want slice, got %T", value)
		}
		slice := reflect.MakeSlice(dst.Type(), len(raw), len(raw))
		for i := range raw {
			if err := applyValue(slice.Index(i), raw[i]); err != nil {
				return fmt.Errorf("index %d: %w", i, err)
			}
		}
		dst.Set(slice)
		return nil
	}

	src := reflect.ValueOf(value)
	if src.IsValid() && src.Type().AssignableTo(dst.Type()) {
		dst.Set(src)
		return nil
	}

	switch dst.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch n := value.(type) {
		case int:
			dst.SetInt(int64(n))
			return nil
		case int64:
			dst.SetInt(n)
			return nil
		case float64:
			dst.SetInt(int64(n))
			return nil
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		switch n := value.(type) {
		case int:
			dst.SetUint(uint64(n))
			return nil
		case uint64:
			dst.SetUint(n)
			return nil
		case float64:
			dst.SetUint(uint64(n))
			return nil
		}
	case reflect.String:
		if s, ok := value.(string); ok {
			dst.SetString(s)
			return nil
		}
	case reflect.Bool:
		if b, ok := value.(bool); ok {
			dst.SetBool(b)
			return nil
		}
	case reflect.Float32, reflect.Float64:
		switch n := value.(type) {
		case float64:
			dst.SetFloat(n)
			return nil
		case int:
			dst.SetFloat(float64(n))
			return nil
		}
	}
	return fmt.Errorf("cannot apply %T to %s", value, dst.Type())
}

func lowerFirst(s string) string {
	if s == "" {
		return s
	}
	r, size := utf8.DecodeRuneInString(s)
	if r == utf8.RuneError && size == 0 {
		return s
	}
	return string(unicode.ToLower(r)) + s[size:]
}

// wireName derives the wire key for a struct field on the projection path.
// A `json` tag (the declared public name shared with the schema and the TS
// clients) is authoritative; lowerFirst(GoName) is the fallback for untagged
// fields. lowerFirst(GoName) alone is wrong for generator-emitted acronym
// fields (`ID` → `iD` while the schema/TS name is `Id`), which silently
// desynchronizes the frontend from the projection wire.
func wireName(f reflect.StructField) string {
	if tag, ok := f.Tag.Lookup("json"); ok {
		if idx := strings.Index(tag, ","); idx >= 0 {
			tag = tag[:idx]
		}
		if tag != "" && tag != "-" {
			return tag
		}
	}
	return lowerFirst(f.Name)
}

// wireFieldName is wireName with skip semantics: json:"-" and unexported
// fields are excluded from the snapshot entirely.
func wireFieldName(f reflect.StructField) (string, bool) {
	if !f.IsExported() {
		return "", false
	}
	if tag, ok := f.Tag.Lookup("json"); ok {
		if idx := strings.Index(tag, ","); idx >= 0 {
			tag = tag[:idx]
		}
		if tag == "-" {
			return "", false
		}
	}
	return wireName(f), true
}
