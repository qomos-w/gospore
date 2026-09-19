package projection

import (
	"encoding/json"
	"reflect"
	"testing"
)

// wireTagged mirrors the generator output for schema acronyms: Go field `ID`
// (idiomatic all-caps) carrying the schema-declared public name in the json
// tag. The wire must speak the declared public name, not lowerFirst(GoName)
// (`iD`), which silently desyncs schema/TS consumers.
type wireTagged struct {
	ID      string `json:"Id"`
	URL     string `json:"Url,omitempty"`
	Plain   string // untagged: lowerFirst fallback
	Skipped string `json:"-"`
}

func TestSnapshotWireNamesJSONTagAuthoritative(t *testing.T) {
	v := wireTagged{ID: "a1", URL: "https://x", Plain: "p", Skipped: "s"}
	snap, err := snapshotValue(reflect.ValueOf(v))
	if err != nil {
		t.Fatal(err)
	}
	fs, ok := snap.(FieldSnapshot)
	if !ok {
		t.Fatalf("want FieldSnapshot, got %T", snap)
	}
	for _, want := range []string{"Id", "Url", "plain"} {
		if _, ok := fs[want]; !ok {
			t.Errorf("wire missing key %q (got %v)", want, keysOf(fs))
		}
	}
	if _, ok := fs["Skipped"]; ok {
		t.Error("json:\"-\" field must be excluded from the wire")
	}
	if _, ok := fs["iD"]; ok {
		t.Error("lowerFirst(GoName) leaked through for a tagged field")
	}

	// Round-trip: ApplyTo must read back by the same derivation.
	dst := wireTagged{}
	if err := applyValue(reflect.ValueOf(&dst).Elem(), snap); err != nil {
		t.Fatal(err)
	}
	if dst.ID != "a1" || dst.URL != "https://x" || dst.Plain != "p" {
		t.Errorf("ApplyTo round-trip lost fields: %+v", dst)
	}
}

func TestSnapshotWireNamesJSONCompatible(t *testing.T) {
	v := wireTagged{ID: "a1", URL: "u", Plain: "p", Skipped: "s"}
	snap, _ := snapshotValue(reflect.ValueOf(v))
	fs := snap.(FieldSnapshot)
	// For tagged fields (every generator-emitted component), the wire keys
	// must agree exactly with encoding/json's view: tags honored, "-" hidden.
	// Untagged fields deliberately keep the legacy lowerFirst convention
	// (Plain → `plain`) for hand-written components — a documented divergence.
	jm := map[string]any{}
	raw, _ := json.Marshal(v)
	_ = json.Unmarshal(raw, &jm)
	if _, ok := jm["Skipped"]; ok {
		t.Fatal("test fixture: json.Marshal unexpectedly serialized json:\"-\" field")
	}
	for _, k := range []string{"Id", "Url"} {
		if _, ok := fs[k]; !ok {
			t.Errorf("wire key %q missing from projection snapshot (got %v)", k, keysOf(fs))
		}
	}
	if _, ok := fs["plain"]; !ok {
		t.Error("untagged field must still publish under lowerFirst fallback (`plain`)")
	}
}

func keysOf(fs FieldSnapshot) []string {
	out := make([]string, 0, len(fs))
	for k := range fs {
		out = append(out, k)
	}
	return out
}
