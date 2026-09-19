package schema

import (
	"encoding/json"
	"testing"
)

func TestProjectionDeclMarshal(t *testing.T) {
	p := ProjectionDecl{
		Namespace:  "test",
		ActorPath:  "/test",
		Component:  "Field",
		SchemaID:   1,
		Mode:       "full",
		Visibility: "public",
	}
	data, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["visibility"] != "public" {
		t.Errorf("visibility = %v, want public", got["visibility"])
	}
}

func TestProjectionDeclMarshalOmitEmpty(t *testing.T) {
	p := ProjectionDecl{
		Namespace: "test",
		Component: "Field",
		SchemaID:  1,
		Mode:      "full",
		// Visibility omitted → should not appear in JSON
	}
	data, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := got["visibility"]; ok {
		t.Error("empty visibility should be omitted from JSON")
	}
}

func TestEventDeclMarshal(t *testing.T) {
	e := EventDecl{
		Namespace:  "test",
		Kind:       "turn",
		SchemaID:   2,
		Visibility: "admin",
	}
	data, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["visibility"] != "admin" {
		t.Errorf("visibility = %v, want admin", got["visibility"])
	}
}

func TestEventDeclMarshalOmitEmpty(t *testing.T) {
	e := EventDecl{
		Namespace: "test",
		Kind:      "turn",
		SchemaID:  2,
		// Visibility omitted
	}
	data, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := got["visibility"]; ok {
		t.Error("empty visibility should be omitted from JSON")
	}
}

func TestGosporeManifestRoundTrip(t *testing.T) {
	gm := GosporeManifest{
		Projections: []ProjectionDecl{
			{Namespace: "ns", ActorPath: "/ns", Component: "X", SchemaID: 1, Mode: "full", Visibility: "public"},
		},
		Events: []EventDecl{
			{Namespace: "ns", Kind: "k", SchemaID: 2, Visibility: "internal"},
		},
	}
	data, err := json.Marshal(gm)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var gm2 GosporeManifest
	if err := json.Unmarshal(data, &gm2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(gm2.Projections) != 1 || gm2.Projections[0].Visibility != "public" {
		t.Error("ProjectionDecl round-trip failed")
	}
	if len(gm2.Events) != 1 || gm2.Events[0].Visibility != "internal" {
		t.Error("EventDecl round-trip failed")
	}
}
