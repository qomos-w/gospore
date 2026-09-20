package app

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/qomos-w/gospore/actor"
	"github.com/qomos-w/gospore/id"
	"github.com/qomos-w/gospore/internal/cell"
	"github.com/qomos-w/gospore/internal/handler"
	"github.com/qomos-w/gospore/invoke"
	"github.com/qomos-w/gospore/ref"
	sporeIdentity "github.com/qomos-w/spore/identity"
	spore "github.com/qomos-w/spore/schema"
)

// exportTestActor is a minimal actor for export tests.
type exportTestActor struct {
	actor.Host
}

func (a *exportTestActor) Type() string { return "test" }

var _ actor.Actor = &exportTestActor{}

// fakeRefForExport is a minimal ref.Ref for export tests.
type fakeRefForExport struct {
	id id.ActorID
}

func (f *fakeRefForExport) ID() id.ActorID  { return f.id }
func (f *fakeRefForExport) Service() (string, bool) { return "", false }
func (f *fakeRefForExport) Invoke(_ context.Context, _ string, _ any, _ ...map[string]string) *invoke.Call { return nil }

var _ ref.Ref = (*fakeRefForExport)(nil)

func newFakeRef(raw byte) *fakeRefForExport {
	var buf [16]byte
	buf[0] = raw
	return &fakeRefForExport{id: id.From(sporeIdentity.CanonicalID(buf))}
}

func TestBuildGosporeManifest_CallableVisibility(t *testing.T) {
	self := newFakeRef(0x01)

	fn := func(_ actor.Context, req struct{ Name string }) (struct{ Result string }, error) {
		return struct{ Result string }{}, nil
	}

	ht := handler.NewTable()
	if err := ht.Register("test.public", fn, actor.Public()); err != nil {
		t.Fatalf("register public: %v", err)
	}
	if err := ht.Register("test.internal", fn, actor.Internal()); err != nil {
		t.Fatalf("register internal: %v", err)
	}

	c := cell.New(cell.Config{
		Self:     self,
		Actor:    &exportTestActor{},
		Handlers: ht,
	})

	cells := map[id.ActorID]*cell.Cell{self.ID(): c}
	gm, err := buildGosporeManifest(cells, nil)
	if err != nil {
		t.Fatalf("buildGosporeManifest: %v", err)
	}

	vis := map[string]string{}
	for _, mc := range gm.Callables {
		vis[mc.Name] = mc.Visibility
	}

	if vis["public"] != "public" {
		t.Errorf("public callable visibility = %q, want public", vis["public"])
	}
	if vis["internal"] != "internal" {
		t.Errorf("internal callable visibility = %q, want internal", vis["internal"])
	}
}

func TestBuildGosporeManifest_EventVisibility(t *testing.T) {
	self := newFakeRef(0x01)

	c := cell.New(cell.Config{Self: self, Actor: &exportTestActor{}})

	ctx := cell.NewStartContextForTest(c)
	if err := ctx.RegisterLoop("custom.events", actor.ModeStateful); err != nil {
		t.Fatalf("register custom loop: %v", err)
	}
	if err := ctx.RegisterEventKind("publicEvent", struct{ X int }{}, actor.Public(), actor.WithLoop("custom.events")); err != nil {
		t.Fatalf("register public event: %v", err)
	}
	if err := ctx.RegisterEventKind("internalEvent", struct{ Y int }{}, actor.Internal()); err != nil {
		t.Fatalf("register internal event: %v", err)
	}

	cells := map[id.ActorID]*cell.Cell{self.ID(): c}
	gm, err := buildGosporeManifest(cells, nil)
	if err != nil {
		t.Fatalf("buildGosporeManifest: %v", err)
	}

	vis := map[string]string{}
	loops := map[string]string{}
	for _, e := range gm.Events {
		vis[e.Kind] = e.Visibility
		loops[e.Kind] = e.Loop
	}

	if vis["publicEvent"] != "public" {
		t.Errorf("public event visibility = %q, want public", vis["publicEvent"])
	}
	if vis["internalEvent"] != "internal" {
		t.Errorf("internal event visibility = %q, want internal", vis["internalEvent"])
	}
	if loops["publicEvent"] != "custom.events" {
		t.Errorf("public event loop = %q, want custom.events", loops["publicEvent"])
	}
	if loops["internalEvent"] != actor.DefaultLoopOwner {
		t.Errorf("internal event loop = %q, want %q", loops["internalEvent"], actor.DefaultLoopOwner)
	}
}

func TestBuildGosporeManifest_SchemaVisibilityInheritance(t *testing.T) {
	self := newFakeRef(0x01)

	type SharedReq struct{ Name string }

	publicFn := func(_ actor.Context, req SharedReq) (struct{ Result string }, error) {
		return struct{ Result string }{}, nil
	}
	internalFn := func(_ actor.Context, req SharedReq) (struct{ Result string }, error) {
		return struct{ Result string }{}, nil
	}

	ht := handler.NewTable()
	if err := ht.Register("test.public", publicFn, actor.Public()); err != nil {
		t.Fatalf("register public: %v", err)
	}
	if err := ht.Register("test.internal", internalFn, actor.Internal()); err != nil {
		t.Fatalf("register internal: %v", err)
	}

	c := cell.New(cell.Config{
		Self:     self,
		Actor:    &exportTestActor{},
		Handlers: ht,
	})

	cells := map[id.ActorID]*cell.Cell{self.ID(): c}
	gm, err := buildGosporeManifest(cells, nil)
	if err != nil {
		t.Fatalf("buildGosporeManifest: %v", err)
	}

	var found bool
	for _, s := range gm.Schemas {
		if s.Name == "SharedReq" {
			found = true
			if s.Visibility != "public" {
				t.Errorf("SharedReq schema visibility = %q, want public", s.Visibility)
			}
			break
		}
	}
	if !found {
		t.Error("SharedReq schema not found in manifest")
	}
}

func TestBuildGosporeManifest_DefaultVisibilityIsInternal(t *testing.T) {
	self := newFakeRef(0x01)

	fn := func(_ actor.Context, req struct{ Name string }) (struct{ Result string }, error) {
		return struct{ Result string }{}, nil
	}

	ht := handler.NewTable()
	if err := ht.Register("test.handler", fn); err != nil {
		t.Fatalf("register: %v", err)
	}

	c := cell.New(cell.Config{
		Self:     self,
		Actor:    &exportTestActor{},
		Handlers: ht,
	})

	cells := map[id.ActorID]*cell.Cell{self.ID(): c}
	gm, err := buildGosporeManifest(cells, nil)
	if err != nil {
		t.Fatalf("buildGosporeManifest: %v", err)
	}

	if len(gm.Callables) != 1 {
		t.Fatalf("len(callables) = %d, want 1", len(gm.Callables))
	}
	if gm.Callables[0].Visibility != "internal" {
		t.Errorf("default callable visibility = %q, want internal", gm.Callables[0].Visibility)
	}
}

func TestBuildGosporeManifest_EmptyCells(t *testing.T) {
	gm, err := buildGosporeManifest(nil, nil)
	if err != nil {
		t.Fatalf("buildGosporeManifest(nil, nil): %v", err)
	}
	if len(gm.Schemas) != 0 {
		t.Errorf("len(schemas) = %d, want 0", len(gm.Schemas))
	}
	if len(gm.Callables) != 0 {
		t.Errorf("len(callables) = %d, want 0", len(gm.Callables))
	}
}

func TestCallableTypes(t *testing.T) {
	unary := func(_ actor.Context, req struct{ Name string }) (struct{ Result string }, error) {
		return struct{ Result string }{}, nil
	}
	req, final, chunk, isStreaming := callableTypes(unary, false)
	if req == nil {
		t.Error("unary req should not be nil")
	}
	if final == nil {
		t.Error("unary final should not be nil")
	}
	if chunk != nil {
		t.Error("unary chunk should be nil")
	}
	if isStreaming {
		t.Error("unary should not be streaming")
	}
}

func TestTypeDescAndID_Struct(t *testing.T) {
	type MyStruct struct{ X int }
	ids := map[typeKey]uint64{{ns: "test", name: "MyStruct"}: 42}

	td, sid := typeDescAndID("test", reflect.TypeOf(MyStruct{}), ids)
	if td.Kind != spore.TypeKindStruct {
		t.Errorf("Kind = %v, want TypeKindStruct", td.Kind)
	}
	if sid != 42 {
		t.Errorf("SchemaID = %d, want 42", sid)
	}
}

func TestTypeDescAndID_Time(t *testing.T) {
	ids := map[typeKey]uint64{}

	td, sid := typeDescAndID("test", reflect.TypeOf(time.Time{}), ids)
	if td.Kind != spore.TypeKindScalar {
		t.Errorf("time.Kind = %v, want TypeKindScalar", td.Kind)
	}
	if td.Name != "string" {
		t.Errorf("time.Name = %q, want string", td.Name)
	}
	if sid != 0 {
		t.Errorf("time.SchemaID = %d, want 0", sid)
	}
}

func TestBuildGosporeManifest_CallableEffectServiceToolName(t *testing.T) {
	self := newFakeRef(0x01)

	fn := func(_ actor.Context, req struct{ Name string }) (struct{ Result string }, error) {
		return struct{ Result string }{}, nil
	}

	ht := handler.NewTable()
	if err := ht.RegisterWithService("test.withmeta", fn, "filesystem",
		actor.WithEffect("write"),
		actor.WithToolName("my_write_tool"),
	); err != nil {
		t.Fatalf("register: %v", err)
	}

	c := cell.New(cell.Config{
		Self:     self,
		Actor:    &exportTestActor{},
		Handlers: ht,
	})

	cells := map[id.ActorID]*cell.Cell{self.ID(): c}
	gm, err := buildGosporeManifest(cells, nil)
	if err != nil {
		t.Fatalf("buildGosporeManifest: %v", err)
	}

	if len(gm.Callables) != 1 {
		t.Fatalf("len(callables) = %d, want 1", len(gm.Callables))
	}
	mc := gm.Callables[0]
	if mc.Effect != "write" {
		t.Errorf("Effect = %q, want write", mc.Effect)
	}
	if mc.Service != "filesystem" {
		t.Errorf("Service = %q, want filesystem", mc.Service)
	}
	if mc.ToolName != "my_write_tool" {
		t.Errorf("ToolName = %q, want my_write_tool", mc.ToolName)
	}
}

func TestBuildGosporeManifest_CallableZeroValueEffectServiceToolName(t *testing.T) {
	self := newFakeRef(0x01)

	fn := func(_ actor.Context, req struct{ Name string }) (struct{ Result string }, error) {
		return struct{ Result string }{}, nil
	}

	ht := handler.NewTable()
	// Register without WithEffect/WithToolName or an injected service — zero values must not appear in JSON.
	if err := ht.Register("test.nodmeta", fn); err != nil {
		t.Fatalf("register: %v", err)
	}

	c := cell.New(cell.Config{
		Self:     self,
		Actor:    &exportTestActor{},
		Handlers: ht,
	})

	cells := map[id.ActorID]*cell.Cell{self.ID(): c}
	gm, err := buildGosporeManifest(cells, nil)
	if err != nil {
		t.Fatalf("buildGosporeManifest: %v", err)
	}

	if len(gm.Callables) != 1 {
		t.Fatalf("len(callables) = %d, want 1", len(gm.Callables))
	}
	mc := gm.Callables[0]
	if mc.Effect != "" {
		t.Errorf("Effect = %q, want empty", mc.Effect)
	}
	if mc.Service != "" {
		t.Errorf("Service = %q, want empty", mc.Service)
	}
	if mc.ToolName != "" {
		t.Errorf("ToolName = %q, want empty", mc.ToolName)
	}
}

func TestRegister_WithToolNameInvalid(t *testing.T) {
	fn := func(_ actor.Context, req struct{ Name string }) (struct{ Result string }, error) {
		return struct{ Result string }{}, nil
	}

	ht := handler.NewTable()
	cases := []string{"has space", "has.dot", "has/slash"}
	for _, name := range cases {
		err := ht.Register("test.badtool", fn, actor.WithToolName(name))
		if err == nil {
			t.Errorf("Register with WithToolName(%q) should fail", name)
		}
	}
}

func TestRegister_WithToolNameValid(t *testing.T) {
	fn := func(_ actor.Context, req struct{ Name string }) (struct{ Result string }, error) {
		return struct{ Result string }{}, nil
	}

	ht := handler.NewTable()
	if err := ht.Register("test.goodtool", fn, actor.WithToolName("valid-tool_name")); err != nil {
		t.Fatalf("Register with valid tool name: %v", err)
	}
}

type StreamChunk struct{ FrameIndex int }
type StreamTile struct{ X, Y int }

func TestBuildGosporeManifest_StreamingChunkType(t *testing.T) {
	self := newFakeRef(0x01)

	streamingFn := func(_ actor.PureContext, req struct{ Target string }, emit actor.Emitter) error {
		return nil
	}

	ht := handler.NewTable()
	if err := ht.Register("test.stream", streamingFn, actor.Streaming[StreamChunk](), actor.Public()); err != nil {
		t.Fatalf("register streaming: %v", err)
	}

	c := cell.New(cell.Config{
		Self:     self,
		Actor:    &exportTestActor{},
		Handlers: ht,
	})

	cells := map[id.ActorID]*cell.Cell{self.ID(): c}
	gm, err := buildGosporeManifest(cells, nil)
	if err != nil {
		t.Fatalf("buildGosporeManifest: %v", err)
	}

	// StreamChunk should be registered as a schema.
	var chunkSchemaID uint64
	for _, s := range gm.Schemas {
		if s.Name == "StreamChunk" {
			chunkSchemaID = s.SchemaID
			break
		}
	}
	if chunkSchemaID == 0 {
		t.Fatal("StreamChunk schema not found in manifest")
	}

	// The callable should reference the chunk schema, not "any".
	if len(gm.Callables) != 1 {
		t.Fatalf("len(callables) = %d, want 1", len(gm.Callables))
	}
	mc := gm.Callables[0]
	if mc.Mode != "streaming" {
		t.Errorf("mode = %q, want streaming", mc.Mode)
	}
	if mc.ChunkSchemaID == 0 || mc.ChunkSchemaID == 0xFFFFFFFF { // BuiltinAny
		t.Errorf("ChunkSchemaID = %d, want concrete schema ID", mc.ChunkSchemaID)
	}
	if mc.Chunk == nil || mc.Chunk.Kind != spore.TypeKindStruct {
		t.Errorf("Chunk.Kind = %v, want TypeKindStruct", mc.Chunk.Kind)
	}
}
