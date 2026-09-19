package actor

import "testing"

func TestResolveVisibility_Default(t *testing.T) {
	// No visibility options → default Internal
	vis := ResolveVisibility()
	if vis != VisibilityInternal {
		t.Errorf("default visibility = %v, want VisibilityInternal", vis)
	}
}

func TestResolveVisibility_Public(t *testing.T) {
	vis := ResolveVisibility(Public())
	if vis != VisibilityPublic {
		t.Errorf("Public() visibility = %v, want VisibilityPublic", vis)
	}
}

func TestResolveVisibility_AdminOnly(t *testing.T) {
	vis := ResolveVisibility(AdminOnly())
	if vis != VisibilityAdmin {
		t.Errorf("AdminOnly() visibility = %v, want VisibilityAdmin", vis)
	}
}

func TestResolveVisibility_Diagnostic(t *testing.T) {
	vis := ResolveVisibility(Diagnostic())
	if vis != VisibilityDiagnostic {
		t.Errorf("Diagnostic() visibility = %v, want VisibilityDiagnostic", vis)
	}
}

func TestResolveVisibility_Internal(t *testing.T) {
	vis := ResolveVisibility(Internal())
	if vis != VisibilityInternal {
		t.Errorf("Internal() visibility = %v, want VisibilityInternal", vis)
	}
}

func TestResolveVisibility_WithVisibility(t *testing.T) {
	vis := ResolveVisibility(WithVisibility(VisibilityPublic))
	if vis != VisibilityPublic {
		t.Errorf("WithVisibility(Public) = %v, want VisibilityPublic", vis)
	}
}

func TestResolveVisibility_LastWins(t *testing.T) {
	// When multiple visibility options are passed, the last one wins
	vis := ResolveVisibility(Public(), AdminOnly())
	if vis != VisibilityAdmin {
		t.Errorf("last wins: got %v, want VisibilityAdmin", vis)
	}
}

func TestResolveVisibility_WithStreaming(t *testing.T) {
	// Streaming option should not affect visibility resolution
	vis := ResolveVisibility(Streaming[string](), Public())
	if vis != VisibilityPublic {
		t.Errorf("Streaming + Public = %v, want VisibilityPublic", vis)
	}

	chunkTy, ok := ResolveStreamingChunkType(Streaming[string](), Public())
	if !ok {
		t.Error("Streaming chunk type should be resolved")
	}
	if chunkTy == nil {
		t.Error("Streaming chunk type should not be nil")
	}
}

func TestResolveVisibility_NilOption(t *testing.T) {
	// Nil options should be skipped
	var nilOpt RegisterOption
	vis := ResolveVisibility(nilOpt, Public())
	if vis != VisibilityPublic {
		t.Errorf("nil + Public = %v, want VisibilityPublic", vis)
	}
}

func TestResolveMode(t *testing.T) {
	mode, ok := ResolveMode(WithMode(ModeStateless))
	if !ok {
		t.Fatal("ResolveMode should report explicit mode")
	}
	if mode != ModeStateless {
		t.Fatalf("ResolveMode = %v, want %v", mode, ModeStateless)
	}
}

func TestResolveLoopOrDefault(t *testing.T) {
	if got := ResolveLoopOrDefault(ModeStateful); got != DefaultLoopOwner {
		t.Fatalf("stateful default loop = %q, want %q", got, DefaultLoopOwner)
	}
	if got := ResolveLoopOrDefault(ModeStateless); got != DefaultLoopPure {
		t.Fatalf("stateless default loop = %q, want %q", got, DefaultLoopPure)
	}
	if got := ResolveLoopOrDefault(ModeStateful, WithLoop("custom.exec")); got != "custom.exec" {
		t.Fatalf("explicit loop = %q, want %q", got, "custom.exec")
	}
}

func TestResolveLoop(t *testing.T) {
	if got := ResolveLoop(); got != "" {
		t.Fatalf("default explicit loop = %q, want empty", got)
	}
	if got := ResolveLoop(WithLoop("custom.exec")); got != "custom.exec" {
		t.Fatalf("ResolveLoop = %q, want %q", got, "custom.exec")
	}
}

// --- ResolveEffect tests ---

func TestResolveEffect_Default(t *testing.T) {
	got := ResolveEffect()
	if got != "" {
		t.Errorf("default effect = %q, want empty", got)
	}
}

func TestResolveEffect_Set(t *testing.T) {
	got := ResolveEffect(WithEffect("write"))
	if got != "write" {
		t.Errorf("WithEffect(write) = %q, want write", got)
	}
}

func TestResolveEffect_LastWins(t *testing.T) {
	got := ResolveEffect(WithEffect("read"), WithEffect("mutate"))
	if got != "mutate" {
		t.Errorf("last wins: got %q, want mutate", got)
	}
}

// --- ResolveToolName tests ---

func TestResolveToolName_Default(t *testing.T) {
	got := ResolveToolName()
	if got != "" {
		t.Errorf("default toolName = %q, want empty", got)
	}
}

func TestResolveToolName_Set(t *testing.T) {
	got := ResolveToolName(WithToolName("my_tool"))
	if got != "my_tool" {
		t.Errorf("WithToolName(my_tool) = %q, want my_tool", got)
	}
}

func TestResolveToolName_LastWins(t *testing.T) {
	got := ResolveToolName(WithToolName("old"), WithToolName("new"))
	if got != "new" {
		t.Errorf("last wins: got %q, want new", got)
	}
}

// --- WithToolName validation tests ---

func TestValidateToolName_Valid(t *testing.T) {
	cases := []string{"tool", "my_tool", "my-tool", "tool123", "A_b-C"}
	for _, name := range cases {
		if err := ValidateToolName(name); err != nil {
			t.Errorf("ValidateToolName(%q) = %v, want nil", name, err)
		}
	}
}

func TestValidateToolName_Empty(t *testing.T) {
	if err := ValidateToolName(""); err == nil {
		t.Error("ValidateToolName('') should return error")
	}
}

func TestValidateToolName_InvalidChars(t *testing.T) {
	cases := []string{"my tool", "my.tool", "my/tool", "my@tool"}
	for _, name := range cases {
		if err := ValidateToolName(name); err == nil {
			t.Errorf("ValidateToolName(%q) should return error", name)
		}
	}
}
