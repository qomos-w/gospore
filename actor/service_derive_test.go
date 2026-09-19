package actor

import "testing"

func TestServiceNameFor(t *testing.T) {
	tests := []struct {
		name    string
		domains []string
		callID  string
		want    string
	}{
		{
			name:    "hit: single domain matches first segment",
			domains: []string{"appmanager"},
			callID:  "appmanager.list",
			want:    "appmanager",
		},
		{
			name:    "miss: first segment not a declared domain",
			domains: []string{"appmanager"},
			callID:  "filesystem.read",
			want:    "",
		},
		{
			name:    "hit: one of several declared domains",
			domains: []string{"unified_graph", "inspect", "observation"},
			callID:  "inspect.document",
			want:    "inspect",
		},
		{
			name:    "prefix nesting never matches: app vs appmanager",
			domains: []string{"app"},
			callID:  "appmanager.list",
			want:    "",
		},
		{
			name:    "prefix nesting never matches: appmanager vs app",
			domains: []string{"appmanager"},
			callID:  "app.list",
			want:    "",
		},
		{
			name:    "flat callable (no dot) is agent-local",
			domains: []string{"agent"},
			callID:  "list_callables",
			want:    "",
		},
		{
			name:    "empty callID",
			domains: []string{"x"},
			callID:  "",
			want:    "",
		},
		{
			name:    "leading dot: empty first segment is not a domain",
			domains: []string{""},
			callID:  ".trailing",
			want:    "",
		},
		{
			name:    "empty domains",
			domains: []string{},
			callID:  "anything.do",
			want:    "",
		},
		{
			name:    "nil domains",
			domains: nil,
			callID:  "anything.do",
			want:    "",
		},
		{
			name:    "deep callID still derives from first segment",
			domains: []string{"appmanager"},
			callID:  "appmanager.apps.state.get",
			want:    "appmanager",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ServiceNameFor(tt.domains, tt.callID); got != tt.want {
				t.Errorf("ServiceNameFor(%v, %q) = %q, want %q", tt.domains, tt.callID, got, tt.want)
			}
		})
	}
}
