package gateway

import "testing"

func TestStaticIfNoneMatch(t *testing.T) {
	const tag = `"abc123"`
	cases := []struct {
		header string
		want   bool
	}{
		{"", false},
		{tag, true},
		{"W/" + tag, true},
		{`"other", ` + tag, true},
		{`  W/` + tag + `  `, true},
		{`"other"`, false},
		{"*", true},
	}
	for _, tc := range cases {
		if got := staticIfNoneMatch(tc.header, tag); got != tc.want {
			t.Errorf("If-None-Match %q vs %s = %v, want %v", tc.header, tag, got, tc.want)
		}
	}
}
