package wiregen

import (
	"bytes"
	"encoding/hex"
	"flag"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/qomos-w/gospore/gateway"
)

var update = flag.Bool("update", false, "rewrite the committed web-client artifacts")

const (
	systemTSPath   = "../../web-client/src/generated/system_protocol.ts"
	frameVectorDir = "../../web-client/testdata/frame_vector.json"
)

func TestSystemProtocolTSInSync(t *testing.T) {
	want := RenderSystemProtocolTS()
	got, err := os.ReadFile(systemTSPath)
	if err != nil {
		t.Fatalf("read %s: %v (run `go test ./internal/wiregen -update` to generate)", systemTSPath, err)
	}
	if string(got) != want {
		t.Fatalf("%s is out of sync with schema/builtin.go; run `go test ./internal/wiregen -update` and commit", systemTSPath)
	}
}

func TestFrameVectorInSync(t *testing.T) {
	want, err := RenderFrameVectorJSON()
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(frameVectorDir)
	if err != nil {
		t.Fatalf("read %s: %v (run `go test ./internal/wiregen -update` to generate)", frameVectorDir, err)
	}
	if !bytes.Equal(bytes.TrimSpace(got), bytes.TrimSpace(want)) {
		t.Fatalf("%s is out of sync with gateway/frame_binary.go; run `go test ./internal/wiregen -update` and commit", frameVectorDir)
	}
}

// TestFrameVectorRoundTripGuarantees re-derives every vector wire blob
// through the real Go implementation and decodes it back, so the
// committed vectors can never rot while the sync tests pass.
func TestFrameVectorRoundTripGuarantees(t *testing.T) {
	for _, tc := range frameCases() {
		wire, err := hex.DecodeString(tc.Wire)
		if err != nil {
			t.Fatalf("%s: bad hex: %v", tc.Name, err)
		}
		got, err := gateway.UnmarshalWireFrame(wire)
		if err != nil {
			t.Fatalf("%s: unmarshal committed vector: %v", tc.Name, err)
		}
		if got.CorID != parseU64(t, tc.Frame.CorID) || got.CallID != tc.Frame.CallID ||
			got.From != tc.Frame.From || got.Target != tc.Frame.Target || got.SubID != tc.Frame.SubID ||
			got.ErrorMsg != tc.Frame.ErrorMsg || got.Seq != tc.Frame.Seq ||
			got.TransID != parseU64(t, tc.Frame.TransID) || got.Type != gateway.FrameType(tc.Frame.Type) ||
			got.Flags != gateway.Flags(tc.Frame.Flags) {
			t.Fatalf("%s: decode mismatch: %+v", tc.Name, got)
		}
		wantPayload := tc.Frame.Payload
		gotPayload := ""
		if wantPayload != nil {
			gotPayload = *wantPayload
			w, err := hex.DecodeString(gotPayload)
			if err != nil {
				t.Fatalf("%s: payload hex: %v", tc.Name, err)
			}
			if !bytes.Equal(got.Payload, w) {
				t.Fatalf("%s: payload mismatch", tc.Name)
			}
			continue
		}
		if len(got.Payload) != 0 {
			t.Fatalf("%s: expected empty payload, got %d bytes", tc.Name, len(got.Payload))
		}
	}
	for _, ec := range errorCases() {
		wire, err := hex.DecodeString(ec.Wire)
		if err != nil {
			t.Fatalf("%s: bad hex: %v", ec.Name, err)
		}
		if _, err := gateway.UnmarshalWireFrame(wire); err == nil {
			t.Fatalf("%s: expected decode error, got none", ec.Name)
		}
	}
}

func parseU64(t *testing.T, s string) uint64 {
	t.Helper()
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return v
}

func TestMain(m *testing.M) {
	flag.Parse()
	if *update {
		if err := os.MkdirAll(filepath.Dir(systemTSPath), 0o755); err != nil {
			panic(err)
		}
		if err := os.WriteFile(systemTSPath, []byte(RenderSystemProtocolTS()), 0o644); err != nil {
			panic(err)
		}
		vec, err := RenderFrameVectorJSON()
		if err != nil {
			panic(err)
		}
		if err := os.MkdirAll(filepath.Dir(frameVectorDir), 0o755); err != nil {
			panic(err)
		}
		if err := os.WriteFile(frameVectorDir, vec, 0o644); err != nil {
			panic(err)
		}
	}
	os.Exit(m.Run())
}
