package cell

import "encoding/json"

// ScriptNormalizeValue is the single script-visible value normalization
// shared by every script host (cell runtime and gateway interceptor).
//
// Two shapes arrive from invoke paths:
//   - raw []byte wire payloads (the cell runtime's internal frame path):
//     with an app codec configured — the real-app default — payload
//     decoding stays the codec's business and scripts see a string;
//     without one, a best-effort JSON decode lets scripts work with
//     plain objects (test/embedded hosts).
//   - typed Go values (the public ref.Invoke path decodes replies via
//     the resolver-bound codec): the VM cannot marshal arbitrary host
//     structs, so they round-trip through JSON into plain maps.
//
// Before this existed the hosts drifted: the cell host returned string
// under a codec and a decoded JSON value otherwise, while the gateway
// handed raw []byte (or typed structs the VM panics on) to scripts —
// the same callable produced different payload shapes depending on
// which host executed the script.
func ScriptNormalizeValue(v any, codecSet bool) any {
	if b, ok := v.([]byte); ok {
		if codecSet {
			return string(b)
		}
		var decoded any
		if json.Unmarshal(b, &decoded) == nil {
			return decoded
		}
		return string(b)
	}
	switch v.(type) {
	case nil, bool, string,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64,
		[]any, map[string]any:
		return v
	}
	if data, err := json.Marshal(v); err == nil {
		var decoded any
		if json.Unmarshal(data, &decoded) == nil {
			return decoded
		}
	}
	return v
}
