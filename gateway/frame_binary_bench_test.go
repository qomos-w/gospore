package gateway

import "testing"

// Wire-frame codec baseline. These benches pin the marshal/unmarshal cost
// of the GSF binary frame across payload and header shapes so future
// codec changes have a reference point.

func benchFrame(payloadLen int, headers int) *WireFrame {
	f := &WireFrame{
		Type:    FrameTypeChunk,
		CorID:   42,
		Seq:     7,
		CallID:  "bench.call",
		SubID:   "sub-1",
		Target:  "0100000000000000000000000000000f",
		From:    "0100000000000000000000000000000e",
		Payload: make([]byte, payloadLen),
	}
	if headers > 0 {
		f.Headers = make(map[string]string, headers)
		for i := 0; i < headers; i++ {
			f.Headers["x-bench-"+string(rune('a'+i%26))+string(rune('0'+i/26))] = "value"
		}
	}
	return f
}

func BenchmarkMarshalWireFrame(b *testing.B) {
	cases := []struct {
		name    string
		payload int
		headers int
	}{
		{"small_no_headers", 16, 0},
		{"typical", 512, 2},
		{"large_payload_64k", 64 << 10, 0},
		{"header_heavy_16", 64, 16},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			f := benchFrame(tc.payload, tc.headers)
			b.SetBytes(int64(tc.payload))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := MarshalWireFrame(f); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkUnmarshalWireFrame(b *testing.B) {
	cases := []struct {
		name    string
		payload int
		headers int
	}{
		{"small_no_headers", 16, 0},
		{"typical", 512, 2},
		{"large_payload_64k", 64 << 10, 0},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			wire, err := MarshalWireFrame(benchFrame(tc.payload, tc.headers))
			if err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(len(wire)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := UnmarshalWireFrame(wire); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkWireFrameRoundtrip(b *testing.B) {
	wire, err := MarshalWireFrame(benchFrame(512, 2))
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(wire)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := MarshalWireFrame(&WireFrame{
			Type: FrameTypeChunk, CorID: 42, Seq: 7,
			CallID: "bench.call", SubID: "sub-1",
			Target: "0100000000000000000000000000000f",
			From:   "0100000000000000000000000000000e",
			Payload: func() []byte {
				out, err := UnmarshalWireFrame(wire)
				if err != nil {
					b.Fatal(err)
				}
				return out.Payload
			}(),
		})
		if err != nil {
			b.Fatal(err)
		}
		_ = data
	}
}
