package gateway

import (
	"bytes"
	"testing"
)

func TestCompressDecompress_RoundTrip(t *testing.T) {
	original := bytes.Repeat([]byte("hello world "), 100) // > 256 bytes

	compressed, comp, err := Compress(original)
	if err != nil {
		t.Fatalf("Compress: %v", err)
	}
	if comp != CompressionGzip {
		t.Fatalf("expected CompressionGzip, got %d", comp)
	}
	if len(compressed) >= len(original) {
		t.Fatalf("compression should shrink payload: %d -> %d", len(original), len(compressed))
	}

	decompressed, err := Decompress(compressed, comp)
	if err != nil {
		t.Fatalf("Decompress: %v", err)
	}
	if !bytes.Equal(decompressed, original) {
		t.Fatal("round-trip mismatch")
	}
}

func TestCompress_BelowThreshold(t *testing.T) {
	original := []byte("small")

	data, comp, err := Compress(original)
	if err != nil {
		t.Fatalf("Compress: %v", err)
	}
	if comp != CompressionNone {
		t.Fatalf("expected CompressionNone for small payload, got %d", comp)
	}
	if !bytes.Equal(data, original) {
		t.Fatal("small payload should be returned as-is")
	}
}

func TestCompress_Ineffective(t *testing.T) {
	// Compress already-compressed data — gzip header overhead (~18 B) makes
	// the output larger than the input for small payloads.
	compressible := bytes.Repeat([]byte("hello "), 100)
	compressed, _, err := Compress(compressible)
	if err != nil {
		t.Fatalf("Compress: %v", err)
	}

	data, comp, err := Compress(compressed)
	if err != nil {
		t.Fatalf("Compress: %v", err)
	}
	if comp != CompressionNone {
		t.Fatalf("expected CompressionNone when ineffective, got %d", comp)
	}
	if !bytes.Equal(data, compressed) {
		t.Fatal("ineffective compression should fall back to raw")
	}
}

func TestDecompress_None(t *testing.T) {
	original := []byte("raw data")
	data, err := Decompress(original, CompressionNone)
	if err != nil {
		t.Fatalf("Decompress: %v", err)
	}
	if !bytes.Equal(data, original) {
		t.Fatal("CompressionNone should return data as-is")
	}
}

func TestDecompress_Empty(t *testing.T) {
	data, err := Decompress([]byte{}, CompressionNone)
	if err != nil {
		t.Fatalf("Decompress: %v", err)
	}
	if len(data) != 0 {
		t.Fatal("empty input should yield empty output")
	}
}

// TestCrossLanguageFixture generates a gzip fixture that TypeScript tests
// can hard-code to verify browser-side decompression.
func TestCrossLanguageFixture(t *testing.T) {
	payload := bytes.Repeat([]byte("cross-language gzip fixture for binary wire protocol testing "), 20)
	compressed, comp, err := Compress(payload)
	if err != nil {
		t.Fatalf("Compress: %v", err)
	}
	if comp != CompressionGzip {
		t.Fatal("fixture should be gzip compressed")
	}

	t.Logf("Fixture hex: %x", compressed)

	decompressed, err := Decompress(compressed, CompressionGzip)
	if err != nil {
		t.Fatalf("Decompress: %v", err)
	}
	if !bytes.Equal(decompressed, payload) {
		t.Fatal("fixture round-trip failed")
	}
}
