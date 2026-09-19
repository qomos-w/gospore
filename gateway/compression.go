package gateway

import (
	"bytes"
	"compress/gzip"
	"sync"
	"time"
)

const compressThreshold = 256

var gzipWriterPool = sync.Pool{
	New: func() any { return gzip.NewWriter(nil) },
}

var gzipReaderPool = sync.Pool{
	New: func() any { return new(gzip.Reader) },
}

// Compress gzip-compresses data when it exceeds the threshold and the
// compressed result is actually smaller.  It returns the (possibly unchanged)
// data and the compression method that was applied.
func Compress(data []byte) ([]byte, PayloadCompression, error) {
	if len(data) < compressThreshold {
		return data, CompressionNone, nil
	}

	w := gzipWriterPool.Get().(*gzip.Writer)
	defer gzipWriterPool.Put(w)

	var buf bytes.Buffer
	w.Reset(&buf)
	w.Header.ModTime = time.Time{} // deterministic output for tests / caching
	if _, err := w.Write(data); err != nil {
		return nil, CompressionNone, err
	}
	if err := w.Close(); err != nil {
		return nil, CompressionNone, err
	}

	if buf.Len() >= len(data) {
		// Compression expanded the payload — send raw instead.
		return data, CompressionNone, nil
	}

	return buf.Bytes(), CompressionGzip, nil
}

// Decompress decompresses data according to the compression method.
// When comp is CompressionNone the original data is returned as-is.
func Decompress(data []byte, comp PayloadCompression) ([]byte, error) {
	if comp == CompressionNone || len(data) == 0 {
		return data, nil
	}

	r := gzipReaderPool.Get().(*gzip.Reader)
	defer gzipReaderPool.Put(r)

	if err := r.Reset(bytes.NewReader(data)); err != nil {
		return nil, err
	}
	defer r.Close()

	var out bytes.Buffer
	if _, err := out.ReadFrom(r); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}
