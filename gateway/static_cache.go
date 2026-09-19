package gateway

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"mime"
	"net/http"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

// staticOptimizer layers browser-friendly caching and compression on top of
// an embedded static FS. embed.FS reports a zero ModTime, so http.FileServer
// alone sends no Last-Modified, no ETag and no Cache-Control — browsers must
// re-download every asset on every load. The optimizer computes a strong
// content-hash ETag per file (once, lazily) and serves pre-gzipped bytes from
// memory, so:
//
//   - first load: gzip (~4x smaller for the JS-heavy SPA bundle)
//   - reload: If-None-Match → 304 with no body
//   - hashed asset names: Cache-Control immutable, no revalidation at all
type staticOptimizer struct {
	dist fs.FS

	mu      sync.Mutex
	entries map[string]*staticEntry
}

type staticEntry struct {
	etagBase string // 16 hex chars of sha256 over the identity bytes
	gz       []byte // nil when the file is not worth gzipping
}

const staticMinGzipSize = 1024

// Vite default output: "<name>-<8 base64url chars>.<ext>". Such names change
// whenever content changes, so they are safe to cache immutably. Names
// without the hash suffix (main.js, index.html) must revalidate every time.
var staticImmutableName = regexp.MustCompile(`-[A-Za-z0-9_-]{8}\.[A-Za-z0-9]+$`)

func newStaticOptimizer(dist fs.FS) *staticOptimizer {
	return &staticOptimizer{dist: dist, entries: map[string]*staticEntry{}}
}

func (o *staticOptimizer) entry(name string) (*staticEntry, error) {
	o.mu.Lock()
	if e, ok := o.entries[name]; ok {
		o.mu.Unlock()
		return e, nil
	}
	o.mu.Unlock()

	data, err := fs.ReadFile(o.dist, name)
	if err != nil {
		return nil, err
	}
	e := buildStaticEntry(name, data)

	o.mu.Lock()
	o.entries[name] = e
	o.mu.Unlock()
	return e, nil
}

func buildStaticEntry(name string, data []byte) *staticEntry {
	sum := sha256.Sum256(data)
	e := &staticEntry{etagBase: hex.EncodeToString(sum[:])[:16]}
	if len(data) >= staticMinGzipSize && staticCompressible(name) {
		var buf bytes.Buffer
		zw, _ := gzip.NewWriterLevel(&buf, gzip.DefaultCompression)
		_, _ = zw.Write(data)
		_ = zw.Close()
		e.gz = buf.Bytes()
	}
	return e
}

func staticCompressible(name string) bool {
	switch strings.ToLower(path.Ext(name)) {
	case ".js", ".mjs", ".cjs", ".css", ".html", ".htm", ".json",
		".svg", ".txt", ".xml", ".map", ".wasm":
		return true
	}
	return false
}

func staticContentType(name string) string {
	if ct := mime.TypeByExtension(path.Ext(name)); ct != "" {
		return ct
	}
	return "application/octet-stream"
}

func staticCacheControl(name string) string {
	if staticImmutableName.MatchString(path.Base(name)) {
		return "public, max-age=31536000, immutable"
	}
	return "no-cache"
}

// serveFile serves an existing dist file. identity responses are delegated to
// the wrapped file server (keeps Range/HEAD semantics); gzip and 304 responses
// are written directly.
func (o *staticOptimizer) serveFile(w http.ResponseWriter, r *http.Request, name string, fileServer http.Handler) {
	e, err := o.entry(name)
	if err != nil {
		fileServer.ServeHTTP(w, r)
		return
	}
	o.serveEntry(w, r, name, e, func() { fileServer.ServeHTTP(w, r) })
}

// serveBytes serves an in-memory file (the SPA index.html fallback).
func (o *staticOptimizer) serveBytes(w http.ResponseWriter, r *http.Request, name string, data []byte) {
	o.serveEntry(w, r, name, buildStaticEntry(name, data), func() {
		w.Header().Set("Content-Type", staticContentType(name))
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		if r.Method != http.MethodHead {
			_, _ = w.Write(data)
		}
	})
}

func (o *staticOptimizer) serveEntry(w http.ResponseWriter, r *http.Request, name string, e *staticEntry, writeIdentity func()) {
	w.Header().Set("Cache-Control", staticCacheControl(name))
	if e.gz != nil {
		w.Header().Add("Vary", "Accept-Encoding")
	}

	gz := r.Method == http.MethodGet &&
		e.gz != nil &&
		r.Header.Get("Range") == "" &&
		strings.Contains(r.Header.Get("Accept-Encoding"), "gzip")

	etag := `"` + e.etagBase
	if gz {
		etag += "-gz"
	}
	etag += `"`
	w.Header().Set("ETag", etag)

	if staticIfNoneMatch(r.Header.Get("If-None-Match"), etag) {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	if gz {
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Type", staticContentType(name))
		w.Header().Set("Content-Length", strconv.Itoa(len(e.gz)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(e.gz)
		return
	}
	writeIdentity()
}

// staticIfNoneMatch implements the weak-enough subset of RFC 9110 §13.1.2:
// a comma-separated list of tags (or `*`) matches when any element equals the
// served tag, ignoring W/ prefixes.
func staticIfNoneMatch(header, etag string) bool {
	if header == "" {
		return false
	}
	for _, part := range strings.Split(header, ",") {
		t := strings.TrimSpace(part)
		t = strings.TrimPrefix(t, "W/")
		if t == etag || t == "*" {
			return true
		}
	}
	return false
}
