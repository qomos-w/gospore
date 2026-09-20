package gateway_test

import (
	"compress/gzip"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/qomos-w/gospore/gateway"
)

func staticTestFS() fstest.MapFS {
	bigJS := "const x=1;" + strings.Repeat("//padpadpad\n", 200) // ~2.4KB
	return fstest.MapFS{
		"dist/index.html":                {Data: []byte("<html><body>spa</body></html>")},
		"dist/assets/main.js":            {Data: []byte(bigJS)},
		"dist/assets/app-AB12cdEF.js":   {Data: []byte(bigJS)},
		"dist/assets/tiny.js":            {Data: []byte("1;")},
		"dist/assets/logo.png":           {Data: make([]byte, 4096)},
	}
}

func httpGet(t *testing.T, url string, hdr map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestStaticServing_Gzip(t *testing.T) {
	_, addr, shutdown := newGatewayAppWithStaticFS(t, gateway.Nop(), staticTestFS())
	defer shutdown()
	base := "http://" + addr

	resp := httpGet(t, base+"/assets/main.js", map[string]string{"Accept-Encoding": "gzip"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if resp.Header.Get("Content-Encoding") != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", resp.Header.Get("Content-Encoding"))
	}
	if resp.Header.Get("Vary") != "Accept-Encoding" {
		t.Fatalf("Vary = %q", resp.Header.Get("Vary"))
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "javascript") {
		t.Fatalf("Content-Type = %q", ct)
	}
	zr, err := gzip.NewReader(resp.Body)
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	body, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("gunzip: %v", err)
	}
	if !strings.Contains(string(body), "padpadpad") {
		t.Fatal("gunzipped body does not match source file")
	}
	etag := resp.Header.Get("ETag")
	if etag == "" || !strings.HasSuffix(etag, `-gz"`) {
		t.Fatalf("ETag = %q, want gz-suffixed strong tag", etag)
	}

	// Identity request must not be gzipped. (Explicit identity header — Go's
	// transport otherwise injects transparent Accept-Encoding: gzip.)
	resp2 := httpGet(t, base+"/assets/main.js", map[string]string{"Accept-Encoding": "identity"})
	if resp2.Header.Get("Content-Encoding") != "" {
		t.Fatalf("identity response unexpectedly gzipped")
	}
	if etag2 := resp2.Header.Get("ETag"); strings.HasSuffix(etag2, `-gz"`) {
		t.Fatalf("identity ETag = %q, must not carry -gz", etag2)
	}

	// 304 for both representations, each revalidated with its own encoding.
	r304gz := httpGet(t, base+"/assets/main.js", map[string]string{"If-None-Match": etag, "Accept-Encoding": "gzip"})
	if r304gz.StatusCode != http.StatusNotModified {
		t.Fatalf("If-None-Match %s (gzip): status = %d, want 304", etag, r304gz.StatusCode)
	}
	r304id := httpGet(t, base+"/assets/main.js", map[string]string{"If-None-Match": resp2.Header.Get("ETag"), "Accept-Encoding": "identity"})
	if r304id.StatusCode != http.StatusNotModified {
		t.Fatalf("If-None-Match %s (identity): status = %d, want 304", resp2.Header.Get("ETag"), r304id.StatusCode)
	}
}

func TestStaticServing_CacheControl(t *testing.T) {
	_, addr, shutdown := newGatewayAppWithStaticFS(t, gateway.Nop(), staticTestFS())
	defer shutdown()
	base := "http://" + addr

	hashed := httpGet(t, base+"/assets/app-AB12cdEF.js", nil)
	if cc := hashed.Header.Get("Cache-Control"); cc != "public, max-age=31536000, immutable" {
		t.Fatalf("hashed asset Cache-Control = %q", cc)
	}

	plain := httpGet(t, base+"/assets/main.js", nil)
	if cc := plain.Header.Get("Cache-Control"); cc != "no-cache" {
		t.Fatalf("unhashed asset Cache-Control = %q", cc)
	}

	idx := httpGet(t, base+"/some/spa/route", nil)
	if cc := idx.Header.Get("Cache-Control"); cc != "no-cache" {
		t.Fatalf("SPA fallback Cache-Control = %q", cc)
	}
	if ct := idx.Header.Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Fatalf("SPA fallback Content-Type = %q", ct)
	}
	body, _ := io.ReadAll(idx.Body)
	if !strings.Contains(string(body), "spa") {
		t.Fatal("SPA fallback did not return index.html")
	}
	if etag := idx.Header.Get("ETag"); etag == "" {
		t.Fatal("SPA fallback missing ETag")
	}
}

func TestStaticServing_SmallAndBinaryNotGzipped(t *testing.T) {
	_, addr, shutdown := newGatewayAppWithStaticFS(t, gateway.Nop(), staticTestFS())
	defer shutdown()
	base := "http://" + addr

	tiny := httpGet(t, base+"/assets/tiny.js", map[string]string{"Accept-Encoding": "gzip"})
	if tiny.Header.Get("Content-Encoding") != "" {
		t.Fatal("tiny file must not be gzipped")
	}
	if tiny.Header.Get("Vary") != "" {
		t.Fatal("tiny file must not vary on Accept-Encoding")
	}

	png := httpGet(t, base+"/assets/logo.png", map[string]string{"Accept-Encoding": "gzip"})
	if png.Header.Get("Content-Encoding") != "" {
		t.Fatal("png must not be gzipped")
	}
}
