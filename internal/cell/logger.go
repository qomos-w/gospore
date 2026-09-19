package cell

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/qomos-w/gospore/actor"
)

// defaultLogger implements actor.Logger with runtime.Caller-based file:line.
// When a LogHook is set, entries are forwarded to the hook instead of
// stdout so downstream consumers (e.g. myxos) receive structured data.
type defaultLogger struct {
	mu     sync.Mutex
	prefix string
	hook   actor.LogHook
}

func newDefaultLogger(prefix string, hook actor.LogHook) *defaultLogger {
	return &defaultLogger{prefix: prefix, hook: hook}
}

// NewDefaultLogger creates a runtime.Caller-based logger with the given prefix.
// Used by app.New when no logger is configured.
func NewDefaultLogger(prefix string, hook actor.LogHook) actor.Logger {
	return newDefaultLogger(prefix, hook)
}

func (l *defaultLogger) Debug(msg string, args ...any) { l.log("debug", msg, args) }
func (l *defaultLogger) Info(msg string, args ...any)  { l.log("info", msg, args) }
func (l *defaultLogger) Warn(msg string, args ...any)  { l.log("warn", msg, args) }
func (l *defaultLogger) Error(msg string, args ...any) { l.log("error", msg, args) }

func (l *defaultLogger) log(level, msg string, args []any) {
	_, file, line, ok := runtime.Caller(2)
	caller := ""
	if ok {
		caller = fmt.Sprintf("%s:%d", shortFile(file), line)
	}

	fields := parseArgs(args)

	// If a hook is registered, forward the structured entry and skip stdout.
	if l.hook != nil {
		l.hook(level, msg, caller, fields)
		return
	}

	var sb strings.Builder
	sb.WriteString(level)
	sb.WriteString(" ")
	sb.WriteString(caller)
	sb.WriteString(" ")
	if l.prefix != "" {
		sb.WriteString("[")
		sb.WriteString(l.prefix)
		sb.WriteString("] ")
	}
	sb.WriteString(msg)
	if len(fields) > 0 {
		sb.WriteString(" ")
		first := true
		for k, v := range fields {
			if !first {
				sb.WriteString(" ")
			}
			fmt.Fprintf(&sb, "%s=%v", k, v)
			first = false
		}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	log.Print(sb.String())
}

// parseArgs converts alternating key-value pairs into a map.
// Odd trailing args are kept with a synthetic key.
func parseArgs(args []any) map[string]any {
	if len(args) == 0 {
		return nil
	}
	m := make(map[string]any, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		if i+1 < len(args) {
			key, ok := args[i].(string)
			if !ok {
				key = fmt.Sprintf("arg%d", i)
			}
			m[key] = args[i+1]
		} else {
			m[fmt.Sprintf("arg%d", i)] = args[i]
		}
	}
	return m
}

// shortFile returns the path relative to the nearest parent directory that
// contains a go.mod file.  This preserves the full package path (e.g.
// "pkg/workspace/workspace.go") so the frontend can click through to the
// source file.  If no go.mod is found it falls back to the last two path
// components.  Results are cached per-directory so repeated calls in the
// same package are cheap.
var (
	shortFileRoots   = map[string]string{} // dir → go-mod root (or "" for none)
	shortFileRootsMu sync.RWMutex
)

func shortFile(path string) string {
	dir := filepath.Dir(path)

	shortFileRootsMu.RLock()
	root, ok := shortFileRoots[dir]
	shortFileRootsMu.RUnlock()
	if !ok {
		root = findGoModRoot(dir)
		shortFileRootsMu.Lock()
		shortFileRoots[dir] = root
		shortFileRootsMu.Unlock()
	}

	if root != "" {
		rel, err := filepath.Rel(root, path)
		if err == nil {
			return filepath.ToSlash(rel)
		}
	}

	// Fallback: last two components.
	base := filepath.Base(path)
	parent := filepath.Base(dir)
	if parent == "." || parent == "/" || parent == filepath.VolumeName(dir) {
		return base
	}
	return filepath.ToSlash(filepath.Join(parent, base))
}

func findGoModRoot(start string) string {
	dir := start
	for {
		if info, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil && !info.IsDir() {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}
