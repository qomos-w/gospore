package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/qomos-w/gospore/internal/collections"
	"github.com/qomos-w/gospore/schema"
	"github.com/qomos-w/spore/gen/render"
	spore "github.com/qomos-w/spore/schema"
)

func main() {
	var (
		inPath        = flag.String("in", "-", `manifest path; "-" reads stdin`)
		outDir        = flag.String("out", "", "output directory (required)")
		visibilityCSV = flag.String("visibility", "public", "comma-separated visibilities to include: internal,public,admin,diagnostic")
		header        = flag.String("header", "// AUTO-GENERATED — DO NOT EDIT", "comment header prepended to every generated file (\\n interpreted as newline)")
	)
	flag.Parse()

	if *outDir == "" {
		fmt.Fprintln(os.Stderr, "gospore-gen-ts: --out is required")
		flag.Usage()
		os.Exit(2)
	}

	// Interpret \n escapes in header so Makefiles can pass multi-line strings.
	hdr := strings.ReplaceAll(*header, `\n`, "\n")

	visibilities, err := parseVisibilities(*visibilityCSV)
	if err != nil {
		fail(err)
	}

	raw, err := readInput(*inPath)
	if err != nil {
		fail(fmt.Errorf("read manifest: %w", err))
	}

	var gm schema.GosporeManifest
	if err := json.Unmarshal(raw, &gm); err != nil {
		fail(fmt.Errorf("decode manifest: %w", err))
	}

	// Convert spore Manifest portion to render inputs.
	schemas, callables := convertManifest(gm, visibilities)

	// Spore base layer.
	files, err := render.Generate(schemas, callables, render.Options{
		Visibilities: visibilities,
		Header:       hdr,
	})
	if err != nil {
		fail(fmt.Errorf("generate: %w", err))
	}

	// Gospore extensions (filtered by visibility).
	proj := filterProjections(gm.Projections, visibilities)
	if len(proj) > 0 {
		files["projections.ts"] = renderProjections(proj, gm.Manifest.Schemas, gm.Manifest.Callables, hdr)
		projectionClientFiles := renderProjectionClients(proj, gm.Manifest.Schemas, hdr)
		for p, content := range projectionClientFiles {
			files[p] = content
			ns := strings.SplitN(p, "/", 2)[0]
			idxPath := ns + "/index.ts"
			if existing, ok := files[idxPath]; ok {
				files[idxPath] = existing + fmt.Sprintf("export * from \"./projection-client.js\";\n")
			}
		}
	}
	events := filterEvents(gm.Events, visibilities)
	if len(events) > 0 {
		files["events.ts"] = renderEvents(events, gm.Manifest.Schemas, hdr)
	}

	// Client convenience functions per namespace (filtered by visibility).
	visibilitySet := make(map[render.Visibility]struct{}, len(visibilities))
	for _, v := range visibilities {
		visibilitySet[v] = struct{}{}
	}
	var filteredCallables []spore.ManifestCallable
	seenClientCallables := make(map[string]struct{})
	for _, c := range gm.Manifest.Callables {
		v, _ := render.ParseVisibility(c.Visibility)
		if _, ok := visibilitySet[v]; !ok {
			continue
		}
		// Same dedup rationale as convertManifest: identical callables on
		// sibling cells collapse to one client function; routing per
		// instance is done via opts.target.
		key := c.Name
		if c.Namespace != "" {
			key = c.Namespace + "." + c.Name
		}
		if _, dup := seenClientCallables[key]; dup {
			continue
		}
		seenClientCallables[key] = struct{}{}
		filteredCallables = append(filteredCallables, c)
	}
	clientFiles := renderClients(filteredCallables, events, gm.Manifest.Schemas, hdr)
	for p, content := range clientFiles {
		files[p] = content
		// Append client export to namespace index.ts if it exists.
		ns := strings.SplitN(p, "/", 2)[0]
		idxPath := ns + "/index.ts"
		if existing, ok := files[idxPath]; ok {
			files[idxPath] = existing + fmt.Sprintf("export * from \"./client.js\";\n")
		}
	}

	// Top-level barrel that re-exports every namespace plus projections/events.
	files["index.ts"] = renderTopIndex(files, hdr)

	for relPath, content := range files {
		full := filepath.Join(*outDir, relPath)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			fail(fmt.Errorf("mkdir %s: %w", full, err))
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			fail(fmt.Errorf("write %s: %w", full, err))
		}
	}
	fmt.Fprintf(os.Stderr, "gospore-gen-ts: wrote %d file(s) to %s\n", len(files), *outDir)
}

func readInput(path string) ([]byte, error) {
	if path == "-" {
		return io.ReadAll(os.Stdin)
	}
	return os.ReadFile(path)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "gospore-gen-ts:", err)
	os.Exit(1)
}

func parseVisibilities(csv string) ([]render.Visibility, error) {
	var out []render.Visibility
	for _, raw := range strings.Split(csv, ",") {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		v, ok := render.ParseVisibility(raw)
		if !ok {
			return nil, fmt.Errorf("unknown visibility %q (want one of internal/public/admin/diagnostic)", raw)
		}
		out = append(out, v)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("--visibility cannot be empty")
	}
	return out, nil
}

func convertManifest(m schema.GosporeManifest, visibilities []render.Visibility) ([]render.NamedObjectDesc, []render.NamedCallableDesc) {
	visibilitySet := make(map[render.Visibility]struct{}, len(visibilities))
	for _, v := range visibilities {
		visibilitySet[v] = struct{}{}
	}

	referencedSchemaIDs := make(map[uint64]struct{})
	schemaByID := make(map[uint64]spore.ManifestSchema, len(m.Schemas))
	classNameToID := make(map[string]uint64, len(m.Schemas))
	for _, s := range m.Schemas {
		schemaByID[s.SchemaID] = s
		if s.Object.Name != "" {
			classNameToID[s.Object.Name] = s.SchemaID
		}
		if s.Name != "" && s.Name != s.Object.Name {
			classNameToID[s.Name] = s.SchemaID
		}
	}
	var collectReferencedSchemaIDs func(uint64)
	collectReferencedSchemaIDs = func(schemaID uint64) {
		if schemaID == 0 {
			return
		}
		if _, seen := referencedSchemaIDs[schemaID]; seen {
			return
		}
		referencedSchemaIDs[schemaID] = struct{}{}
		s, ok := schemaByID[schemaID]
		if !ok {
			return
		}
		collectTypeDescSchemaIDs(s.Object, classNameToID, collectReferencedSchemaIDs)
	}

	for _, c := range m.Callables {
		v, _ := render.ParseVisibility(c.Visibility)
		if _, ok := visibilitySet[v]; !ok {
			continue
		}
		collectReferencedSchemaIDs(c.ReqSchemaID)
		collectReferencedSchemaIDs(c.ChunkSchemaID)
		collectReferencedSchemaIDs(c.FinalSchemaID)
	}
	for _, e := range m.Events {
		v, _ := render.ParseVisibility(e.Visibility)
		if _, ok := visibilitySet[v]; !ok {
			continue
		}
		collectReferencedSchemaIDs(e.SchemaID)
	}
	for _, p := range m.Projections {
		v, _ := render.ParseVisibility(p.Visibility)
		if _, ok := visibilitySet[v]; !ok {
			continue
		}
		collectReferencedSchemaIDs(p.SchemaID)
		collectTypeDescNestedSchemaIDs(p.Type, classNameToID, collectReferencedSchemaIDs)
	}

	var schemas []render.NamedObjectDesc
	for _, s := range m.Schemas {
		v, _ := render.ParseVisibility(s.Visibility)
		_, visAllowed := visibilitySet[v]
		_, referenced := referencedSchemaIDs[s.SchemaID]
		if !visAllowed && !referenced {
			continue
		}
		schemas = append(schemas, render.NamedObjectDesc{
			Namespace:  s.Namespace,
			SchemaID:   s.SchemaID,
			Name:       s.Name,
			Object:     s.Object,
			Visibility: v,
		})
	}

	var callables []render.NamedCallableDesc
	seenCallables := make(map[string]struct{})
	for _, c := range m.Callables {
		v, _ := render.ParseVisibility(c.Visibility)
		if _, ok := visibilitySet[v]; !ok {
			continue
		}
		// Manifest may carry the same (namespace, name) for each cell that
		// registers it (e.g. two sibling aggregator children with identical
		// callable surfaces). Emit one client function per unique pair —
		// per-instance routing is handled at call time via opts.target.
		key := c.Name
		if c.Namespace != "" {
			key = c.Namespace + "." + c.Name
		}
		if _, dup := seenCallables[key]; dup {
			continue
		}
		seenCallables[key] = struct{}{}
		mode := spore.CallableMode(c.Mode)
		// Single-segment (actor-local) callables register under the "local"
		// namespace in the spore TS registry; the wire callID remains the bare
		// name (see wireCallID).
		registryNS := c.Namespace
		if registryNS == "" {
			registryNS = "local"
		}
		callables = append(callables, render.NamedCallableDesc{
			Namespace:     registryNS,
			Name:          c.Name,
			Visibility:    v,
			Mode:          mode,
			ReqSchemaID:   c.ReqSchemaID,
			ChunkSchemaID: c.ChunkSchemaID,
			FinalSchemaID: c.FinalSchemaID,
			Req:           c.Req,
			Chunk:         c.Chunk,
			Final:         c.Final,
		})
	}
	return schemas, callables
}

func collectTypeDescSchemaIDs(td spore.ObjectDesc, classNameToID map[string]uint64, visit func(uint64)) {
	for _, field := range td.Fields {
		collectTypeDescNestedSchemaIDs(field.Type, classNameToID, visit)
	}
}

func collectTypeDescNestedSchemaIDs(td spore.TypeDesc, classNameToID map[string]uint64, visit func(uint64)) {
	if td.ClassID != 0 {
		visit(td.ClassID)
	} else if td.ClassName != "" {
		if id, ok := classNameToID[td.ClassName]; ok {
			visit(id)
		}
	}
	if td.Element != nil {
		collectTypeDescNestedSchemaIDs(*td.Element, classNameToID, visit)
	}
	if td.Key != nil {
		collectTypeDescNestedSchemaIDs(*td.Key, classNameToID, visit)
	}
	if td.Value != nil {
		collectTypeDescNestedSchemaIDs(*td.Value, classNameToID, visit)
	}
}

func filterProjections(projections []schema.ProjectionDecl, visibilities []render.Visibility) []schema.ProjectionDecl {
	visibilitySet := make(map[render.Visibility]struct{}, len(visibilities))
	for _, v := range visibilities {
		visibilitySet[v] = struct{}{}
	}
	var out []schema.ProjectionDecl
	for _, p := range projections {
		v, _ := render.ParseVisibility(p.Visibility)
		if _, ok := visibilitySet[v]; !ok {
			continue
		}
		out = append(out, p)
	}
	return out
}

func filterEvents(events []schema.EventDecl, visibilities []render.Visibility) []schema.EventDecl {
	visibilitySet := make(map[render.Visibility]struct{}, len(visibilities))
	for _, v := range visibilities {
		visibilitySet[v] = struct{}{}
	}
	var out []schema.EventDecl
	for _, e := range events {
		v, _ := render.ParseVisibility(e.Visibility)
		if _, ok := visibilitySet[v]; !ok {
			continue
		}
		out = append(out, e)
	}
	return out
}

func renderProjections(projections []schema.ProjectionDecl, schemas []spore.ManifestSchema, callables []spore.ManifestCallable, header string) string {
	var b strings.Builder
	if header != "" {
		b.WriteString(header)
		b.WriteString("\n")
	}
	b.WriteString("import { GosporeClient } from \"@qomos/gospore-client\";\n")
	b.WriteString("\n")
	b.WriteString("export const Projections = {\n")

	schemaByID := make(map[uint64]spore.ManifestSchema, len(schemas))
	for _, s := range schemas {
		schemaByID[s.SchemaID] = s
	}

	// Group by namespace for readability.
	byNS := make(map[string][]schema.ProjectionDecl)
	for _, p := range projections {
		byNS[p.Namespace] = append(byNS[p.Namespace], p)
	}
	nss := collections.SortedKeys(byNS)

	for _, ns := range nss {
		b.WriteString(fmt.Sprintf("  %s: {\n", tsIdent(ns)))
		for _, p := range byNS[ns] {
			schemaName := p.SchemaName
			if schemaName == "" {
				if s, ok := schemaByID[p.SchemaID]; ok && s.Name != "" {
					schemaName = s.Name
				} else {
					schemaName = pascalCase(p.Component)
				}
			}
			b.WriteString(fmt.Sprintf("    %s: {\n", p.Component))
			b.WriteString(fmt.Sprintf("      actorPath: %q,\n", p.ActorPath))
			b.WriteString(fmt.Sprintf("      component: %q,\n", p.Component))
			b.WriteString(fmt.Sprintf("      schemaId: %d,\n", p.SchemaID))
			b.WriteString(fmt.Sprintf("      schemaName: %q,\n", schemaName))
			b.WriteString(fmt.Sprintf("      mode: %q,\n", p.Mode))
			b.WriteString("    },\n")
		}
		b.WriteString("  },\n")
	}
	b.WriteString("} as const;\n\n")

	getReqSchemaID := findCallableReqSchemaID(callables, "gospore.projection.get")
	watchReqSchemaID := findCallableReqSchemaID(callables, "gospore.projection.watch")

	b.WriteString(`export async function getProjection<T>(client: GosporeClient, p: {
  actorPath: string
  component: string
  schemaId: number
}): Promise<T> {
  return client.invoke("gospore.projection.get", {
    actorPath: p.actorPath,
    component: p.component,
    schemaId: p.schemaId,
  }, { reqSchemaId: `)
	b.WriteString(fmt.Sprintf("%d", getReqSchemaID))
	b.WriteString(` }) as Promise<T>
}

export async function *watchProjection<T>(client: GosporeClient, p: {
  actorPath: string
  component: string
  schemaId: number
}): AsyncIterable<T> {
  yield* client.subscribe<T>("gospore.projection.watch", {
    actorPath: p.actorPath,
    component: p.component,
    schemaId: p.schemaId,
  }, { reqSchemaId: `)
	b.WriteString(fmt.Sprintf("%d", watchReqSchemaID))
	b.WriteString(` })
}
`)
	return b.String()
}

// wireCallID reconstructs the on-the-wire callable ID. Single-segment
// (actor-local) callables carry an empty namespace and are invoked by
// their bare name.
func wireCallID(c spore.ManifestCallable) string {
	if c.Namespace == "" {
		return c.Name
	}
	return c.Namespace + "." + c.Name
}

func findCallableReqSchemaID(callables []spore.ManifestCallable, callID string) uint64 {
	for _, c := range callables {
		if wireCallID(c) == callID {
			return c.ReqSchemaID
		}
	}
	return 0
}

func renderEvents(events []schema.EventDecl, schemas []spore.ManifestSchema, header string) string {
	var b strings.Builder
	if header != "" {
		b.WriteString(header)
		b.WriteString("\n")
	}
	b.WriteString("export const Events = {\n")

	schemaByID := make(map[uint64]spore.ManifestSchema, len(schemas))
	for _, s := range schemas {
		schemaByID[s.SchemaID] = s
	}
	byNS := make(map[string][]schema.EventDecl)
	for _, e := range events {
		byNS[e.Namespace] = append(byNS[e.Namespace], e)
	}
	nss := collections.SortedKeys(byNS)
	for _, ns := range nss {
		b.WriteString(fmt.Sprintf("  %s: {\n", tsIdent(ns)))
		for _, e := range byNS[ns] {
			schemaName := ""
			if s, ok := schemaByID[e.SchemaID]; ok && s.Name != "" {
				schemaName = s.Name
			} else {
				schemaName = pascalCase(e.Kind)
			}
			b.WriteString(fmt.Sprintf("    %s: {\n", tsKey(e.Kind)))
			b.WriteString(fmt.Sprintf("      actorPath: %q,\n", e.ActorPath))
			b.WriteString(fmt.Sprintf("      kind: %q,\n", e.Kind))
			b.WriteString(fmt.Sprintf("      schemaId: %d,\n", e.SchemaID))
			b.WriteString(fmt.Sprintf("      schemaName: %q,\n", schemaName))
			b.WriteString("    },\n")
		}
		b.WriteString("  },\n")
	}
	b.WriteString("} as const;\n")
	return b.String()
}

func renderTopIndex(files map[string]string, header string) string {
	seen := map[string]bool{}
	var namespaces []string
	for rel := range files {
		parts := strings.Split(rel, "/")
		if len(parts) >= 2 && parts[len(parts)-1] == "index.ts" {
			ns := parts[0]
			if ns != "" && !seen[ns] {
				seen[ns] = true
				namespaces = append(namespaces, ns)
			}
		}
	}
	slices.Sort(namespaces)
	var b strings.Builder
	if header != "" {
		b.WriteString(header)
		b.WriteString("\n")
	}
	for _, ns := range namespaces {
		b.WriteString(fmt.Sprintf("export * as %s from \"./%s/index.js\";\n", tsIdent(ns), ns))
	}
	if _, ok := files["projections.ts"]; ok {
		b.WriteString("export * from \"./projections.js\";\n")
	}
	if _, ok := files["events.ts"]; ok {
		b.WriteString("export * from \"./events.js\";\n")
	}
	return b.String()
}

// tsIdent maps manifest namespaces like "workspace.ui" to stable JS
// identifiers suitable for object keys without quotes. Dots — common in
// spore namespaces — are not legal in TS identifiers.
func tsIdent(ns string) string {
	return strings.ReplaceAll(ns, ".", "_")
}

// tsKey returns s as a TS object key, quoting it when it contains characters
// that are not legal in bare identifiers (digits, hyphens, dots, etc.).
func tsKey(s string) string {
	if isValidTSIdent(s) {
		return s
	}
	return fmt.Sprintf("%q", s)
}

func isValidTSIdent(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		if r == '_' {
			continue
		}
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' {
			continue
		}
		if r >= '0' && r <= '9' && i > 0 {
			continue
		}
		return false
	}
	return true
}

func pascalCase(s string) string {
	parts := strings.FieldsFunc(s, func(r rune) bool {
		return r == '.' || r == '_' || r == '-' || r == ' '
	})
	for i, part := range parts {
		if part == "" {
			continue
		}
		runes := []rune(part)
		parts[i] = strings.ToUpper(string(runes[0])) + strings.ToLower(string(runes[1:]))
	}
	return strings.Join(parts, "")
}

func camelCase(s string) string {
	p := pascalCase(s)
	if p == "" {
		return ""
	}
	runes := []rune(p)
	return escapeTSReserved(strings.ToLower(string(runes[0])) + string(runes[1:]))
}

// tsReservedWords are JS/TS keywords that cannot be used as identifiers.
var tsReservedWords = map[string]bool{
	"break": true, "case": true, "catch": true, "class": true, "const": true,
	"continue": true, "debugger": true, "default": true, "delete": true, "do": true,
	"else": true, "export": true, "extends": true, "finally": true, "for": true,
	"function": true, "if": true, "import": true, "in": true, "instanceof": true,
	"new": true, "return": true, "super": true, "switch": true, "this": true,
	"throw": true, "try": true, "typeof": true, "var": true, "void": true,
	"while": true, "with": true, "yield": true, "enum": true, "await": true,
	"implements": true, "interface": true, "let": true, "package": true,
	"private": true, "protected": true, "public": true, "static": true,
	"null": true, "true": true, "false": true,
}

func escapeTSReserved(s string) string {
	if tsReservedWords[s] {
		return "_" + s
	}
	return s
}

func renderProjectionClients(projections []schema.ProjectionDecl, schemas []spore.ManifestSchema, header string) map[string]string {
	byNS := make(map[string][]schema.ProjectionDecl)
	for _, p := range projections {
		byNS[p.Namespace] = append(byNS[p.Namespace], p)
	}

	schemaByID := make(map[uint64]spore.ManifestSchema, len(schemas))
	schemaByName := make(map[string]spore.ManifestSchema, len(schemas))
	for _, s := range schemas {
		schemaByID[s.SchemaID] = s
		if s.Name != "" {
			schemaByName[s.Name] = s
		}
		if s.Object.Name != "" && s.Object.Name != s.Name {
			schemaByName[s.Object.Name] = s
		}
	}

	nss := collections.SortedKeys(byNS)

	out := make(map[string]string)
	for _, ns := range nss {
		var b strings.Builder
		if header != "" {
			b.WriteString(header)
			b.WriteString("\n")
		}
		b.WriteString("import { GosporeClient } from \"@qomos/gospore-client\";\n")
		b.WriteString("import { getProjection, watchProjection, Projections } from \"../projections\";\n")

		projectionImports := make(map[string]struct{})
		renderedTypes := make(map[string]string)
		needsLocalTypes := false
		for _, p := range byNS[ns] {
			typeRef, imp := tsTypeFromDescRelative(ns, p.Type, p.SchemaName, schemaByID, schemaByName)
			for k := range imp {
				projectionImports[k] = struct{}{}
			}
			if strings.HasPrefix(typeRef, "types.") {
				needsLocalTypes = true
			}
			renderedTypes[p.Component] = typeRef
		}
		if needsLocalTypes {
			b.WriteString("import type * as types from \"./types\";\n")
		}
		var sortedImports []string
		for sourceNs := range projectionImports {
			if sourceNs == ns {
				continue
			}
			sortedImports = append(sortedImports, sourceNs)
		}
		slices.Sort(sortedImports)
		for _, sourceNs := range sortedImports {
			b.WriteString(fmt.Sprintf("import type * as %sTypes from \"../%s/types\";\n", tsIdent(sourceNs), sourceNs))
		}
		b.WriteString("\n")

		for _, p := range byNS[ns] {
			funcName := camelCase(p.Component)
			typeRef := renderedTypes[p.Component]
			b.WriteString(fmt.Sprintf("export async function get%s(client: GosporeClient): Promise<%s> {\n", pascalCase(funcName), typeRef))
			b.WriteString(fmt.Sprintf("  return getProjection<%s>(client, Projections.%s.%s);\n", typeRef, ns, p.Component))
			b.WriteString("}\n\n")
			b.WriteString(fmt.Sprintf("export async function *watch%s(client: GosporeClient): AsyncIterable<%s> {\n", pascalCase(funcName), typeRef))
			b.WriteString(fmt.Sprintf("  yield* watchProjection<%s>(client, Projections.%s.%s);\n", typeRef, ns, p.Component))
			b.WriteString("}\n\n")
		}

		out[ns+"/projection-client.ts"] = b.String()
	}
	return out
}

func renderClients(callables []spore.ManifestCallable, events []schema.EventDecl, schemas []spore.ManifestSchema, header string) map[string]string {
	byNS := make(map[string][]spore.ManifestCallable)
	for _, c := range callables {
		// Single-segment (actor-local) callables group under "local".
		ns := c.Namespace
		if ns == "" {
			ns = "local"
		}
		byNS[ns] = append(byNS[ns], c)
	}
	eventsByNS := make(map[string][]schema.EventDecl)
	for _, e := range events {
		eventsByNS[e.Namespace] = append(eventsByNS[e.Namespace], e)
	}
	schemaByID := make(map[uint64]spore.ManifestSchema, len(schemas))
	schemaByName := make(map[string]spore.ManifestSchema, len(schemas))
	for _, s := range schemas {
		schemaByID[s.SchemaID] = s
		if s.Name != "" {
			schemaByName[s.Name] = s
		}
		if s.Object.Name != "" && s.Object.Name != s.Name {
			schemaByName[s.Object.Name] = s
		}
	}

	nsSet := make(map[string]struct{})
	for ns := range byNS {
		nsSet[ns] = struct{}{}
	}
	for ns := range eventsByNS {
		nsSet[ns] = struct{}{}
	}
	nss := collections.SortedKeys(nsSet)

	out := make(map[string]string)
	for _, ns := range nss {
		cs := byNS[ns]
		es := eventsByNS[ns]
		var b strings.Builder
		if header != "" {
			b.WriteString(header)
			b.WriteString("\n")
		}
		b.WriteString("import { GosporeClient } from \"@qomos/gospore-client\";\n")
		// Emit InvokeOptions import only when at least one unary or
		// streaming callable is rendered for this namespace; events-only
		// namespaces don't need it.
		if len(cs) > 0 {
			b.WriteString("import type { InvokeOptions } from \"@qomos/gospore-client\";\n")
		}
		// Collect imports needed for callable signatures. Local struct types
		// need ./types, while external struct types need imports from sibling
		// namespaces.
		needsLocalTypes := false
		callableImports := make(map[string]struct{})
		for _, c := range cs {
			_, reqImp := tsTypeFromDescRelative(ns, c.Req, "", schemaByID, schemaByName)
			for k := range reqImp {
				callableImports[k] = struct{}{}
			}
			if isLocalStruct(c.Req, ns, schemaByID, schemaByName) {
				needsLocalTypes = true
			}

			_, finalImp := tsTypeFromDescRelative(ns, c.Final, "", schemaByID, schemaByName)
			for k := range finalImp {
				callableImports[k] = struct{}{}
			}
			if isLocalStruct(c.Final, ns, schemaByID, schemaByName) {
				needsLocalTypes = true
			}

			if c.Chunk != nil {
				_, chunkImp := tsTypeFromDescRelative(ns, *c.Chunk, "", schemaByID, schemaByName)
				for k := range chunkImp {
					callableImports[k] = struct{}{}
				}
				if isLocalStruct(*c.Chunk, ns, schemaByID, schemaByName) {
					needsLocalTypes = true
				}
			}
		}
		for _, e := range es {
			if s, ok := schemaByID[e.SchemaID]; ok && s.Name != "" {
				if s.Namespace == ns {
					needsLocalTypes = true
				} else {
					callableImports[s.Namespace] = struct{}{}
				}
			}
		}
		if needsLocalTypes {
			b.WriteString(fmt.Sprintf("import type * as types from \"./types\";\n"))
		}
		var sortedImports []string
		for sourceNs := range callableImports {
			sortedImports = append(sortedImports, sourceNs)
		}
		slices.Sort(sortedImports)
		for _, sourceNs := range sortedImports {
			b.WriteString(fmt.Sprintf("import type * as %sTypes from \"../%s/types\";\n", tsIdent(sourceNs), sourceNs))
		}
		b.WriteString("\n")

		for _, c := range cs {
			funcName := camelCase(c.Name)
			fullCallID := wireCallID(c)
			mode := spore.CallableMode(c.Mode)

			reqType, _ := tsTypeFromDescRelative(ns, c.Req, "", schemaByID, schemaByName)
			resType, _ := tsTypeFromDescRelative(ns, c.Final, "", schemaByID, schemaByName)

			switch mode {
			case spore.CallableModeUnary:
				optsArg := buildOptsArg(c, mode)
				if reqType == "void" {
					b.WriteString(fmt.Sprintf(
						"export async function %s(client: GosporeClient, opts?: InvokeOptions): Promise<%s> {\n",
						funcName, resType))
					b.WriteString(fmt.Sprintf(
						"  return client.invoke<void, %s>(%q, undefined, %s);\n",
						resType, fullCallID, optsArg))
				} else {
					b.WriteString(fmt.Sprintf(
						"export async function %s(client: GosporeClient, req: %s, opts?: InvokeOptions): Promise<%s> {\n",
						funcName, reqType, resType))
					b.WriteString(fmt.Sprintf(
						"  return client.invoke<%s, %s>(%q, req, %s);\n",
						reqType, resType, fullCallID, optsArg))
				}
				b.WriteString("}\n\n")
				if reqType != "void" {
					b.WriteString(fmt.Sprintf("export const %s_meta = {\n", funcName))
					b.WriteString(fmt.Sprintf("  callable: %q,\n", fullCallID))
					b.WriteString(fmt.Sprintf("  name: %q,\n", c.Name))
					if c.ReqSchemaID > 1 {
						b.WriteString(fmt.Sprintf("  reqSchemaId: %d,\n", c.ReqSchemaID))
					}
					if c.FinalSchemaID > 1 {
						b.WriteString(fmt.Sprintf("  resSchemaId: %d,\n", c.FinalSchemaID))
					}
					b.WriteString("} as const;\n\n")
				}

			case spore.CallableModeStreaming:
				optsArg := buildOptsArg(c, mode)
				chunkType := "any"
				if c.Chunk != nil {
					chunkType, _ = tsTypeFromDescRelative(ns, *c.Chunk, "", schemaByID, schemaByName)
				}
				if reqType == "void" {
					b.WriteString(fmt.Sprintf(
						"export async function *%s(client: GosporeClient, opts?: InvokeOptions): AsyncIterable<%s> {\n",
						funcName, chunkType))
					b.WriteString(fmt.Sprintf(
						"  yield* client.subscribe<%s>(%q, undefined, %s);\n",
						chunkType, fullCallID, optsArg))
				} else {
					b.WriteString(fmt.Sprintf(
						"export async function *%s(client: GosporeClient, req: %s, opts?: InvokeOptions): AsyncIterable<%s> {\n",
						funcName, reqType, chunkType))
					b.WriteString(fmt.Sprintf(
						"  yield* client.subscribe<%s>(%q, req, %s);\n",
						chunkType, fullCallID, optsArg))
				}
				b.WriteString("}\n\n")
			}
		}

		for _, e := range es {
			payloadType := "any"
			if s, ok := schemaByID[e.SchemaID]; ok && s.Name != "" {
				if s.Namespace == ns {
					payloadType = fmt.Sprintf("types.%s", s.Name)
				} else {
					payloadType = fmt.Sprintf("%sTypes.%s", tsIdent(s.Namespace), s.Name)
				}
			}
			eventName := pascalCase(e.Kind)
			b.WriteString(fmt.Sprintf("export type %sHandler = (payload: %s) => void;\n\n", eventName, payloadType))
			if e.ServiceName != "" {
				b.WriteString(fmt.Sprintf("export function On%s(client: GosporeClient, handler: %sHandler): () => void {\n", eventName, eventName))
				b.WriteString(fmt.Sprintf("  return client.events.onService(%q, %q, (payload) => handler(payload as %s));\n", e.ServiceName, e.Kind, payloadType))
			} else {
				b.WriteString(fmt.Sprintf("export function On%s(client: GosporeClient, actorId: string, handler: %sHandler): () => void {\n", eventName, eventName))
				b.WriteString(fmt.Sprintf("  return client.events.onInstance(actorId, %q, (payload) => handler(payload as %s));\n", e.Kind, payloadType))
			}
			b.WriteString("}\n\n")
			b.WriteString(fmt.Sprintf("export function Off%s(cancel: () => void): void {\n", eventName))
			b.WriteString("  cancel();\n")
			b.WriteString("}\n\n")
		}

		out[ns+"/client.ts"] = b.String()
	}
	return out
}

// buildOptsArg generates the opts expression for a callable invocation,
// automatically injecting reqSchemaId and resSchemaId when they are known.
func buildOptsArg(c spore.ManifestCallable, mode spore.CallableMode) string {
	var parts []string
	if c.ReqSchemaID > 1 {
		parts = append(parts, fmt.Sprintf("reqSchemaId: %d", c.ReqSchemaID))
	}
	if mode == spore.CallableModeStreaming {
		if c.ChunkSchemaID > 1 {
			parts = append(parts, fmt.Sprintf("chunkSchemaId: %d", c.ChunkSchemaID))
		}
	} else {
		if c.FinalSchemaID > 1 {
			parts = append(parts, fmt.Sprintf("resSchemaId: %d", c.FinalSchemaID))
		}
	}
	if len(parts) > 0 {
		return fmt.Sprintf("{ %s, ...opts }", strings.Join(parts, ", "))
	}
	return "opts"
}

func tsTypeFromDesc(td spore.TypeDesc) string {
	switch td.Kind {
	case spore.TypeKindVoid:
		return "void"
	case spore.TypeKindStruct:
		if td.ClassName != "" {
			return "types." + td.ClassName
		}
		if td.Name != "" {
			return "types." + td.Name
		}
		return "any"
	case spore.TypeKindArray:
		if td.Element != nil {
			return tsTypeFromDesc(*td.Element) + "[]"
		}
		return "any[]"
	case spore.TypeKindScalar:
		return tsScalarType(td.Name)
	case spore.TypeKindMap:
		if td.Element != nil {
			return "Record<string, " + tsTypeFromDesc(*td.Element) + ">"
		}
		return "Record<string, any>"
	default:
		return "any"
	}
}

func tsScalarType(name string) string {
	switch name {
	case "string":
		return "string"
	case "int", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64", "float32", "float64":
		return "number"
	case "bool":
		return "boolean"
	case "any":
		return "any"
	case "bytes":
		return "Uint8Array"
	case "null":
		return "null"
	case "":
		return "unknown"
	default:
		return name
	}
}

// tsTypeFromDescRelative renders a TypeDesc relative to the given namespace.
// It returns the TypeScript type string and a set of external namespaces that
// must be imported. It is used for event and projection payload types whose
// schema may live in a different namespace (e.g. system) than the callable.
func tsTypeFromDescRelative(ns string, td spore.TypeDesc, schemaName string, schemaByID map[uint64]spore.ManifestSchema, schemaByName map[string]spore.ManifestSchema) (string, map[string]struct{}) {
	imports := make(map[string]struct{})
	switch td.Kind {
	case spore.TypeKindVoid:
		return "void", imports
	case spore.TypeKindStruct:
		name := td.ClassName
		if name == "" {
			name = td.Name
		}
		if name == "" {
			name = schemaName
		}
		if name == "" {
			return "any", imports
		}
		var s spore.ManifestSchema
		var ok bool
		if td.ClassID != 0 {
			s, ok = schemaByID[td.ClassID]
		}
		if !ok {
			s, ok = schemaByName[name]
		}
		if !ok {
			return "any", imports
		}
		if s.Namespace == ns {
			return s.Name, imports
		}
		imports[s.Namespace] = struct{}{}
		return tsIdent(s.Namespace) + "Types." + s.Name, imports
	case spore.TypeKindArray:
		if td.Element != nil {
			inner, imp := tsTypeFromDescRelative(ns, *td.Element, "", schemaByID, schemaByName)
			for k := range imp {
				imports[k] = struct{}{}
			}
			return inner + "[]", imports
		}
		return "any[]", imports
	case spore.TypeKindScalar:
		return tsScalarType(td.Name), imports
	case spore.TypeKindMap:
		if td.Element != nil {
			v, imp := tsTypeFromDescRelative(ns, *td.Element, "", schemaByID, schemaByName)
			for k := range imp {
				imports[k] = struct{}{}
			}
			return "Record<string, " + v + ">", imports
		}
		return "Record<string, any>", imports
	default:
		return "any", imports
	}
}

// isLocalStruct reports whether td is a struct type whose canonical namespace
// is ns. It is used when deciding whether a callable signature needs to
// import ./types.
func isLocalStruct(td spore.TypeDesc, ns string, schemaByID map[uint64]spore.ManifestSchema, schemaByName map[string]spore.ManifestSchema) bool {
	if td.Kind != spore.TypeKindStruct {
		return false
	}
	if td.ClassID != 0 {
		if s, ok := schemaByID[td.ClassID]; ok {
			return s.Namespace == ns
		}
	}
	name := td.ClassName
	if name == "" {
		name = td.Name
	}
	if name == "" {
		return false
	}
	if s, ok := schemaByName[name]; ok {
		return s.Namespace == ns
	}
	return false
}
