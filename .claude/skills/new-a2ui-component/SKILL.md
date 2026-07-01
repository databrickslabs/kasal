---
name: new-a2ui-component
description: Add a new A2UI generative-UI component to Kasal — register it in the shared catalog (backend contract), implement its React renderer, and wire the registry, so agents can emit it in chat surfaces and it ships to exported Databricks apps. Use when adding a new declarative UI block (chart type, card, interactive widget, etc.) that agents render.
---

# new-a2ui-component

A2UI is Kasal's generative-UI system: the backend "composer" asks an LLM to emit a
declarative **Surface** (`{surfaceKind, root, components[], dataModel}`), and the
frontend renders it. Components are the building blocks agents can emit (Markdown,
Chart, SlideDeck, Quiz, Map, ...). This skill adds a new one.

## The one contract, two sides

A2UI has a **single shared contract** consumed by both sides. Adding a component
means keeping them in lockstep:

1. **Backend catalog** — `src/backend/src/shared/a2ui/catalog.json`. The composer is
   told "here are the components you may emit" from this file. If a component is not
   in the catalog, the model is never allowed to produce it.
2. **Frontend registry + renderer** — `src/frontend/src/shared/a2ui/`. The renderer
   turns each emitted `ComponentNode` into React. A component in the catalog but not
   in the registry renders as `Unsupported`.

Both trees are the **single source of truth**: the exporter vendors
`shared/a2ui/compose.py` + `catalog.json` into `agent_server/a2ui/` and the whole
`frontend/src/shared/a2ui/` tree into exported Databricks apps **verbatim**. So a
component you add here automatically ships to exported apps — **do not** create a
second copy in the export templates (drifted copies were deliberately removed).

## Steps

### 1. Add the component to the backend catalog
`src/backend/src/shared/a2ui/catalog.json` → `components` object. Add an entry:
```json
"Timeline": {
  "summary": "A vertical timeline of dated events. events is a list of {date, title, description}. Use for chronologies and roadmaps.",
  "props": {
    "title": { "type": "string", "binding": true },
    "events": {
      "type": "array", "binding": true,
      "shape": "[{ date, title, description }]"
    }
  }
}
```
- The `summary` is prompt text the LLM reads — describe the component AND the exact
  data shape (say "supply the data, not a description of it", as Quiz/Flashcards do).
- Prop `type` is one of: `string`, `int`, `bool`, `array`, `object`, `enum`
  (+ `values`), `componentIds` (child ids for containers).
- `"binding": true` means the value may be a literal OR a `{path}` binding into
  `dataModel` (resolved by `resolve.ts`). Container children use
  `"type": "componentIds"`.
- If the component is a **new surface root** (like SlideDeck/Quiz/Map), also add its
  `surfaceKind` to the `surfaceKinds` array, and mention "Use as the root for
  surfaceKind '<kind>'" in the summary.

### 2. Decide catalog presets
- If the component should be available in the **minimal** preset (prose/structure
  only), add its name to `MINIMAL_COMPONENTS` in
  `src/backend/src/shared/a2ui/compose.py`. Rich/interactive components (charts,
  decks, quizzes) normally stay OUT of minimal.

### 3. Implement the React renderer
`src/frontend/src/shared/a2ui/components.tsx`. Export a component with the
`NodeProps` signature (`{ node, render, resolve }`):
```tsx
export function Timeline({ node, resolve }: NodeProps) {
  const title = asStr(resolve(node.title))
  const events = asArr(resolve(node.events))
  return (/* Tailwind + shadcn ui/ primitives only */)
}
```
- Use `resolve(node.<prop>)` for every prop that is `binding: true` (a prop may
  arrive as a literal or a `{path}` binding — `resolve` handles both).
- For container components, call `render(childId)` for each id in `node.children`.
- **Self-containment rule**: this whole tree ships verbatim into exports and must use
  only **relative imports** (no `@/` alias) and its own `lib/` + `ui/` primitives
  (shadcn `Card`, `Button`, Tailwind `--a2-*` tokens). Do not import from elsewhere
  in the frontend app — that would break the export.
- Reuse existing helpers in the file: `asStr`, `asArr`, `cn`, deck theming
  (`DeckThemeContext`), `SurfaceContext`.

### 4. Register the renderer
`src/frontend/src/shared/a2ui/registry.tsx`:
- Import your component and add it to the `registry` map. The **key must equal the
  catalog component name** (e.g. `Timeline`). If the renderer's exported name
  differs from the catalog name, alias it (see `Map: GeoMap`).

### 5. (Only if it introduces a new surfaceKind) container styling
`src/frontend/src/shared/a2ui/A2UIRenderer.tsx` → add an entry to `SURFACE_CLASS`
for the new `surfaceKind` (its container className). Unknown kinds fall back to the
document container, so this is optional for components that live inside an existing
surface.

### 6. Export renderer as needed
If other modules need the type, it flows through `src/frontend/src/shared/a2ui/index.ts`
(the public barrel). Component renderers themselves are internal to the registry —
you usually don't touch `index.ts` unless you add a new exported type.

## Tests (colocate, Vitest)
Add a `*.test.tsx` next to `components.tsx` (see `components.slide.test.tsx`,
`components.deliverables.test.tsx`): build a minimal `Surface` payload that uses your
component and assert it renders the expected content (and resolves a binding).

## Validation before done
```bash
# frontend renderer + registry
cd src/frontend && npm run tsc && npm run test:run -- src/shared/a2ui
# backend catalog is valid JSON and loads
cd ../backend && .venv/bin/python -c "from src.shared.a2ui.compose import load_catalog; c=load_catalog(); assert 'Timeline' in c['components']; print('catalog ok')"
# export still vendors cleanly (no drifted second copy)
.venv/bin/python run_tests.py --type unit -k a2ui
```

## Common footguns
- **Catalog/registry name mismatch** → renders as `Unsupported`. The catalog key and
  the `registry` key must be identical (alias if the function name differs).
- **Forgetting `resolve()`** on a `binding: true` prop → you render a raw `{path}`
  object instead of the value.
- **Non-relative import / app-only dependency** in the renderer → breaks the exported
  Databricks app (the tree must stay self-contained).
- **Editing a template copy** under `exporters/templates/databricks_app/` instead of
  the shared tree → drift; the exporter vendors the shared tree, not the template.
- **New surfaceKind not added** to both the catalog `surfaceKinds` array and
  (optionally) `SURFACE_CLASS`.

See `src/backend/src/shared/a2ui/catalog.json` (the contract),
`src/frontend/src/shared/a2ui/registry.tsx`, and the exporter's `_a2ui_vendor_files`
/ `_a2ui_frontend_files` in `engines/crewai/exporters/databricks_app_exporter.py`.
