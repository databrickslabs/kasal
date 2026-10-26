# A2UI CLAUDE.md

Instructions for the **A2UI generative-UI system** — the shared renderer + composer
that turns an agent's answer into a rich, branded surface (dashboard, presentation,
quiz, forecast, graph, …). This system is split across **frontend, backend, and the
exported-app template**, so a change in one place almost always needs matching changes
in the others. Read this before adding or editing an A2UI component — it is easy to miss
a touchpoint (the UIConfigurator and the exported app are the ones most often forgotten).

## Mental model — three concepts, don't conflate them

- **Component** — a renderable node (`Chart`, `Table`, `Forecast`, `Album`, …). Lives in
  the renderer registry + the composer catalog. Components compose into a surface.
- **surfaceKind** — the container the renderer draws (`dashboard`, `document`,
  `presentation`, `mindmap`, `quiz`, `flashcards`, `map`, `conversation`). A component
  usually lives *inside* a `dashboard`/`document`; only a few are their own surfaceKind.
- **Deliverable** — the UIConfigurator branding/settings bucket (`dashboard`, `genie`,
  `report`, `forecast`, …). **Decoupled from surfaceKind** (e.g. `genie`/`report`/
  `forecast` are deliverables that render inside a `document`). Drives the per-type
  palette + the composer directives.

**Decision when adding something new:** prefer a **component** inside `dashboard`/
`document` (small blast radius — what Forecast/Graph/Sequence/Album do). Only make it a
new **surfaceKind** if it needs a full dedicated canvas (like mindmap/quiz) — that
touches many more places (see step 9).

## The `{text, a2ui}` envelope + the double-render rule

A completed run persists either a plain string or a `{text, a2ui: Surface}` envelope.
The surface **is** the canonical rendering, so the raw text must NOT also show:

- Backend gate — `services/a2ui/runner.py::compose_surface` drops a
  `dashboard`/`document` surface that has **no deliverable component** (`_has_data_component`,
  over `DATA_COMPONENTS` in `services/a2ui/stream.py`, which runner.py imports as
  `_DATA_COMPONENTS`). This stops prose-only surfaces from double-rendering **but** means
  any new deliverable component MUST be added to `stream.py::DATA_COMPONENTS` or its surface
  gets dropped back to plain text (this is the "Album rendered as markdown, not a carousel" bug).
- Frontend drop — `features/chat/store/executionStore.ts::completeExecution` posts an
  empty message body when a surface exists **and the reader never saw the text**
  (`const body = surface && !readerSawText ? '' : resultText`). The condition matters: a
  composed surface can take tens of seconds, and Kasal chat streams the answer meanwhile
  precisely so there is something to read. Blanking that at the end reads as the answer
  being retracted — and when the surface is a dashboard carrying only headline numbers,
  most of the answer goes with it. Same rule `attachSurface` states for a late surface:
  never take away what the reader has already seen, never add what they never saw.
- See the memories `chatmode-surface-canonical-drop-text` and
  `chatmode-a2ui-only-derive-from-content`.

## Adding / editing a component — the full checklist

Do ALL that apply. Frontend paths are under `src/frontend/src`; backend under
`src/backend/src`.

1. **Renderer** — `shared/a2ui/components/` (a directory, split by concern: `primitives.tsx`,
   `data.tsx`, `diagrams.tsx`, `media.tsx`, `geo.tsx`, `interactive.tsx`, `kanban.tsx`,
   `mindmap.tsx`, …, re-exported from `components/index.ts`). Put the component in the file
   for its concern and export it from `index.ts`. Implement
   `function Foo({node, render, resolve}: NodeProps)`.
   - Resolve bindings with `resolve(node.x)` — literals AND `{path:"/k"}` both work. **A prop
     that can be data MUST go through `resolve`** (missing this is why the Table header was
     blank when `columns` was bound). Coerce with `asStr` / `asArr` / `asNum`.
   - Theme from `useContext(DeckThemeContext)` + `seriesFromAccent(theme.accent, n)` for
     series colors. Never hardcode colors (UIConfigurator is the source of truth).
   - **Dependencies must be self-contained**: `react`, `recharts`, `lucide-react`, `react-markdown`
     are already declared. Diagrams → plain SVG (no dep). Adding a new npm import means adding
     it to the export `frontend/package.json` template AND it must pass
     `test_a2ui_frontend_imports_are_declared_deps`.
2. **Registry** — `shared/a2ui/registry.tsx`. Map `Name -> Component`. Unregistered names
   render as `Unsupported`.
3. **Wire types** — `shared/a2ui/types.ts` only if you add new wire shapes (usually not).
4. **Catalog** — `backend/src/services/a2ui/catalog.json`. Add `{summary, props}` so the composer
   is *allowed* to emit it. Read live at export (no vendoring).
5. **Composer prompt** — `backend/src/services/a2ui/compose.py`:
   - Add a routing line to **rule 5** (surfaceKind + root component selection) and/or the
     special-components block (**rule 11**) telling the model when/how to emit it.
   - Add trigger words to `RICH_INTENT` (so a chat request even *invokes* the composer).
   - Add to `DELIVERABLE_KEYWORDS` if it's its own deliverable (order matters — specific
     multi-word keys before bare ones, e.g. `network graph` before a bare `graph`).
6. **Prose gate** — `backend/src/services/a2ui/stream.py`: add the component to
   `DATA_COMPONENTS` (runner.py imports it as `_DATA_COMPONENTS`) if it's a genuine
   deliverable (chart/table/diagram/gallery/map), or its `dashboard`/`document` surface will
   be dropped as "prose-only".
7. **Legacy adapter** — `features/chat/utils/surfaceAdapter.ts`: add to `UiComponentType`
   + `VALID_TYPES`. If it's a deliverable, add to `DELIVERABLE_BY_COMPONENT` +
   `DELIVERABLE_TO_SURFACE_KIND`.
8. **UIConfigurator (if it should be brandable / configurable)** — this is the step most
   often missed:
   - `features/configuration/components/uiConfigShared.ts` — add to `DELIVERABLE_TYPES` (the list shown
     in "Branding & per-type settings") and `TYPE_OPTIONS` (per-type controls; each carries a
     `phrase()` that becomes the composer directive).
   - `features/chat/components/Chat/A2uiSurface.tsx` — add to `ROOT_COMPONENT_TO_DELIVERABLE`
     so a surface whose ROOT is this component resolves its per-type palette (otherwise it
     inherits the dashboard/document palette).
9. **New surfaceKind ONLY** (skip if it's a component): `catalog.json` `surfaceKinds`,
   `A2uiSurface.tsx` `SURFACE_TO_DELIVERABLE` (+ token-vs-deck theming set), the export
   `frontend/src/App.tsx` `RICH` set (`test_a2ui_rich_surface_kinds_cover_live_renderer` guards it), and
   `surfaceAdapter.ts` maps.
10. **Exported app — RE-VENDOR (do not forget):** the exported Databricks App ships its OWN
    byte-identical copy of the renderer.
    - Copy every changed `shared/a2ui/**` file (NOT `*.test.*`, NOT this CLAUDE.md), keeping
      the same relative path (e.g. `components/data.tsx`), to
      `backend/src/services/export/templates/databricks_app/frontend/src/a2ui/`.
      `test_vendor_in_sync_with_frontend_source`
      (`tests/unit/services/export/test_databricks_app_exporter.py`) fails until you do.
    - `catalog.json` and `compose.py` are copied **live** at export time — no vendoring.
    - Export parity (kept in sync with Kasal chat — preserve when editing the template):
      the double-render dedup (`frontend/src/App.tsx` shows the surface XOR the text bubble — the
      exported app does NOT stream, so the reader never sees the text first and the plain
      XOR stays correct there; this is the one place the two intentionally differ), the prose
      gate (`agent.py::_schedule_a2ui` drops prose-only dashboard/document surfaces via
      `_a2ui_has_data_component`), and palette-by-root-component (`frontend/src/App.tsx`
      `ROOT_COMPONENT_TO_DELIVERABLE` / `deliverableForSurface`).
    - See memory `a2ui-renderer-vendored-copy`.
11. **Tests:**
    - Frontend: a render test in `shared/a2ui/components.deliverables.test.tsx`. SVG components
      are fully assertable; **recharts components need a `ResizeObserver` polyfill** in jsdom
      (see the top of that file) and can't assert SVG internals (0-size container) — assert the
      title + empty-guard instead.
    - Backend: extend `tests/unit/services/a2ui/test_runner.py` (`_has_data_component`,
      `compose_surface` keep/drop) and the other tests in `tests/unit/services/a2ui/`
      (catalog/keywords/intent, e.g. `test_compose_resolvers.py`, `test_wants_rich_surface.py`).
      Every change ships with a regression test.

## Quick file map

| Concern | File |
|---|---|
| Component renderers | `frontend/src/shared/a2ui/components/` (by concern; `index.ts` re-exports) |
| Registry (name→renderer) | `frontend/src/shared/a2ui/registry.tsx` |
| Composer + prompt + intent + deliverable keywords | `backend/src/services/a2ui/compose.py` |
| Catalog (what the model may emit) | `backend/src/services/a2ui/catalog.json` |
| Prose gate / `{text,a2ui}` envelope build | `backend/src/services/a2ui/runner.py` (`DATA_COMPONENTS` lives in `stream.py`) |
| Legacy parse + component/deliverable maps | `frontend/src/features/chat/utils/surfaceAdapter.ts` |
| Per-type branding list + settings | `frontend/src/features/configuration/components/uiConfigShared.ts` |
| Palette resolution + root→deliverable | `frontend/src/features/chat/components/Chat/A2uiSurface.tsx` |
| Text/surface dedup on completion | `frontend/src/features/chat/store/executionStore.ts` |
| Exported-app vendored renderer | `backend/src/services/export/templates/databricks_app/frontend/src/a2ui/` |
| Exported-app composition / rendering | `…/templates/databricks_app/agent_server/agent.py`, `…/templates/databricks_app/frontend/src/App.tsx` |
