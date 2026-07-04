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

- Backend gate — `engines/crewai/kernel/a2ui_runner.py::compose_surface` drops a
  `dashboard`/`document` surface that has **no deliverable component** (`_has_data_component`
  / `_DATA_COMPONENTS`). This stops prose-only surfaces from double-rendering **but** means
  any new deliverable component MUST be added to `_DATA_COMPONENTS` or its surface gets
  dropped back to plain text (this is the "Album rendered as markdown, not a carousel" bug).
- Frontend drop — `components/ChatMode/store/executionStore.ts::completeExecution` posts an
  empty message body when a surface exists (`const body = surface ? '' : resultText`).
- See the memories `chatmode-surface-canonical-drop-text` and
  `chatmode-a2ui-only-derive-from-content`.

## Adding / editing a component — the full checklist

Do ALL that apply. Frontend paths are under `src/frontend/src`; backend under
`src/backend/src`.

1. **Renderer** — `shared/a2ui/components.tsx`. Implement `function Foo({node, render, resolve}: NodeProps)`.
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
4. **Catalog** — `backend/src/shared/a2ui/catalog.json`. Add `{summary, props}` so the composer
   is *allowed* to emit it. Read live at export (no vendoring).
5. **Composer prompt** — `backend/src/shared/a2ui/compose.py`:
   - Add a routing line to **rule 5** (surfaceKind + root component selection) and/or the
     special-components block (**rule 11**) telling the model when/how to emit it.
   - Add trigger words to `RICH_INTENT` (so a chat request even *invokes* the composer).
   - Add to `DELIVERABLE_KEYWORDS` if it's its own deliverable (order matters — specific
     multi-word keys before bare ones, e.g. `network graph` before a bare `graph`).
6. **Prose gate** — `backend/src/engines/crewai/kernel/a2ui_runner.py`: add the component to
   `_DATA_COMPONENTS` if it's a genuine deliverable (chart/table/diagram/gallery/map), or its
   `dashboard`/`document` surface will be dropped as "prose-only".
7. **Legacy adapter** — `components/ChatMode/utils/surfaceAdapter.ts`: add to `UiComponentType`
   + `VALID_TYPES`. If it's a deliverable, add to `DELIVERABLE_BY_COMPONENT` +
   `DELIVERABLE_TO_SURFACE_KIND`.
8. **UIConfigurator (if it should be brandable / configurable)** — this is the step most
   often missed:
   - `components/Configuration/uiConfigShared.ts` — add to `DELIVERABLE_TYPES` (the list shown
     in "Branding & per-type settings") and `TYPE_OPTIONS` (per-type controls; each carries a
     `phrase()` that becomes the composer directive).
   - `components/ChatMode/components/Chat/A2uiSurface.tsx` — add to `ROOT_COMPONENT_TO_DELIVERABLE`
     so a surface whose ROOT is this component resolves its per-type palette (otherwise it
     inherits the dashboard/document palette).
9. **New surfaceKind ONLY** (skip if it's a component): `catalog.json` `surfaceKinds`,
   `A2uiSurface.tsx` `SURFACE_TO_DELIVERABLE` (+ token-vs-deck theming set), the export
   `App.tsx` `RICH` set (`test_a2ui_rich_surface_kinds_cover_live_renderer` guards it), and
   `surfaceAdapter.ts` maps.
10. **Exported app — RE-VENDOR (do not forget):** the exported Databricks App ships its OWN
    byte-identical copy of the renderer.
    - Copy every changed `shared/a2ui/*` file (NOT `*.test.*`) to
      `backend/src/engines/crewai/exporters/templates/databricks_app/frontend/src/a2ui/`.
      `test_vendor_in_sync_with_frontend_source` fails until you do.
    - `catalog.json` and `compose.py` are copied **live** at export time — no vendoring.
    - Export parity (kept in sync with Kasal chat — preserve when editing the template):
      the double-render dedup (`App.tsx` shows the surface XOR the text bubble), the prose
      gate (`agent.py::_schedule_a2ui` drops prose-only dashboard/document surfaces via
      `_a2ui_has_data_component`), and palette-by-root-component (`App.tsx`
      `ROOT_COMPONENT_TO_DELIVERABLE` / `deliverableForSurface`).
    - See memory `a2ui-renderer-vendored-copy`.
11. **Tests:**
    - Frontend: a render test in `shared/a2ui/components.deliverables.test.tsx`. SVG components
      are fully assertable; **recharts components need a `ResizeObserver` polyfill** in jsdom
      (see the top of that file) and can't assert SVG internals (0-size container) — assert the
      title + empty-guard instead.
    - Backend: extend `test_a2ui_runner.py` (`_has_data_component`, `compose_surface` keep/drop)
      and `tests/unit/shared/a2ui/` (catalog/keywords). Every change ships with a regression test.

## Quick file map

| Concern | File |
|---|---|
| Component renderers | `frontend/src/shared/a2ui/components.tsx` |
| Registry (name→renderer) | `frontend/src/shared/a2ui/registry.tsx` |
| Composer + prompt + intent + deliverable keywords | `backend/src/shared/a2ui/compose.py` |
| Catalog (what the model may emit) | `backend/src/shared/a2ui/catalog.json` |
| Prose gate / `{text,a2ui}` envelope build | `backend/src/engines/crewai/kernel/a2ui_runner.py` |
| Legacy parse + component/deliverable maps | `frontend/src/components/ChatMode/utils/surfaceAdapter.ts` |
| Per-type branding list + settings | `frontend/src/components/Configuration/uiConfigShared.ts` |
| Palette resolution + root→deliverable | `frontend/src/components/ChatMode/components/Chat/A2uiSurface.tsx` |
| Text/surface dedup on completion | `frontend/src/components/ChatMode/store/executionStore.ts` |
| Exported-app vendored renderer | `backend/src/engines/crewai/exporters/templates/databricks_app/frontend/src/a2ui/` |
| Exported-app composition / rendering | `…/templates/databricks_app/agent_server/agent.py`, `…/frontend/src/App.tsx` |
