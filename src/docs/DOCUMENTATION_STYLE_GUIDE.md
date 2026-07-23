# Documentation style guide

How to write and structure docs in `src/docs/`. These docs are rendered on GitHub and served in-app (the frontend copies `src/docs/` to `public/docs/`), so every rule below has to hold in both renderers. Follow it for new pages and when you touch an existing one.

This guide is itself a reference page. For the rationale behind the four-mode model, see [Diátaxis](https://diataxis.fr/) and the [Write the Docs guide](https://www.writethedocs.org/guide/writing/docs-principles/).

---

## 1. Organize by what the reader is doing (Diátaxis)

Every page serves exactly one of four needs. Do not blend them in one page — a tutorial that drifts into API tables, or a reference that teaches, helps no one. When a page mixes modes, split it.

| Mode | Reader need | Answers | Kasal examples |
|------|-------------|---------|----------------|
| **Get started / Tutorials** | Learn by doing | "Teach me, I'm new" | `END_USER_TUTORIAL_CATALOG.md`, `powerbi/02-simple-migration-story.md`, the "Getting started in 30 seconds" block |
| **How-to guides** | Reach a specific goal | "How do I do X?" | `crew-export-deployment.md`, `lakebase-deployment.md`, `mlflow-tracing-setup.md`, `powerbi/01-authentication-setup.md`, `powerbi/ucmv-migration-guide.md` |
| **Reference** | Look up exact facts | "What are the exact params/flags/errors?" | `api_endpoints.md`, `powerbi/README.md` + the `tool-*.md` set, `CODE_STRUCTURE_GUIDE.md`, `UCMV_PIPELINE_CONFIG_GUIDE.md` |
| **Concepts / Explanation** | Understand the why | "Why is it built this way?" | `WHY_KASAL.md`, `ARCHITECTURE_GUIDE.md`, `crewai-engine-refactor-proposal.md` |

Quick test for an existing page ([Diátaxis compass](https://diataxis.fr/compass/)): is it **action or cognition**? Is it for **studying or working**? Action+study = tutorial; action+work = how-to; cognition+work = reference; cognition+study = explanation.

**Per-mode page rules:**

- **Tutorial** ([ref](https://diataxis.fr/tutorials/)): state the outcome up front ("By the end you'll have…"); numbered, fully reproducible steps; tell the reader what to notice; minimal explanation, link out for depth. No option lists, no alternatives.
- **How-to** ([ref](https://diataxis.fr/how-to-guides/)): action-titled ("How to export a crew to a Databricks App"); start with a `## Before you begin` prerequisites list; conditional imperatives ("If you want X, do Y"); assume competence; defer full option lists to reference.
- **Reference** ([ref](https://diataxis.fr/reference/)): structure mirrors the product/code; austere and factual; list every param/flag/error with a short example; no teaching, no opinion.
- **Concept** ([ref](https://diataxis.fr/explanation/)): discuss design decisions, trade-offs, connections; no step-by-step instructions, no exhaustive specs.

Don't build empty four-bucket scaffolding for its own sake — improve pages one at a time. But when you add a new doc, put it where its mode belongs.

---

## 2. Page anatomy

Every page follows this skeleton:

1. **One H1 title** (`# Title`), sentence case, matching the topic. Exactly one H1 per page; the body starts at H2. (Note: `DEVELOPER_GUIDE.md` currently opens at H2 — fix when you touch it.)
2. **One-line summary** directly under the H1: what this page is and who it's for. Don't just restate the title.
3. **Optional TOC** for long pages (roughly more than one screen of H2s): a bullet list of `[Section](#slug)` links placed after the summary, before the first H2.
4. **Body**, organized by H2/H3 per the mode rules above.
5. **`## Related` or `## Next steps`** at the end (use `## See also` on reference pages): up to 5 curated links. How-tos link to the concepts they apply and the reference they use; concepts link to the how-tos that apply them; tutorials link to the next step.
6. **Link back to the hub**: end with a line linking to [`README.md`](./README.md) (or the section's own `README.md`).

How-to and tutorial pages additionally start with a `## Before you begin` prerequisites list. Tutorials end with `## Cleanup` when they create resources. (Page-type skeletons follow the [Kubernetes content templates](https://kubernetes.io/docs/contribute/style/page-content-types/).)

---

## 3. Hyperlinking

Cross-link liberally — heavy internal linking is what turns a flat folder into a navigable graph. The first mention of a named Kasal concept, tool, or API on a page should link to its canonical doc.

- **Descriptive link text** — the destination's title or a phrase naming it. Never "click here", "this doc", "here", or a bare URL. ([Google](https://developers.google.com/style/cross-references))
  - Right: `see the [API endpoints reference](./api_endpoints.md)`
  - Wrong: `see [here](./api_endpoints.md)`
- **Relative `.md` links** between docs, no leading slash, preferring same-or-child directory. `[API reference](./api_endpoints.md)`, `[parent hub](../README.md)`. GitHub and the in-app renderer both resolve these; absolute site URLs and `.html` paths break. ([GitHub](https://docs.github.com/en/get-started/writing-on-github/getting-started-with-writing-and-formatting-on-github/basic-writing-and-formatting-syntax))
- **Minimize `../` chains** — they break first when files move.
- **External / cross-repo links** use full `https://` URLs (relative `.md` only resolves inside this tree). Raw HTML `<a href>` is not rewritten by renderers — always use Markdown `[]()` syntax for internal links.
- **Anchor links** use `path.md#heading-slug`. Compute the slug mechanically: lowercase, strip punctuation, spaces → `-` (so `## Before you begin` → `#before-you-begin`). Duplicate headings on a page get `-1`, `-2` suffixes. Don't hand-write anchors that don't match a real heading.
- **Keep each link on one line**, even when long. Use a "For more information, see…" lead-in for consistency.

---

## 4. Headings, code, callouts, tables

**Headings**
- Sentence case: capitalize only the first word and proper nouns ("Export a crew", not "Export A Crew"). ([Google](https://developers.google.com/style/headings))
- Don't skip levels (no H2 → H4); increment one at a time. Avoid going deeper than H4 — split the page instead.
- No links, no trailing punctuation, no bold/code inside headings (they break anchors).
- Task headings use a bare infinitive ("Create a workflow"); concept headings use noun phrases ("Execution lifecycle"). Avoid starting with an "-ing" gerund.
- Keep headings stable — don't put step numbers or volatile text in them; changing a heading breaks every inbound anchor.

**Code**
- Inline code (single backticks) for file names, paths, function/class names, env vars, CLI commands, and API fields: `crew_preparation.py`, `uv sync`, `AIRLLM_BASE_URL`.
- Fenced code blocks for snippets, always with a language tag: ```` ```python ````, ```` ```bash ````, ```` ```json ````. Bare ```` ``` ```` is not allowed.
- Precede a code block with an introductory sentence ending in a colon.
- Never put real URLs, hosts, tokens, or workspace IDs in samples — use placeholders like `https://example.com`, `<your-app>.databricksapps.com`, `<workspace-id>` (per the project rule in `CLAUDE.md`).

**Callouts** — use GitHub alert syntax, sparingly (at most one or two per page; readers skip boxes). Put anything required for success in the body text, not a callout.

```markdown
> [!NOTE] Useful but non-critical context.
> [!TIP] Optional shortcut.
> [!IMPORTANT] Essential to get right.
> [!WARNING] Risk of data loss or irreversible action.
```

Don't use callouts for prerequisites, steps, or cross-references — those belong in the body.

**Tables** — use only for items with 3+ data points each; use lists for simpler data. Sentence-case column headers, no trailing punctuation, no merged cells, no blank cells (use "None" / "Not applicable"). Introduce a table with a complete sentence.

**Lists** — numbered for sequences, bulleted otherwise. Keep items parallel (all start with a verb, or all nouns). Introduce with a complete sentence.

**Voice** — second person ("you"), active voice, present tense, imperative for instructions ("Click **Run**"). Avoid "will" for current behavior and marketing superlatives.

---

## 5. Files, folders, and section indexes

- **kebab-case** for new file and folder names: `crew-export-deployment.md`, `powerbi/`. No spaces, capitals, or underscores in anything that generates a path. (Existing `SCREAMING_SNAKE` files — `DEVELOPER_GUIDE.md`, `WHY_KASAL.md` — stay as-is to avoid breaking inbound links; new files follow kebab-case. Don't rename without updating every link to them.)
- **One concept per file**; the filename should echo the H1.
- **Each subfolder gets a `README.md`** that states the section's scope, links to every page in the folder, and links back up to the parent hub. GitHub renders it when browsing the directory; the in-app nav uses it as the default page. `powerbi/README.md` is the model.
- **`src/docs/README.md` is the top-level hub** — mostly navigation: a short intro, then grouped links to section pages. Keep its links as relative `.md` paths and keep them in sync when you add or move a page.
- **Images** live in `images/` (already present); reference them with relative paths.

---

## 6. Per-page checklist

Before opening a PR, confirm each page:

- [ ] Serves a single Diátaxis mode (tutorial / how-to / reference / concept) and sits in the matching area.
- [ ] Has exactly one H1, sentence case, with a one-line summary under it.
- [ ] Headings increment one level at a time, sentence case, no links/punctuation/code in them.
- [ ] How-to and tutorial pages open with `## Before you begin`.
- [ ] Long pages have a TOC after the summary.
- [ ] Ends with `## Related` / `## Next steps` (or `## See also` for reference), ≤5 links, plus a link back to the hub `README.md`.
- [ ] Links use descriptive text (no "click here") and relative `.md` paths; anchors match real heading slugs.
- [ ] Every fenced code block has a language tag; inline code used for names/paths/commands.
- [ ] No real URLs, hosts, secrets, or workspace IDs — placeholders only.
- [ ] Callouts limited to one or two; nothing required-for-success hidden in a callout.
- [ ] New files are kebab-case; new subfolders have a `README.md` index; the hub links to the page.

---

## References

- Diátaxis: <https://diataxis.fr/> · [compass](https://diataxis.fr/compass/) · page types ([tutorials](https://diataxis.fr/tutorials/), [how-to](https://diataxis.fr/how-to-guides/), [reference](https://diataxis.fr/reference/), [explanation](https://diataxis.fr/explanation/))
- Kubernetes content templates: <https://kubernetes.io/docs/contribute/style/page-content-types/>
- Google developer style guide: <https://developers.google.com/style/>
- Write the Docs principles: <https://www.writethedocs.org/guide/writing/docs-principles/>
- GitHub Markdown & alerts: <https://docs.github.com/en/get-started/writing-on-github/getting-started-with-writing-and-formatting-on-github/basic-writing-and-formatting-syntax>

---

Back to the [documentation hub](./README.md).
