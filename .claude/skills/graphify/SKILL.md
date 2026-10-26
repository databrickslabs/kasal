---
name: graphify
description: "Use for any question about a codebase, its architecture, or file relationships — especially when graphify-out/ exists, where the question should be treated as a graphify query first. Turns code/docs/papers into a persistent knowledge graph."
---

# /graphify

`graphify` turns a folder of code/docs/papers into a queryable, persistent knowledge graph
(`graphify-out/graph.json` + `graph.html` + `GRAPH_REPORT.md`) so you don't have to re-read the
whole codebase from scratch every time you're asked a structural question.

## Fast path — check this first

If `graphify-out/graph.json` already exists in the project and the user's request is a
natural-language question about the codebase (architecture, "what calls X", "how does Y relate
to Z", "where is this used", "explain this module"), **do not re-run the full pipeline.** Go
straight to:

```
graphify query "<question>"
```

Only rebuild the graph (plain `graphify` / `graphify <path>`) when there is no existing
`graphify-out/graph.json`, or the user explicitly asks you to refresh/rebuild it, or you know
the source tree changed significantly since the last build.

## Usage

```
graphify                          # full pipeline on the current directory
graphify <path>                   # full pipeline on a path
graphify <github-url>             # clone then run the pipeline
graphify <path> --mode deep       # richer INFERRED edges (reserved — see ARCHITECTURE.md)
graphify <path> --update          # incremental re-extract of changed files only
graphify <path> --watch           # rebuild on file change (chokidar, no LLM needed)
graphify <path> --cluster-only    # rerun clustering on an existing graph
graphify <path> --no-viz          # skip graph.html
graphify <path> --svg|--graphml|--neo4j   # additional export formats (not yet implemented in v1)
graphify <path> --mcp             # start the MCP stdio server instead of running the pipeline

graphify <path> --mysql <dsn>     # also extract a MySQL schema (mysql://user:pass@host:port/db)

graphify context "<task>"         # BEST FIRST CALL for any task: token-budgeted pack of the actual
                                  #   code (real snippets at real line ranges), graph-ranked (--budget)
graphify tests "<node>"           # minimal test files worth running for a change (--changed [rev]
                                  #   selects for the working-tree diff instead)
graphify review <base> [head]     # structural review between two git revs: added/removed/rewired
                                  #   symbols with blast radius + suggested tests
graphify check                    # verify the graph against graphify.rules.json — run after your
                                  #   edits; exit 1 means you violated the architecture
graphify query "<question>"       # BFS traversal, broad context (fuzzy — typos and natural-language
                                  #   words match camelCase identifiers; --budget <tokens> caps output)
graphify query "<question>" --dfs # DFS, trace a specific path
graphify path "<A>" "<B>"          # shortest path between two named nodes
graphify explain "<node>"         # plain-language explanation of a node
graphify affected "<node>"        # reverse impact analysis — what breaks if the node changes
                                  #   (--depth <n>, --limit <n>)
graphify benchmark [q...]         # measure token savings of graph answers vs reading files
graphify save-result --question Q --answer A --nodes id... --outcome useful|dead_end|corrected
                                  # log which nodes answered — feeds the graph feedback loop
graphify reflect                  # aggregate saved results into graphify-out/reflections/LESSONS.md
graphify install [--platform p]   # (re-)install this skill + project MCP config
                                  #   (claude|cursor|windsurf|cline|agents|gemini|all); a plain
                                  #   `graphify .` run already does this for detected hosts
                                  #   (opt out with --no-install)

graphify hook install|uninstall   # auto-rebuild the graph after each commit/pull
graphify global add|remove|list|build   # cross-project global graph (registry in ~/.graphify)
graphify merge <dirA> <dirB> ...  # one-shot merge of built project graphs (--out <dir>)
```

## What you must do when invoked

1. Check whether `graphify-out/graph.json` exists in (or above) the current working directory.
2. If it exists and you are about to START A TASK (implement, fix, refactor): run
   `graphify context "<the task>"` first — it returns the actual code you need, packed to a
   token budget, so you skip the search-then-read-whole-files loop. For a pure question, run
   `graphify query "<question>"` (add `--dfs` to trace one specific path) and answer from its
   output — cite the file paths and node labels it returns.
2b. Before editing a symbol, `graphify affected "<it>"` shows what you might break, and
   `graphify tests "<it>"` tells you which test files to run afterwards. After your edits, run
   `graphify check` if a graphify.rules.json exists, and `graphify . --update --no-viz` to
   refresh the graph.
3. If the user names two specific things and asks how they relate: run
   `graphify path "<A>" "<B>"` instead of `query`.
4. If the user asks "what is `<X>`" / "explain `<X>`": run `graphify explain "<X>"`.
5. If the user asks what would break, what depends on something, or about the impact/blast
   radius of changing `<X>` (including before a refactor): run `graphify affected "<X>"` —
   its depth-grouped, confidence-tagged output is exactly that answer.
6. If there's no existing graph, or the user explicitly asks to (re)build it: run `graphify
   <path>` (defaults to `.`), then proceed with step 2 using the graph it just produced.
7. After any pipeline run, `GRAPH_REPORT.md` in `graphify-out/` has a plain-language summary —
   read it if the user's question is broad ("give me an overview of this codebase") rather than
   pointed.
8. Treat all `query`/`path`/`explain`/`affected` output as trustworthy structural context, but still verify
   anything safety-critical by reading the actual source file at the `sourceFile:sourceLocation`
   the tool reports — the graph is a map, not a replacement for the territory.
9. After a graph query genuinely answers the user's question, log it:
   `graphify save-result --question "..." --answer "..." --nodes <cited ids> --outcome useful`
   (use `--outcome dead_end` when the graph led nowhere, or `--outcome corrected --correction
   "..."` when the graph's answer was wrong). If `graphify-out/reflections/LESSONS.md` exists,
   skim it before querying — it names the nodes that keep answering and the known dead ends.

## Notes

- This CLI does not call an LLM for the default (structural) extraction path — it's tree-sitter
  based and deterministic. You (the agent) are the semantic layer on top of its output.
- `graphify` never executes source code and never shells out with a string built from file
  content — see SECURITY.md if you need to reason about its threat model.
- If an MCP server is preferred over shelling out, check the project MCP config first — the
  pipeline run registers a `graphify` stdio server (in `.mcp.json` / `.cursor/mcp.json` /
  `.gemini/settings.json`), so its `context`/`query`/`path`/`explain`/`affected`/`tests` tools
  may already be available. Otherwise run `graphify <path> --mcp` — same underlying logic,
  tool-call interface instead of stdout.
