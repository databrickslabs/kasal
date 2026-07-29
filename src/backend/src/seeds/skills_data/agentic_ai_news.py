"""Tracking what actually changed in agentic AI.

Bundles two reference files: where to look, and how to judge a claim. Both are
loaded on demand, so a run that only needs the workflow never pays for them.
"""

_SOURCES = """# Where to look, in priority order

The ranking is by SIGNAL, not by traffic. A tier-1 source states what a system
does and how it was measured; a tier-3 source states that something is
revolutionary.

## Tier 1 — primary, load-bearing

- **Vendor model cards, system cards and release notes** (Anthropic, OpenAI,
  Google DeepMind, Meta, Mistral, Qwen, DeepSeek). The only place capability
  claims come with an evaluation method attached.
- **Vendor changelogs and API docs.** A quietly shipped parameter is often the
  real story — a new tool-calling mode changes more than a launch post does.
- **arXiv (cs.AI, cs.CL, cs.MA)** for method papers, and the benchmark's own
  repository for how a score was produced.
- **Standards and protocol specs** — MCP, A2A, Agent Skills. A protocol change
  affects every product built on it and is usually reported late by everyone
  else.

## Tier 2 — useful, needs corroboration

- Benchmark leaderboards and aggregators (LLM-Stats, price-per-token trackers,
  agentic-AI news roundups). Good for "what shipped this week", weak on what a
  number means.
- Engineering blogs from teams running agents in production. Failure reports
  from these are worth more than any launch announcement.
- Established technical press, where the piece links to a primary source.

## Tier 3 — signal that something happened, not what

- Social posts, including from labs. Treat as a pointer to a primary source, not
  as the source.
- Vendor marketing pages and analyst "top trends" lists.
- Anything whose central claim is a superlative.

## Practical notes

- Prefer the **changelog over the blog post**: the blog says what they want you
  to notice, the changelog says what changed.
- Date everything. In this field a six-month-old capability claim may describe a
  model that has since been deprecated.
- When two sources disagree on a benchmark number, the difference is almost
  always the harness — scaffolding, tool access, number of attempts — not the
  model.

## Carrying the link through

Search tools return a numbered source list alongside their answer. That list is
the deliverable, not scaffolding — copy the URLs into your output exactly as
received. Summarising a source without its link converts a checkable claim into
an unverifiable one, which for this subject matter is most of the value gone.

If a tool answered with no URL for a claim, report the claim AND the gap:
"reported by <tool>, no primary source returned". A reader can act on that. A
claim that quietly appears sourceless reads as though it were verified.
"""

_JUDGING = """# Telling a capability change from an announcement

Most agentic-AI news is one of four things. Classify before writing.

| Kind | What it is | How to treat it |
|---|---|---|
| **Capability** | A system can now do something it measurably could not | The story. Needs the evaluation. |
| **Availability** | Existing capability, new region/price/tier/API | Real, but say what it is. |
| **Packaging** | A product wrapper around existing capability | Usually not news. |
| **Intention** | A roadmap, a preview, a partnership | Not a capability. Label as announced. |

## Questions to ask of a capability claim

1. **Measured how?** Name the benchmark and the harness. "Better at agentic
   tasks" without an eval is a marketing sentence.
2. **Compared with what?** A jump against a year-old baseline is not the same as
   a jump against the current best.
3. **Under what scaffolding?** Agentic scores move enormously with tool access,
   retry budget and prompt. The same model scores very differently in two
   harnesses, and vendors quote the favourable one.
4. **Reproducible by whom?** Open weights and a public harness, or a number only
   the vendor can produce?
5. **What is the failure mode?** A capability reported with no error analysis
   has usually not been examined hard.

## Red flags

- A benchmark introduced in the same post that reports the result.
- "Up to" figures with no distribution.
- Percentages with no denominator ("solves 60% of tasks" — which tasks?).
- A demo video standing in for an eval.
- Comparisons against an unnamed "leading model".

## What is worth reporting even when small

- A protocol or spec change (MCP, A2A, Agent Skills) — it moves the whole
  ecosystem.
- A deprecation or a pricing change on a model people build on.
- A published failure analysis from a team running agents in production. Rarer
  and more useful than any launch.
"""

SKILL = {
    "name": "tracking-agentic-ai-news",
    "description": (
        "Research and summarise recent developments in agentic AI — model and "
        "agent releases, tool-use and reasoning benchmarks, agent protocols "
        "such as MCP and A2A, and framework changes — separating measured "
        "capability from announcement. Use when asked what is new in AI or "
        "agentic AI, for a weekly or monthly AI news digest, a competitive or "
        "landscape scan, whether a specific model or agent capability is real, "
        "or to monitor a vendor's releases. Always answers with source links. "
        "Trigger when the user mentions AI news, latest models, agent "
        "frameworks, LLM benchmarks, what changed this week in AI, keeping up "
        "with agentic AI, or asks for the links, sources or papers behind an "
        "answer."
    ),
    "body": """# Tracking agentic AI

The field produces far more announcement than capability. The job is to find the
few things that actually changed and say why they matter — not to relay press
releases faster.

## When to use this skill

Any request for AI news, a landscape or competitor scan, a recurring digest, or
a check on whether a specific capability claim is real.

## 1. Fix the window and the question before searching

"What is new in agentic AI" is unanswerable. Pin down:

- **Period.** Last week, since a named release, this quarter.
- **Angle.** Model capability? Agent frameworks? Protocols? Cost? Safety?
- **Audience.** An engineering team wants harnesses and failure modes; a
  leadership audience wants what it changes about a decision.

If the request does not say, pick the most likely reading, state it in one line
at the top of the output, and continue. Do not open with a clarifying question
you can answer yourself.

## 2. Search broadly, then go to the primary source

Run several differently-phrased searches — a single query returns one slice of
the week. Then, for anything worth reporting, **open the primary source**: the
model card, the changelog, the paper, the spec. A summary of a summary is where
the errors compound.

See `references/sources.md` for where to look and in what order.

## 3. Classify before you write

Separate capability from availability, packaging and intention. Most items are
not capability changes, and saying so is most of the value.

`references/judging-claims.md` has the classification table and the questions to
ask of a benchmark number.

## 4. Cite everything — this is not optional

An unsourced claim about this field is worthless. Nobody can check it, and half
of what circulates is wrong. Every item you report carries its link.

**Reproduce URLs exactly as your tools returned them.** Search tools return a
numbered source list; copy those URLs character for character into your answer.
Do not shorten them, do not describe them ("the arXiv paper"), do not replace
them with a search query, and never write a URL you did not receive — a
plausible-looking invented link is worse than no link, because it will be
believed and then fail.

Format each item like this:

```
**Claude Opus 5 tool-use benchmark** — 74.3% on SWE-bench Verified, up from
61.2%, measured with the vendor's own harness at 3 attempts.
Source: https://example.com/model-card
```

And close with a plain list of every source used:

```
## Sources
[1] Anthropic — Claude Opus 5 model card — https://example.com/model-card
[2] arXiv 2504.01234 — Multi-agent coordination under partial observability —
    https://arxiv.org/abs/2504.01234
```

Rules that matter:

- **One link per claim, the PRIMARY one.** Link the model card, not the article
  about the model card.
- **If a tool gave you an answer with no URL, say so** — "reported by <tool>, no
  primary source returned" — rather than dropping the item or dressing it up.
- **Keep the URL visible.** Bare links, not "click here": the reader judges a
  source partly by its domain.
- **A benchmark number without a link to its methodology is not reportable.**
  Either find the source or state that the claim is unverifiable.

## 5. Write it

- **Lead with what changed**, not with who announced it.
- One item per paragraph, each with: what it is, what it measurably does, and
  the primary link.
- Say plainly when something is announced-but-unavailable, or a benchmark that
  cannot be independently reproduced.
- **Explicitly note what did NOT change** if a much-covered story turns out to
  be packaging. That is a finding.
- Date every claim. A capability statement without a date rots quietly.

## 6. Say what you could not check

Paywalled papers, vendor-only benchmarks, claims with no eval. A digest that
lists its own gaps is one a reader can act on.

## Cadence, if this is a recurring report

Keep the section order stable — Capability / Availability / Protocols &
standards / Worth watching — so a reader can skim to the part they care about.
Consistency across editions is worth more than covering everything in one.
""",
    "files": [
        {"path": "references/sources.md", "content": _SOURCES},
        {"path": "references/judging-claims.md", "content": _JUDGING},
    ],
}
