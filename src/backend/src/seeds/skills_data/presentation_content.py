"""Writing an answer that composes into a good slide deck.

Aligned deliberately with ``services/a2ui/compose.py``. The agent does NOT emit
A2UI JSON — it writes prose, and the composer plans an outline and builds the
``SlideDeck`` from it. So this skill teaches the shape of ANSWER that survives
that pass, using the composer's own vocabulary: the slide variants in
``catalog.json``, the content-to-visual classification in
``plan_presentation_outline``, and the four things ``presentation_design_lint``
actually flags.

Keeping the two in step matters. If the composer's rules change, this skill
should change with them — otherwise it teaches a shape the composer no longer
rewards.
"""

_VISUALS = """# What becomes a visual

Kasal's composer classifies each section of your answer and picks a visual for
it. You do not choose the visual — you make the content classifiable. These are
the exact shapes it looks for.

| If the content is… | It becomes | Write it as |
|---|---|---|
| Steps, phases, a workflow | process diagram | An ordered list of named steps |
| Dated milestones, a roadmap | timeline | Each item with a date or period |
| A repeating loop | cycle | Stages that return to the start |
| Narrowing stages, conversion | funnel | Stages with a shrinking quantity |
| Layered levels, a maturity model | pyramid | Levels named bottom-to-top |
| Two options weighed | comparison | Both sides, in text, with the same criteria |
| Two evaluation axes | 2×2 matrix | Name both axes and place items in quadrants |
| An org or tree structure | hierarchy | Parent/child relationships |
| A numeric series or breakdown | chart | Labelled numbers over a dimension |
| Three or four headline figures | stats slide | The figures with short labels |
| Detailed rows and columns | table | Consistent fields per row |

## Making a section classifiable

The classifier reads your prose. Two rules do most of the work:

**Name the shape.** "There are four phases" or "over the last three quarters"
tells it what this is. "We looked at how things developed" does not.

**Keep one shape per section.** A section mixing a timeline, a comparison and
three statistics gets one visual at best, usually the wrong one. Split it.

## Numbers

A figure only becomes a stats slide or a chart if it arrives with its label and
unit: "deployment time fell from 45 minutes to 6" works; "deployment got much
faster" cannot be drawn. Give three or four headline figures somewhere in the
answer — a deck with no numbers is a deck of assertions.
"""

_SHAPES = """# Deck shapes that work

Pick the arc that matches the ask, then write your sections in that order. The
composer opens with a title slide and closes with takeaways on its own; you
supply the middle.

## Executive readout (8-12 slides)

1. The answer, in one sentence — what you found, not what you did
2. Why it matters now — the cost of doing nothing
3-6. The findings, one per section, biggest first
7. What it would take — effort, cost, dependencies
8. The recommendation and the decision you are asking for

Leadership decks fail by burying the ask. It goes at the top and again at the
end.

## Research findings (10-16 slides)

1. The question
2. How it was investigated — method, sources, period
3-10. Findings, grouped by theme rather than by source
11. What contradicts them — the honest section
12. What is still unknown
13. What to do about it

## Technical deep-dive (10-16 slides)

1. The problem, stated concretely
2. Why the obvious approach does not work
3-5. The design, one component per section
6. How it behaves under load / failure
7. Trade-offs accepted, and what was given up
8. Migration or rollout path

## Comparison or vendor evaluation (8-12 slides)

1. What is being chosen and the constraints
2. The criteria, named up front
3-6. One section per option, SAME criteria in the SAME order
7. The trade-off that actually decides it
8. The recommendation

Keeping the criteria order identical across options is what lets the composer
produce comparison slides instead of unrelated bullet lists.

## Status or progress update (6-10 slides)

1. Where things stand in one sentence
2. What shipped since last time
3. The numbers
4. What is blocked, and the ask
5. What is next, with dates

## How long

Ten to sixteen sections gives a deck with room to breathe. Under six and the
composer stops trying to make it visual — it treats short decks as fine
text-heavy. Over twenty and every slide gets thin.
"""

SKILL = {
    "name": "writing-presentation-content",
    "description": (
        "Structure an answer so it becomes a strong slide deck: how many "
        "sections, how much per slide, which content shapes turn into "
        "diagrams, charts, comparisons and stats, and the deck arcs that suit "
        "an executive readout, research findings, a technical deep-dive, a "
        "vendor comparison or a status update. Use when asked to create or "
        "improve a presentation, deck, slides or a readout, when a task's "
        "output will be presented, or when a generated deck came out as a wall "
        "of bullets. Trigger when the user mentions a presentation, slide "
        "deck, slides, a readout, a board or leadership update, or presenting "
        "findings to someone."
    ),
    "body": """# Writing content that becomes a good deck

## When to use this skill

Any task whose output is a presentation, and any time a deck came back flat,
thin or bullet-heavy.

## First, the thing that changes everything

**You write prose. Kasal builds the deck.** Do not emit slide markers, JSON, or
"Slide 1:" headings — a composer reads your answer, plans an outline, and lays
out real slides with layouts, diagrams and charts. Your job is to give it
content it can lay out.

That means the failure mode is not ugly slides. It is an answer with nothing to
lay out: one long essay, or sections so thin each becomes a slide with a single
bullet on it.

## 1. Write in sections, not paragraphs

Each section becomes roughly one slide. Give it:

- **A short, specific heading.** "Latency fell 60% after the cache change" — not
  "Performance". The heading becomes the slide title, and a vague title is a
  wasted slide.
- **Three to five substantive points.** Full sentences, each carrying a real
  fact or insight. One point is a slide that should have been merged; ten is two
  slides fighting.
- **One idea.** A section covering three things gets one slide covering none of
  them well.

A section with a heading and one line is the single most common cause of a thin
deck.

## 2. Give it something to draw

At least one section in three should contain content with a recognisable shape —
steps, dated milestones, two options weighed, a numeric series, headline
figures. That is what turns bullets into diagrams and charts.

`references/visual-types.md` has the exact classification the composer uses and
how to write each shape so it is recognised.

If four sections in a row are plain prose, the deck renders as a bullet wall and
gets flagged for it. Break the run: put the numbers in one, a process in
another.

## 3. Front-load the answer

The first section is the finding, not the background. Decks are skimmed, and the
reader who stops after two slides should still have the point.

## 4. Choose the arc

`references/deck-shapes.md` has the section-by-section structure for an
executive readout, research findings, a technical deep-dive, a comparison and a
status update. Pick one and follow it — a deck whose order the audience can
predict is a deck they can follow.

## 5. Carry the sources

If your material has sources, cite them inline as you go. Kasal checks that a
sourced answer produces a sourced deck and will retry the composition when the
citations get dropped — but it can only preserve what you put there. Reproduce
URLs exactly as your tools returned them; never invent one.

## 6. Write the speaker's line

Where a point needs saying out loud rather than reading, put it in the prose as
a short sentence of its own. The composer lifts material into presenter notes,
which are never shown on the slide.

## What produces a bad deck

| You wrote | The deck gets |
|---|---|
| One continuous essay | Three overloaded slides, or an arbitrary split |
| Ten one-line sections | Ten near-empty slides |
| Prose with no numbers or shapes | Bullet walls, no diagrams |
| A heading like "Overview" or "Details" | A slide nobody can summarise |
| Raw table dumps with no narrative | A table slide and no argument |
| Sources omitted "for brevity" | A correction retry, then an unsourced deck |

## A worked shape

```
## Migration cut deployment time from 45 to 6 minutes

- The old pipeline rebuilt every image on each commit, averaging 45 minutes.
- Layer caching plus a shared base image removed 80% of rebuild work.
- Median deploy is now 6 minutes; p95 is 11.
- The remaining cost is the integration suite, which we have not parallelised.

## Rollout ran in four phases

1. Shadow builds alongside the old pipeline (2 weeks)
2. Opt-in for three teams (3 weeks)
3. Default for new services
4. Cutover, with the old pipeline kept warm for a week
```

The first becomes a content slide with real bullets and a headline number; the
second is recognisably a process and becomes a diagram.
""",
    "files": [
        {"path": "references/visual-types.md", "content": _VISUALS},
        {"path": "references/deck-shapes.md", "content": _SHAPES},
    ],
}
