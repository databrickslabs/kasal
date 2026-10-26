"""Comparing options into a decision brief."""

SKILL = {
    "name": "comparing-options",
    "description": (
        "Turn an evaluation of tools, vendors, approaches or designs into a "
        "decision brief: explicit criteria before scoring, a comparison "
        "table with evidence, and a real recommendation with the condition "
        "that would reverse it. Use when weighing alternatives, choosing "
        "between products or architectures, or reviewing build-vs-buy. "
        "Trigger when the user mentions comparing, evaluating, choosing, "
        "pros and cons, alternatives, 'which should we use', or a shortlist."
    ),
    "body": """# Comparing options into a decision

The deliverable is a decision brief, not a survey. "It depends" without saying
on what is a failure of the task.

## When to use this skill

Any request to compare, evaluate or choose between alternatives.

## 1. Criteria first, and weighted

Before examining any option, write down what the decision actually turns on —
usually three to six criteria — and which one or two dominate. Criteria that
appear after the options are chosen to flatter a favourite, and readers know.

## 2. The comparison is a table of evidence, not adjectives

Options as columns, criteria as rows, cells carrying facts: a number, a
supported/unsupported, a limit, a price — with the source. "Strong ecosystem"
is an impression; "42k packages, 9 of our 12 current dependencies available"
is evidence.

## 3. Include the costs that hide

Switching cost, lock-in, operational burden, maturity risk, the price at 10x
today's usage. Options usually differ more here than on the feature grid.

## 4. Eliminate visibly

Options dropped early get one line each and the reason. A shortlist with no
record of what was excluded looks like nothing else was considered.

## 5. Recommend, and say what would flip it

Name the choice for the stated criteria and weights. Then the reversal
condition: "If compliance requires data residency, this flips to B." That
sentence is what makes a recommendation trustworthy rather than pushy — and it
tells the reader exactly what to verify.

## 6. State the decision's reversibility

A cheap-to-reverse choice deserves a fast pick and a trial; an expensive one
justifies deeper verification. Say which kind this is.

## When the options are genuinely close

Say so, quantify how close, and recommend on the tiebreaker — cost of delay,
team familiarity, reversibility — rather than manufacturing a gap.
""",
}
