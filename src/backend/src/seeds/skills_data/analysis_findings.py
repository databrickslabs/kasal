"""Structuring an analytical answer."""

SKILL = {
    "name": "writing-analysis-findings",
    "description": (
        "Structure an analytical answer so the reader gets the point "
        "immediately: the finding first, then the evidence, then the caveats, "
        "with measurements separated from inferences. Use when writing any "
        "analysis, investigation result, summary, executive update or report, "
        "and when asked to make a piece of analysis clearer or shorter. "
        "Trigger when the user mentions writing up findings, summarising "
        "results, an executive summary, a report, or explaining what the data "
        "shows."
    ),
    "body": """# Structuring an analysis

## When to use this skill

Any time findings are being written up for someone else to read.

## Lead with the finding, not the method

The first sentence is the answer: "Revenue fell 12% in EMEA, driven entirely by
one account churning in August." Not "This analysis examines revenue trends."

Nobody reads an analysis to learn that an analysis was performed.

## Then the evidence, ordered by how much it supports the claim

Every number should be traceable: name the source, the filter, the period. A
figure the reader cannot reproduce is one they have to take on trust, and they
will not.

## Then the caveats, honestly

State what would change the conclusion — data you could not access, assumptions
you had to make, periods where coverage is incomplete. A caveat buried mid-
paragraph has not been stated; give it its own line.

## Quantify, or say you cannot

"Significantly" and "substantially" carry no information. Give the number, or
say the data does not support quantifying it.

## Separate what you measured from what you infer

"Revenue fell 12%" is a measurement. "Because the pricing change landed in July"
is a hypothesis. Label the second as one — an inference presented as a finding
is how an analysis becomes wrong later.

## Say it once

Repeating the finding in a summary, the body and a conclusion does not make it
clearer. It makes the reader hunt for the version that has the detail.
""",
}
