"""Answering from documents the user attached."""

SKILL = {
    "name": "grounding-on-documents",
    "description": (
        "Always load this skill when files, documents or knowledge sources "
        "are attached to the task: it defines how to ground the answer in "
        "them — search first, quote and attribute, and never blend document "
        "facts with model memory. Use when summarising, extracting from, or "
        "answering questions about an uploaded or attached document, "
        "contract, report or spec. Trigger when the user attaches a file, "
        "mentions 'the document', 'the attached', 'this PDF', or asks what "
        "a provided source says."
    ),
    "body": """# Grounding an answer in attached documents

An attached document is the ground truth for this task. The failure mode is an
answer that reads well but came from the model's memory instead of the file —
indistinguishable from a grounded one until someone checks.

## When to use this skill

Whenever the task carries attached files or a knowledge source to answer from.

## 1. Search the document before answering

Query the attached source first, with the user's own terms and one or two
rephrasings. Only what came back is document fact.

## 2. Attribute every document fact

Name the file and the place: "the MSA (section 7.2)", "the Q3 report, p. 4".
A claim without a location cannot be checked against the document, which
defeats the point of attaching it.

## 3. Copy numbers and quoted terms exactly

Figures, dates, defined terms, party names: verbatim from the document, never
paraphrased from memory. If a number needs deriving, show the inputs.

## 4. Keep document facts and model knowledge separate

Both are allowed — blended, they are poison. "The contract sets a 30-day cure
period (section 9)" is document fact; "30 days is typical for SaaS agreements"
is background knowledge. Label the second, or leave it out.

## 5. When the document does not answer, say exactly that

"The attached report does not cover churn by segment" is a correct, useful
answer. Filling the gap silently from general knowledge is the single worst
thing this task can do — it will be read as if the document said it.

## 6. When documents conflict

Two attached sources disagreeing is a finding. Report both values with their
locations; do not pick one silently.

## Scope discipline

Answer from the attached material for what the request is about. If the request
also needs outside information, get it — and keep rule 4: what came from where
stays visible in the answer.
""",
}
