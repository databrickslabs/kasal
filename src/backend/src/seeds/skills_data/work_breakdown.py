"""Breaking a goal into a plan of executable work."""

SKILL = {
    "name": "breaking-down-work",
    "description": (
        "Turn a goal or project into milestones and tasks that can actually "
        "be executed: outcome-shaped milestones, verb-first tasks with an "
        "exit criterion each, explicit dependencies, riskiest assumption "
        "first. Use when planning a project, scoping an initiative, or "
        "converting an idea into a backlog or workplan. Trigger when the "
        "user mentions a plan, roadmap, milestones, breaking down work, "
        "scoping, next steps, or how to approach a project."
    ),
    "body": """# Breaking down work

A plan is a claim about how a goal decomposes. Most bad plans fail the same
way: activity-shaped items nobody can call done, and the riskiest part
scheduled last.

## When to use this skill

Any request to plan, scope or break down a goal into work.

## 1. Start from the outcome, not the activities

One sentence for what will be TRUE when this is done, testable by someone
outside the project. Every item below must trace to it; an item that traces to
nothing is scope creep pre-committed.

## 2. Milestones are demonstrable states

"First customer invoice generated in the new system" — not "backend phase".
Someone can witness a milestone. Three to six of them; more is a task list
wearing a coat.

## 3. Tasks: a verb, an object, an exit criterion

"Migrate the orders table — done when row counts match and the reconciliation
query returns zero diffs." A task whose doneness is arguable will be argued.
Keep one owner per task; a task with two owners has none.

## 4. Dependencies are stated, not discovered

For each task: what must exist before it can start. The plan's shape is the
dependency graph — a plan written as a flat list hides the critical path,
and the critical path is where the schedule lives.

## 5. Sequence by risk, not by comfort

The riskiest assumption gets tested first, cheaply. If the vendor API cannot
do bulk export, week one is when to learn it — not week nine. Familiar,
satisfying work first is how projects feel productive right up to the surprise.

## 6. Estimate coarsely, honestly

S / M / L or ranges. False precision ("3.5 days") is a guess in a suit and
will be treated as a promise. Name what would change the estimate.

## The first task is startable today

If nothing on the plan can begin without a meeting, a decision or an access
grant, THAT is the first task — write it down with its owner and ask.
""",
}
