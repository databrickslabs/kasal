"""Writing a status update someone will actually read."""

SKILL = {
    "name": "writing-status-updates",
    "description": (
        "Write a team or project status update in the Progress / Plans / "
        "Problems shape: what shipped, what is next, what is blocked, at the "
        "granularity the audience needs. Use when asked for a weekly update, a "
        "status report, a leadership or stakeholder update, a project summary, "
        "or a stand-up write-up. Trigger when the user mentions a status "
        "update, weekly update, 3P, progress report, or summarising what a "
        "team has been doing."
    ),
    "body": """# Writing a status update

## When to use this skill

Any request for a weekly update, status report, stakeholder summary or project
round-up.

## The three sections

**Progress** — what actually shipped or completed in the period. Milestones and
outcomes, not activity. "Migrated 40 reports" beats "worked on the migration".

**Plans** — what is next, in priority order. Two or three items. A list of ten
is a backlog, not a plan, and the reader stops at three anyway.

**Problems** — what is slowing the team down, and what would unblock it. A
problem stated without the ask is a complaint; "blocked on warehouse access,
need an admin to grant it" is a request someone can act on.

## Match granularity to the audience

The wider the audience, the coarser the item. A team update can say "fixed the
retry bug"; a company update says "cut pipeline failures by half". If an item
would need explaining to the reader, it belongs one level down.

## Length

Under a minute to read. If it is longer, the reader skims, and skimming loses
the Problems section — which is the part that needed action.

## Before writing, gather

- What closed or shipped in the period, from the tracker or the run history.
- What is in flight and genuinely next.
- What is blocked, and on whom.

If the team or the period is not stated, ask — one question, not a list. Guessing
either produces an update that is wrong in a way the reader will notice
immediately.

## What to leave out

- Work that started and is still in progress with nothing to show. It belongs in
  Plans, not Progress.
- Anything you cannot evidence. An update is read by people who were not there;
  a claim they cannot check erodes the ones they can.
""",
}
