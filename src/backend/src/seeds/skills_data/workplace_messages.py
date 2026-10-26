"""Writing workplace messages that get acted on."""

SKILL = {
    "name": "writing-workplace-messages",
    "description": (
        "Write emails, chat posts and announcements that get acted on: the "
        "ask in the first two lines, one message one ask, a deadline and a "
        "default. Use when drafting an email, a Slack or Teams message, a "
        "request to another team, an escalation or an announcement. Trigger "
        "when the user mentions writing an email, a message, reaching out, "
        "following up, announcing something, or asking a team for something."
    ),
    "body": """# Writing a message that gets acted on

Workplace messages fail by burying the ask. The reader skims, finds no action
in the first lines, files it, and the sender waits on a reply that was never
going to come.

## When to use this skill

Any email, chat message, announcement or cross-team request.

## 1. The ask is the first line

"Can you approve the vendor contract by Thursday?" — then context. If the
reader stops after two lines, they must still know what is wanted, from whom,
by when. Subject lines carry the ask too: "Approval needed by Thu: Acme
contract" beats "Contract question".

## 2. One message, one ask

Two asks in one message get one answer — the easier one. Split them, or make
the second explicitly secondary.

## 3. Deadline plus default

"By Thursday EOD; if I hear nothing I will proceed with option A" turns
silence into a decision instead of a stall. Never set a fake deadline — the
first one that slips discredits all the following ones.

## 4. Make responding cheap

Ask a question that can be answered with yes/no or a choice among named
options. "Thoughts?" transfers all the work to the reader; a numbered list of
two options with your recommendation costs them ten seconds.

## 5. Match length to the relationship, tone to the facts

A colleague gets three lines; a first contact gets one short paragraph of who
and why. In escalations, state facts and impact without heat: "third slip,
blocks the March release" lands harder than any adjective — and survives
being forwarded.

## 6. Announcements: what changes for the reader

Lead with what is different for THEM and from when: "From Monday, deploys
need a ticket link — merges without one will be blocked." History and
rationale come after, in one or two lines.

## Before sending

Read it as the receiver: is it clear what they must do, by when, and what
happens if they do nothing? If any answer is fuzzy, the reply will be too.
""",
}
