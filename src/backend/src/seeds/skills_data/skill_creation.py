"""Creating a skill from a conversation."""

SKILL = {
    "name": "creating-skills",
    "description": (
        "Always load this skill when asked to create, write, draft or save an "
        "Agent Skill, or to turn what worked in this conversation into a "
        "reusable skill: it defines the short interview, the SKILL.md shape "
        "Kasal expects, and the fenced skill block that renders as a save card "
        "in the chat. Trigger when the user mentions creating a skill, saving "
        "this as a skill, teaching the agent how to do something, a reusable "
        "playbook, or capturing an approach for next time."
    ),
    "body": """# Creating a skill

A skill is an onboarding note for a future agent: when to act, how, and what
good output looks like. The best ones are written from a conversation that
already went well — the corrections the user made ARE the skill.

## When to use this skill

Any request to create, draft or save a skill, or to capture how something was
done here so it can be reused.

## 0. Creating a skill is writing, not doing

The skill DESCRIBES how a task is done; this turn does not perform it. Do not
run the searches, queries or lookups the skill will tell a future agent to
run. If you genuinely need to learn a domain's source names to write good
instructions, one or two quick searches — then write. A turn that spends its
tool budget doing the task ends with no skill.

## 1. Pick the mode

- **Capture** — the user says "save this as a skill" after an exchange. Mine
  the conversation: what did the user correct, reject or ask for twice? Those
  rules go first. What did they accept without comment? That is the output
  shape. Do not ask questions you can answer from the transcript.
- **Blank page** — the user describes a skill they want. Ask at most three
  questions, in one message: when should it trigger, what does good output
  look like (shape, not adjectives), and which hard rules must never break.
  If the request already answers them, ask nothing.

## 2. The description is the trigger — spend most of the care here

It is the ONLY text an agent sees before deciding to load the skill. Name the
situation with the words a user would actually type: "Use when …" and
"Trigger when the user mentions …". Two to four sentences. A description that
says what the skill is *about* rather than *when to reach for it* never
activates.

## 3. The body is a workflow, not an essay

- Open with `# Title` and a `## When to use this skill` line.
- Then numbered steps, each a rule with the reason it exists, in the order
  the work happens.
- Describe the output as an artefact — shape, sections, format — never as
  quality ("comprehensive", "clear").
- Keep it under sixty lines. Detail that only applies sometimes belongs in a
  reference file, mentioned by name, not inlined.

## 4. Name it

Kebab-case, verb-first, two to four words: `writing-release-notes`,
`reviewing-sql-migrations`. The name is the id; pick it once.

## 5. Emit the draft as a skill block — this is how it gets saved

Put the complete SKILL.md inside ONE fenced block whose language tag is
`skill`. Kasal renders that block as a card with a **Save to teamspace**
button; nothing else saves it, and you must not claim it was saved.

```skill
---
name: writing-release-notes
description: Use when … Trigger when the user mentions …
---

# Writing release notes

## When to use this skill
…

## 1. …
```

One sentence before the block saying what the skill captures; after it, only
what you deliberately left out (if anything). No second block, no prose copy
of the body.

**The block goes in THIS message.** Never end a turn with "let me create it"
or "I'll draft that now" — there is no next turn; the block is the answer.

## 6. After the save

Suggest testing it immediately: pick the skill in the chat "+" menu for the
next turn and run the task it was written for. A skill that has not been
exercised once is a guess.
""",
}
