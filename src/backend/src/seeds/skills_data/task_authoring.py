"""Writing tasks an agent can actually execute."""

SKILL = {
    "name": "writing-agent-tasks",
    "description": (
        "Write task descriptions and expected_output fields that an agent can "
        "actually execute, and diagnose tasks that produced vague or unusable "
        "results. Use when creating a crew, adding or editing a task, "
        "generating a workflow from a prompt, or reviewing why a task returned "
        "a plausible-sounding answer instead of a real one. Trigger when the "
        "user mentions building a crew, writing tasks, expected output, task "
        "descriptions, or an agent that ignored instructions."
    ),
    "body": """# Writing a task an agent can execute

Most disappointing crew output is a task problem, not a model problem. A vague
task gets a vague answer, and the agent had no way to know better.

## When to use this skill

Whenever a task is being written or edited, and whenever a run produced output
that reads well but is not usable.

## 1. The description says what to DO

Start with a verb and name the object. "Analyse Q3 revenue by region using
sales.orders" — not "revenue analysis".

Put these in the description itself:

- **The inputs.** Name the table, file, URL, or the upstream task whose output
  this one consumes. An agent that has to guess where the data is will guess.
- **The boundaries.** Date ranges, segments, systems in scope. Absent
  boundaries get invented ones.
- **What not to do**, only where it is genuinely likely: "do not query
  production", "do not speculate about causes you cannot evidence".

## 2. The expected_output describes the ARTEFACT

This is the field people skip and the one that decides whether the output is
usable. Describe the shape, not the quality:

- Good: "A markdown table with columns region, revenue, QoQ change, followed by
  two sentences naming the largest mover and the evidence for it."
- Useless: "A comprehensive analysis." Every model already believes its output
  is comprehensive.

If a specific format matters — JSON, a table, a filename — say so here. It is
the only place the agent reliably looks.

## 3. Split a task that has two verbs

"Fetch the data and build the dashboard" is two tasks. As one, a failure in
either half loses both and the intermediate result is invisible. Pass the first
task's output to the second through `context`.

## 4. Check the agent can do it

A task assigned to an agent with no relevant tool produces a plausible narrative
instead of a result. Before running, ask: does this agent have what this task
needs?

## 5. Read the task back before running it

If a competent colleague could not do it without asking you a question, the
agent cannot either — it just will not ask.

## Diagnosing a bad result

| Symptom | Usual cause |
|---|---|
| Generic, essay-like output | expected_output described quality, not shape |
| Invented numbers | No input named, or the agent had no tool to fetch them |
| Right shape, wrong scope | No date range or segment boundary in the description |
| Stopped halfway | Two verbs in one task |
| Ignored a rule you stated | The rule was in the crew prompt, not in this task |
""",
}
