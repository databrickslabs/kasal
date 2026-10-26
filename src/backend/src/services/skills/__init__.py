"""Agent Skills — packaged procedural know-how.

A capability package, per ``services/CLAUDE.md``: usable from a chat turn, a
crew run or a flow without importing an orchestrator. It must never import a
path package, or a skill stops being available exactly where it is cheapest to
use.

The layout follows the three tiers of progressive disclosure, because that is
the whole mechanism and the files should say so:

- ``parser`` — conformance, delegated to Anthropic's reference validator.
- ``injection`` — TIER 1: name + description of every enabled skill, always in
  the prompt, ~100 tokens each.
- ``loader`` — TIERS 2 and 3: the body when the model asks for it, a bundled
  file when the instructions call for it. The security boundary is here.
- ``packaging`` — zip in, folder out, so a skill authored here runs elsewhere.
- ``service`` — CRUD, ownership and group scoping.
"""
