"""
Generating workflow definitions with an LLM.

Ask for a crew and get agents, tasks and the edges between them: ``crews`` plans
the whole thing, ``agents``/``tasks`` generate one at a time, ``connections``
wires agents to tasks, ``templates`` writes prompt templates and
``prompt_improvement`` rewrites one. ``crew/`` holds the crew planner's pieces.

All of these read DB-backed prompt templates via
``TemplateService.get_effective_template_content`` — and the seeder OVERWRITES
those rows from src/seeds/prompt_templates.py on every startup. Edit the seed
file, not the row, and remember a running backend only picks it up after a
restart.

See services/CLAUDE.md for the two mistakes that shipped a
\"generation always produces 1 agent + 1 task\" regression.
"""
