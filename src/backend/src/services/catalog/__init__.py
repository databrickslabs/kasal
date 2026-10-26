"""
The workflow catalogue: stored agents, tasks, crews and their support rows.

Plain CRUD over what the user has saved — every path reads from here, so it
belongs to none of them. Prompt templates and output schemas live here too
because they are stored definitions, not behaviour.

These follow the BaseService/repository conventions in services/CLAUDE.md:
group-scoped on every read and write, sensitive tool configs encrypted at rest.
"""
