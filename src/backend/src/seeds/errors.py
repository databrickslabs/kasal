"""Exceptions that make a failed seed visible instead of a log line.

Every seeder used to catch its own failure, log it and return normally, and the
runner did the same one level up. A run against a database with no tables
logged hundreds of ``no such table`` errors and still reported success (and
``run_seeders.py`` exited 0). These two exceptions are the contract that
replaces that:

* a seeder that could not seed everything it defines raises
  :class:`SeederIncomplete` (or lets its own exception propagate);
* the runner keeps going through the remaining seeders, so one failure does not
  hide the next, and then raises :class:`SeedingError` naming each failure.

Callers decide what a failure means: the CLI exits non-zero, the background
seeding at server startup logs it and lets the app keep serving, and a fresh
Lakebase installation refuses to start without its essential defaults.
"""

from typing import Dict, Iterable, List, Mapping

# Seeders whose rows the app cannot work without: no model to pick, no tool to
# assign, no prompt to generate with. installation.py verifies these tables are
# non-empty before a fresh install accepts its first request.
ESSENTIAL_SEEDERS = frozenset({"model_configs", "prompt_templates", "tools"})


class SeederIncomplete(RuntimeError):
    """A seeder ran to the end but some of its items were not written."""

    def __init__(self, seeder: str, failed: int, total: int) -> None:
        self.seeder = seeder
        self.failed = failed
        self.total = total
        super().__init__(
            f"{failed} of {total} item(s) failed to seed "
            "(see the errors logged above)"
        )


class SeedingError(RuntimeError):
    """One or more seeders failed during a run."""

    def __init__(self, failures: Mapping[str, str]) -> None:
        self.failures: Dict[str, str] = dict(failures)
        details = "; ".join(f"{name}: {error}" for name, error in self.failures.items())
        super().__init__(
            f"{len(self.failures)} seeder(s) failed: {', '.join(self.failures)}"
            f" -- {details}"
        )

    def essential_failures(
        self, essential: Iterable[str] = ESSENTIAL_SEEDERS
    ) -> List[str]:
        """The failed seeders the app cannot run without, in failure order."""
        wanted = set(essential)
        return [name for name in self.failures if name in wanted]
