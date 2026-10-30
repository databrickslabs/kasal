"""Exception-handling and logging lint rules, enforced as a per-file count ratchet.

The configured ruff gate (``[tool.ruff.lint]`` in pyproject.toml) is ``E``/``F`` plus
``RUF100``. These five rules found thousands of hits when they were introduced, so
they are enforced HERE instead, against ``ruff_ratchet_baseline.json``:

* ``BLE001`` — ``except Exception`` that neither re-raises nor logs the traceback.
  The single biggest source of "it failed and nothing said why".
* ``S110``   — ``try``/``except``/``pass``: an error swallowed without a trace.
* ``TRY400`` — ``logger.error`` inside ``except``; use ``logger.exception`` so the
  traceback is kept.
* ``G004``   — f-string log messages: formatted even when the level is off, and
  they defeat log aggregation by message template. Use ``logger.info("x %s", y)``.
* ``B904``   — ``raise X`` inside ``except`` without ``from``: the cause is lost.

A file may not gain a hit of any of these, and a new file starts at zero. When
you fix hits the counts drop and this test asks you to lower the baseline::

    uv run python tests/unit/architecture/test_ruff_ratchet.py --update

A deliberate exception still uses ``# noqa: BLE001 — <reason>`` on the line, which
ruff honours here; ``RUF100`` knows these codes as ``lint.external`` so it leaves
those comments alone.

Only ``src/`` is judged. Tests catch broad exceptions and format log lines on
purpose, and none of it ships.
"""

from __future__ import annotations

import json
import pathlib
import subprocess
import sys
from collections import Counter

try:
    from . import _ratchet
except ImportError:  # run as a script for --update
    import _ratchet  # type: ignore[no-redef]

BACKEND = pathlib.Path(__file__).resolve().parents[3]
BASELINE = pathlib.Path(__file__).with_name("ruff_ratchet_baseline.json")
RULES = ("BLE001", "S110", "TRY400", "G004", "B904")


def current_counts() -> dict[str, int]:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "ruff",
            "check",
            "src",
            "--select",
            ",".join(RULES),
            "--output-format",
            "json",
            "--exit-zero",
            "--no-cache",
        ],
        cwd=BACKEND,
        capture_output=True,
        text=True,
        check=True,
    )
    counts: Counter[str] = Counter()
    for hit in json.loads(result.stdout):
        path = pathlib.Path(hit["filename"]).resolve().relative_to(BACKEND)
        counts[f"{path.as_posix()}::{hit['code']}"] += 1
    return dict(counts)


def test_no_file_gains_a_ruff_ratchet_hit():
    baseline = _ratchet.load(BASELINE)
    grown = _ratchet.grown(baseline, current_counts())
    assert not grown, (
        "New BLE001/S110/TRY400/G004/B904 hits (file::rule: allowed -> now):\n  "
        + "\n  ".join(grown)
        + "\n\nSee the docstring of this test for what each rule wants. Log with "
        "%-style args, keep tracebacks (logger.exception / exc_info=True), raise "
        "... from exc, and catch the narrowest exception you can. A reviewed "
        "exception takes `# noqa: <CODE> — <reason>` on the line."
    )


def test_the_baseline_only_shrinks():
    baseline = _ratchet.load(BASELINE)
    stale = _ratchet.stale(baseline, current_counts())
    assert not stale, (
        "Fewer hits than recorded — lock the gain in by lowering the baseline:\n  "
        + "\n  ".join(stale)
        + "\n\n  uv run python tests/unit/architecture/test_ruff_ratchet.py --update"
    )


if __name__ == "__main__":
    if "--update" not in sys.argv:
        sys.exit("usage: test_ruff_ratchet.py --update")
    _ratchet.update(BASELINE, current_counts())
