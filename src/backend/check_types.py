#!/usr/bin/env python3
"""Reject new mypy errors while keeping the pre-existing typing debt explicit.

The baseline records diagnostic counts per file and message, without line numbers
so formatting does not invalidate it. It is an allowance, never a suppression in
mypy itself: `python -m mypy src` still reports the complete strict result.
"""

import json
import re
import subprocess
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent
ERROR = re.compile(r"^(src/.*?):\d+(?::\d+)?: error: (.+)$")


def diagnostics(output: str) -> Counter[tuple[str, str]]:
    return Counter(
        (match[1], match[2])
        for line in output.splitlines()
        if (match := ERROR.match(line))
    )


def main() -> int:
    result = subprocess.run(
        [sys.executable, "-m", "mypy", "src", "--no-pretty", "--show-error-codes"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    current = diagnostics(result.stdout)
    # A crash, missing dependency, malformed configuration, or unparsed error
    # must never be mistaken for an empty/successful type check.
    unparsed = any(
        ": error:" in line and not ERROR.match(line)
        for line in (result.stdout + result.stderr).splitlines()
    )
    if (
        result.returncode not in (0, 1)
        or (result.returncode and not current)
        or unparsed
    ):
        print(result.stdout, end="")
        print(result.stderr, file=sys.stderr, end="")
        return result.returncode or 1
    baseline = json.loads((ROOT / "mypy-baseline.json").read_text())
    allowed = Counter(
        {(row["file"], row["message"]): row["count"] for row in baseline["diagnostics"]}
    )
    new = current - allowed
    for (filename, message), count in sorted(new.items()):
        print(f"{filename}: {message} ({count} new)")
    print(
        f"Mypy: {sum(current.values())} existing diagnostics; {sum(new.values())} new; "
        f"{sum((allowed - current).values())} baseline diagnostics resolved."
    )
    if new:
        print("Fix new errors; do not increase the baseline to make this check pass.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
