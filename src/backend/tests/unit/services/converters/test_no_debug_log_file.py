"""No converter writes a debug log of its own to a fixed file.

The SQL converter used to append its intermediate state to a file under the
system temp directory on every conversion. Nothing read it; diagnostics go
through the module loggers.
"""

import pathlib

SRC = pathlib.Path(__file__).resolve().parents[4] / "src"


def test_no_code_writes_the_sql_debug_log():
    writers = sorted(
        str(path.relative_to(SRC))
        for path in SRC.rglob("*.py")
        if "sql_debug.log" in path.read_text(encoding="utf-8")
    )
    assert writers == []
