"""Seeding fails loudly: a failed seeder is an exit code, not just a log line.

Audit item #10. run_seeders.py used to log hundreds of ``no such table`` errors
on an empty database (it never created the tables) and still exit 0, because
every seeder and the runner caught and logged their own failures.

Covered here:
- the CLI exits 1 and names the failed seeder; exits 0 on success;
- a real run on a fresh SQLite file exits 0 with zero ERROR log lines;
- a seeder that fails some of its items raises SeederIncomplete;
- startup: background seeding logs a failure and never raises; a fresh Lakebase
  install refuses to start only when an ESSENTIAL seeder failed. (That the
  lifespan itself still starts is in tests/unit/test_main_lifespan.py.)
"""

import importlib.util
import logging
import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.seeds.errors import SeederIncomplete, SeedingError

BACKEND_ROOT = Path(__file__).resolve().parents[3]
RUN_SEEDERS = BACKEND_ROOT / "run_seeders.py"


def _load_cli():
    """Import run_seeders.py without its import-time logging/env side effects."""
    spec = importlib.util.spec_from_file_location("_run_seeders_cli", RUN_SEEDERS)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    with patch("logging.basicConfig"), patch.dict(os.environ, {}, clear=False):
        spec.loader.exec_module(module)
    return module


# ---------------------------------------------------------------------------
# run_seeders.py (the CLI)
# ---------------------------------------------------------------------------


class TestCliExitCode:
    @pytest.mark.asyncio
    async def test_a_failing_seeder_exits_1_and_names_it(self, caplog):
        cli = _load_cli()
        failure = SeedingError({"tools": "no such table: tools"})
        with (
            patch("src.db.session.init_db", new=AsyncMock()) as init_db,
            patch(
                "src.seeds.seed_runner.run_all_seeders",
                new=AsyncMock(side_effect=failure),
            ),
            caplog.at_level(logging.ERROR, logger="SeedTest"),
        ):
            code = await cli.run_test()

        assert code == 1
        init_db.assert_awaited_once()  # the schema is built before seeding
        messages = [r.getMessage() for r in caplog.records if r.name == "SeedTest"]
        assert any(
            "SEEDING FAILED" in m and "tools" in m and "no such table" in m
            for m in messages
        ), messages

    @pytest.mark.asyncio
    async def test_a_schema_failure_exits_1(self):
        cli = _load_cli()
        seed = AsyncMock()
        with (
            patch(
                "src.db.session.init_db",
                new=AsyncMock(side_effect=RuntimeError("disk full")),
            ),
            patch("src.seeds.seed_runner.run_all_seeders", new=seed),
        ):
            assert await cli.run_test() == 1
        seed.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_clean_run_exits_0(self):
        cli = _load_cli()
        with (
            patch("src.db.session.init_db", new=AsyncMock()),
            patch("src.seeds.seed_runner.run_all_seeders", new=AsyncMock()),
        ):
            assert await cli.run_test() == 0

    @pytest.mark.asyncio
    async def test_seed_runner_main_returns_1_on_failure(self):
        from src.seeds import seed_runner

        with (
            patch("sys.argv", ["seed_runner", "--all"]),
            patch.object(
                seed_runner,
                "run_all_seeders",
                new=AsyncMock(side_effect=SeedingError({"skills": "boom"})),
            ),
        ):
            assert await seed_runner.main() == 1


class TestFreshDatabaseEndToEnd:
    """The real script against a new SQLite file, in its own interpreter."""

    def test_fresh_database_seeds_cleanly_with_zero_errors(self, tmp_path):
        db = tmp_path / "fresh.db"
        env = {**os.environ, "DATABASE_TYPE": "sqlite", "SQLITE_DB_PATH": str(db)}
        env.pop("DATABASE_URI", None)
        env.pop("SYNC_DATABASE_URI", None)
        proc = subprocess.run(
            [sys.executable, str(RUN_SEEDERS)],
            cwd=BACKEND_ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=300,
        )
        output = proc.stdout + proc.stderr
        error_lines = [line for line in output.splitlines() if " - ERROR - " in line]

        assert proc.returncode == 0, output[-4000:]
        # Before the fix this was 126 ERROR lines (327 "no such table").
        assert error_lines == []
        assert "no such table" not in output
        assert "All seeders completed successfully!" in output


# ---------------------------------------------------------------------------
# Seeders report partial failure
# ---------------------------------------------------------------------------


class TestSeederIncomplete:
    def test_message_counts_the_failed_items(self):
        err = SeederIncomplete("tools", 3, 34)
        assert (err.seeder, err.failed, err.total) == ("tools", 3, 34)
        assert "3 of 34" in str(err)

    def test_seeding_error_names_every_failure_and_the_essential_ones(self):
        err = SeedingError({"example_crews": "x", "tools": "y"})
        assert "example_crews" in str(err) and "tools" in str(err)
        assert err.essential_failures() == ["tools"]

    @pytest.mark.asyncio
    async def test_a_nonconforming_builtin_skill_fails_after_seeding_the_rest(self):
        from src.seeds import skills

        entries = [
            {"name": "good", "description": "d", "body": "b"},
            {"name": "bad", "description": "d", "body": "b"},
        ]

        def validate(name, *_args):
            if name == "bad":
                raise ValueError("bad frontmatter")

        session = AsyncMock()
        result = MagicMock()
        result.scalars.return_value.all.return_value = []
        session.execute = AsyncMock(return_value=result)
        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=session)
        ctx.__aexit__ = AsyncMock(return_value=None)

        with (
            patch.object(skills, "BUILTIN_SKILLS", entries),
            patch.object(skills, "get_isolated_db_session", return_value=ctx),
            patch.object(skills, "_upsert", new=AsyncMock(return_value="created")),
            patch("src.services.skills.parser.validate_row", side_effect=validate),
        ):
            with pytest.raises(SeederIncomplete, match="1 of 2"):
                await skills.seed()

        session.commit.assert_awaited_once()  # "good" was still written


# ---------------------------------------------------------------------------
# Startup behaviour
# ---------------------------------------------------------------------------


class TestBackgroundSeedingAtStartup:
    @pytest.mark.asyncio
    async def test_a_failure_is_logged_and_never_raised(self, caplog):
        from src.seeds.startup import run_seeders_in_background

        failure = SeedingError({"example_crews": "no such table: agents"})
        with (
            patch(
                "src.seeds.seed_runner.run_all_seeders",
                new=AsyncMock(side_effect=failure),
            ),
            caplog.at_level(logging.ERROR),
        ):
            await run_seeders_in_background()  # must not raise

        errors = [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]
        assert any(
            "seeding FAILED at startup" in m and "example_crews" in m for m in errors
        ), errors
        assert not any("essential defaults" in m for m in errors)

    @pytest.mark.asyncio
    async def test_an_essential_failure_says_so(self, caplog):
        from src.seeds.startup import run_seeders_in_background

        with (
            patch(
                "src.seeds.seed_runner.run_all_seeders",
                new=AsyncMock(side_effect=SeedingError({"model_configs": "boom"})),
            ),
            caplog.at_level(logging.ERROR),
        ):
            await run_seeders_in_background()

        assert any(
            "essential defaults (model_configs)" in r.getMessage()
            for r in caplog.records
        )

    @pytest.mark.asyncio
    async def test_an_unexpected_error_is_logged_and_never_raised(self, caplog):
        from src.seeds.startup import run_seeders_in_background

        with (
            patch(
                "src.seeds.seed_runner.run_all_seeders",
                new=AsyncMock(side_effect=RuntimeError("kaboom")),
            ),
            caplog.at_level(logging.ERROR),
        ):
            await run_seeders_in_background()

        assert any("unexpected error" in r.getMessage() for r in caplog.records)


def _session_factory_with_rows(present: bool) -> MagicMock:
    session = AsyncMock()
    result = MagicMock()
    result.scalar_one_or_none.return_value = 1 if present else None
    session.execute = AsyncMock(return_value=result)
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=session)
    ctx.__aexit__ = AsyncMock(return_value=None)
    return MagicMock(return_value=ctx)


class TestFreshLakebaseInstallation:
    @pytest.mark.asyncio
    async def test_an_essential_seeder_failure_blocks_startup(self):
        from src.seeds import installation

        with (
            patch.object(
                installation,
                "run_all_seeders",
                new=AsyncMock(side_effect=SeedingError({"tools": "boom"})),
            ),
            patch.object(
                installation, "async_session_factory", _session_factory_with_rows(True)
            ),
        ):
            with pytest.raises(RuntimeError, match="essential seeder.*tools"):
                await installation.seed_installed_database()

    @pytest.mark.asyncio
    async def test_a_non_essential_failure_is_logged_and_startup_continues(
        self, caplog
    ):
        from src.seeds import installation

        with (
            patch.object(
                installation,
                "run_all_seeders",
                new=AsyncMock(side_effect=SeedingError({"example_crews": "boom"})),
            ),
            patch.object(
                installation, "async_session_factory", _session_factory_with_rows(True)
            ),
            caplog.at_level(logging.ERROR),
        ):
            await installation.seed_installed_database()  # must not raise

        assert any(
            "Non-essential seeding failed" in r.getMessage()
            and "example_crews" in r.getMessage()
            for r in caplog.records
        )

    @pytest.mark.asyncio
    async def test_empty_essential_tables_still_block_startup(self):
        from src.seeds import installation

        with (
            patch.object(installation, "run_all_seeders", new=AsyncMock()),
            patch.object(
                installation,
                "async_session_factory",
                _session_factory_with_rows(False),
            ),
        ):
            with pytest.raises(RuntimeError, match="no seeded defaults"):
                await installation.seed_installed_database()
