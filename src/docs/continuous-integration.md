# Continuous integration

The GitHub Actions workflows in `.github/workflows/`, what each job checks, which checks fail a pull request and which only report, and how to run the same checks locally.

- [Workflows at a glance](#workflows-at-a-glance)
- [Code quality](#code-quality)
- [Security scanning](#security-scanning)
- [Dependency updates](#dependency-updates)
- [Run the checks locally](#run-the-checks-locally)
- [Report-only checks and their follow-ups](#report-only-checks-and-their-follow-ups)

## Workflows at a glance

Every workflow runs on pull requests, on pushes to `main` and on manual dispatch; the security workflows also run weekly. All of them check out the code with `persist-credentials: false` and pin third-party actions to full commit SHAs.

The workflows are:

| Workflow | File | Jobs | Schedule |
|---|---|---|---|
| Code Quality | `quality.yml` | `backend-tests`, `backend-lint`, `migrations`, `frontend`, `frontend-coverage` | None |
| CodeQL | `codeql.yml` | `analyze` (Python, JavaScript/TypeScript) | Mondays 05:30 UTC |
| Secret Scan | `secret-scan.yml` | `gitleaks` | Mondays 05:00 UTC |
| Dependency Audit | `dependency-audit.yml` | `lockfile-hygiene`, `python-cve`, `npm-cve` | Mondays 06:00 UTC |

Dependabot (`.github/dependabot.yml`) is configuration, not a workflow; see [Dependency updates](#dependency-updates).

## Code quality

`quality.yml` runs five independent jobs, so a failing test does not hide lint feedback.

The jobs are:

| Job | What it runs | Gates |
|---|---|---|
| `backend-tests` | `uv sync --frozen`, then `run_tests.py --parallel 2 --skip-lint --coverage` on Python 3.11. Uploads `coverage.xml` | Tests gate. Coverage is report-only: no `fail_under` is set yet |
| `backend-lint` | `run_tests.py --lint-only`: `black --check`, `isort --check-only`, `ruff check`, `check_types.py` (mypy, no new errors against `mypy-baseline.json`) and `lint-imports` (the architecture contracts). The type stubs mypy needs, such as `types-psutil`, are in the `dev` dependency group, so `uv sync --frozen` installs them | Yes |
| `migrations` | On a `pgvector/pgvector:pg16` service: exactly one Alembic head; `init_db()` builds the app schema on an empty PostgreSQL; `alembic upgrade head` from empty | The first two gate. The upgrade step is report-only (`continue-on-error`) |
| `frontend` | Node 22: `npm ci`, `npm run test:run` (Vitest), `npm run lint` (ESLint), `npm run build` (`tsc -b` and `vite build`) | Yes. ESLint warnings do not fail the job; errors do |
| `frontend-coverage` | `vitest run --coverage` with `VITEST_COVERAGE_REPORT_ONLY=1`, which drops the per-path thresholds in `vitest.config.ts`. Uploads `coverage/` | No: the job is `continue-on-error` |

The `migrations` job exists because the app does not run Alembic at startup. It builds its schema with `init_db()` (`create_all` plus the self-heal steps in `src/backend/src/db/self_heal/`), and the unit tests run on SQLite, so this job is the check that the models build a working schema on PostgreSQL.

## Security scanning

**CodeQL** runs static analysis for Python and for JavaScript/TypeScript with `build-mode: none`. Findings appear under **Security > Code scanning** on GitHub.

**Secret Scan** runs the gitleaks CLI (a pinned version, verified by SHA-256) with `--redact`:

- On a pull request or push it scans only the commits the change adds, and a finding fails the job.
- On the weekly schedule or a manual run it scans the whole history. That scan is report-only for now, because the full history still has unreviewed historical hits.

**Dependency Audit** has three jobs:

- `lockfile-hygiene` fails if `src/backend/uv.lock` or `src/frontend/package-lock.json` references an internal package proxy. Committed lockfiles must use public registries so the Databricks Apps build can install.
- `python-cve` runs `uv lock --check` (the lock must match `pyproject.toml`), then `pip-audit` over the exported, locked dependencies. Advisories with no fixed release are ignored by ID in the workflow; their exposure is described in [validation and security](./VALIDATION_AND_SECURITY.md).
- `npm-cve` runs `npm ci` (the lock must be consistent) and `npm audit --audit-level=high`.

## Dependency updates

Dependabot proposes updates every Monday for three ecosystems:

- GitHub Actions: all updates grouped into one pull request.
- `uv` in `src/backend`: minor and patch updates grouped into one pull request; majors arrive one per pull request.
- `npm` in `src/frontend`: the same grouping as `uv`.

Dependabot proposes fixes; the Dependency Audit workflow detects the vulnerabilities.

## Run the checks locally

Run the backend checks from `src/backend`:

```bash
uv sync --frozen
uv run python run_tests.py --lint-only   # what backend-lint runs
uv run python run_tests.py --skip-lint   # tests only, parallel by default
uv run python run_tests.py --skip-lint --coverage --html-coverage
uv run python run_tests.py               # tests, then every lint step
```

`run_tests.py` runs every lint step even when an earlier one fails, so you see all failures at once. `--lint-only` and `--skip-lint` cannot be combined. To fix formatting rather than check it, run `uv run black src tests` and `uv run isort src tests`.

Run the frontend checks from `src/frontend`:

```bash
npm ci
npm run test:run
npm run lint
npm run build
npx vitest run --coverage   # coverage report in coverage/
```

To reproduce the supply-chain checks, run `uv lock --check` in `src/backend` and `npm audit --audit-level=high` in `src/frontend`.

## Report-only checks and their follow-ups

These checks run but cannot fail a pull request yet. Each workflow file records its own follow-up:

- **Backend coverage**: set `fail_under` in `[tool.coverage.report]` in `src/backend/pyproject.toml` from the reported total, then ratchet it up.
- **Frontend coverage**: set the per-path floors in `vitest.config.ts` from the first report, remove `VITEST_COVERAGE_REPORT_ONLY`, then drop `continue-on-error`.
- **`alembic upgrade head` from empty**: the migration history has several roots and no baseline revision, so it cannot build a database from nothing. Add a baseline (or squash), then make the step gate and add `alembic check`.
- **Full-history secret scan**: triage the historical hits, rotate anything real, record the rest in a `.gitleaksignore`, then let the weekly scan gate.

## See also

- [Developer guide](./DEVELOPER_GUIDE.md)
- [Validation and security](./VALIDATION_AND_SECURITY.md)
- [Supply chain security](./README_SECURITY_SUPPLY_CHAIN.md)
- [Configuration reference](./CONFIGURATION.md)

Back to the [documentation hub](./README.md).
