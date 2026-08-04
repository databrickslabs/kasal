#!/usr/bin/env python3
"""Diagnose why GEPA prompt optimization fails with a UC prompt-registry 403.

Runs the SAME check the optimization does (``register_prompt`` against the
Unity Catalog MLflow Prompt Registry) and reports, step by step, WHY it fails —
so you get "the app SP is missing MANAGE on <schema>" instead of a raw stack
trace.

Run it wherever the failing identity's credentials live:

* **Inside the deployed Databricks App** (authenticates as the app SP — the
  identity that actually fails): open a terminal in the app and run
      python backend/scripts/diagnose_prompt_registry.py <catalog>.<schema>
* **Locally** with a PAT/SP creds in the environment
  (DATABRICKS_HOST + DATABRICKS_TOKEN, or DATABRICKS_CLIENT_ID/SECRET):
      python src/backend/scripts/diagnose_prompt_registry.py <catalog>.<schema>

It CREATES a throwaway probe prompt and DELETES it — nothing is left behind on
success. On failure it prints the exact GRANT to run.

Root cause this exists for: the MLflow Prompt Registry stores a prompt as a UC
FUNCTION needing CREATE FUNCTION + EXECUTE + MANAGE on the schema, and UC's
``GRANT ALL PRIVILEGES`` deliberately EXCLUDES MANAGE — so a service principal
with ALL PRIVILEGES still 403s and SHOW GRANTS hides the gap.
"""

from __future__ import annotations

import os
import sys
import uuid


def _ok(msg: str) -> None:
    print(f"  ✓ {msg}")


def _fail(msg: str) -> None:
    print(f"  ✗ {msg}")


def main() -> int:
    schema = sys.argv[1] if len(sys.argv) > 1 else os.getenv("KASAL_PROMPT_SCHEMA", "")
    if not schema or schema.count(".") != 1:
        print("usage: diagnose_prompt_registry.py <catalog>.<schema>")
        print("  e.g. diagnose_prompt_registry.py ai_specialist.kasal")
        return 2
    catalog = schema.split(".")[0]

    print(f"Diagnosing MLflow prompt registry for schema: {schema}\n")

    # 1. Credentials present?
    host = os.getenv("DATABRICKS_HOST")
    has_token = bool(os.getenv("DATABRICKS_TOKEN"))
    has_oauth = bool(
        os.getenv("DATABRICKS_CLIENT_ID") and os.getenv("DATABRICKS_CLIENT_SECRET")
    )
    print("1. Databricks credentials")
    if host:
        _ok(f"DATABRICKS_HOST = {host}")
    else:
        _fail("DATABRICKS_HOST not set")
    if has_token:
        _ok("DATABRICKS_TOKEN present (PAT / OBO)")
    if has_oauth:
        _ok(f"OAuth M2M present (client_id={os.getenv('DATABRICKS_CLIENT_ID')})")
    if not (has_token or has_oauth):
        _fail("no token and no OAuth client — cannot authenticate")
        return 1

    # 2. MLflow import + registry URI.
    print("\n2. MLflow / registry")
    try:
        import mlflow

        mlflow.set_registry_uri("databricks-uc")
        _ok(f"mlflow {mlflow.__version__}, registry_uri=databricks-uc")
    except Exception as e:  # noqa: BLE001
        _fail(f"MLflow unavailable: {e}")
        return 1

    # 3. The actual operation: create a throwaway prompt, then delete it.
    print("\n3. register_prompt probe (the exact call GEPA makes)")
    probe = f"{schema}.kasal_diag_{uuid.uuid4().hex[:8]}"
    try:
        pv = mlflow.genai.register_prompt(
            name=probe, template="diag {{x}}", commit_message="kasal diagnostic"
        )
        _ok(f"register_prompt SUCCEEDED (created {pv.name} v{pv.version})")
        # Clean up — best-effort.
        try:
            from mlflow import MlflowClient

            c = MlflowClient(registry_uri="databricks-uc")
            try:
                c.delete_prompt_version(probe, pv.version)
            except Exception:
                pass
            c.delete_prompt(probe)
            _ok("probe prompt cleaned up")
        except Exception as ce:  # noqa: BLE001
            print(f"  ! could not auto-delete probe {probe}: {str(ce)[:120]}")
        print(
            "\nRESULT: prompt registry works for this identity. GEPA should "
            "register prompts fine."
        )
        return 0
    except Exception as e:  # noqa: BLE001
        text = str(e)
        _fail(f"register_prompt FAILED: {text[:200]}")
        print("\nRESULT: diagnosis")
        if "PERMISSION_DENIED" in text or "Permission denied" in text:
            print(
                f"  The identity lacks a required Unity Catalog privilege on "
                f"'{schema}'. The prompt registry needs CREATE FUNCTION + EXECUTE "
                f"+ MANAGE — and GRANT ALL PRIVILEGES does NOT include MANAGE.\n"
                f"  Fix (as a catalog admin), targeting the app SP application id:\n"
                f"    GRANT USE CATALOG ON CATALOG {catalog} TO `<app-sp-application-id>`;\n"
                f"    GRANT USE SCHEMA, CREATE FUNCTION, EXECUTE, MANAGE ON SCHEMA "
                f"{schema} TO `<app-sp-application-id>`;\n"
                f"  Verify with: SHOW GRANTS `<app-sp-application-id>` ON SCHEMA "
                f"{schema};  (MANAGE must be listed explicitly, not just ALL PRIVILEGES)\n"
                f"  Note: a workspace admin USER bypasses this check, so 'it works "
                f"for me' does not prove the SP is configured."
            )
        elif "required scopes: mlflow" in text:
            print(
                "  The app SP's token has no 'mlflow' scope. Attach an MLflow "
                "experiment resource to the Databricks App (Can Edit), then "
                "redeploy — that is what grants the SP MLflow access."
            )
        elif "does not exist" in text or "not found" in text.lower():
            print(
                f"  The schema '{schema}' may not exist, or Prompt Registry "
                f"(Beta) is not enabled on the Previews page for this workspace."
            )
        else:
            print("  Unrecognized error — see the full text above.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
