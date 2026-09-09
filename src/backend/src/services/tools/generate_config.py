#!/usr/bin/env python3
"""Generate pipeline_config.json from Power BI APIs.

Standalone script — no CrewAI, no LLM, no database.  Calls 4 PBI APIs
directly and produces a pipeline_config.json with auto-filled keys and
TODO markers where human input is needed.

Dependencies: requests (pip install requests)

Usage:
  python generate_config.py \
    --workspace-id ac0fa11c-... \
    --dataset-id ecdd57ae-... \
    --tenant-id 9f37a392-... \
    --client-id 7b597aac-... \
    --client-secret "U5b8Q~..." \
    --admin-client-id 8d8aa6ee-... \
    --admin-client-secret "RXm8Q~..." \
    --catalog my_catalog \
    --schema my_schema \
    --output proposed_pipeline_config.json
"""

import argparse
import json

if __package__:
    from src.services.powerbi import pipeline_config as _config
else:
    # Preserve direct `python path/to/generate_config.py` invocation from any
    # working directory. Load the sibling library by path without modifying
    # sys.path or searching a developer's examples directory.
    import importlib.util
    from pathlib import Path

    _path = Path(__file__).resolve().parents[1] / "powerbi" / "pipeline_config.py"
    _spec = importlib.util.spec_from_file_location("kasal_pipeline_config_cli", _path)
    _config = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_config)

__all__ = _config.__all__


def __getattr__(name):
    """Keep legacy helper imports working while new callers use powerbi."""
    return getattr(_config, name)


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Generate pipeline_config.json from Power BI APIs. "
            "No CrewAI, no LLM — pure API extraction."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Example:\n"
            "  python generate_config.py \\\n"
            "    --workspace-id ac0fa11c-... \\\n"
            "    --dataset-id ecdd57ae-... \\\n"
            "    --tenant-id 9f37a392-... \\\n"
            "    --client-id 7b597aac-... \\\n"
            "    --client-secret 'U5b8Q~...' \\\n"
            "    --admin-client-id 8d8aa6ee-... \\\n"
            "    --admin-client-secret 'RXm8Q~...' \\\n"
            "    --catalog my_catalog \\\n"
            "    --schema my_schema \\\n"
            "    --output proposed_pipeline_config.json"
        ),
    )

    parser.add_argument("--workspace-id", required=True, help="PBI workspace GUID")
    parser.add_argument("--dataset-id", required=True, help="PBI dataset GUID")
    parser.add_argument("--tenant-id", required=True, help="Azure AD tenant GUID")
    parser.add_argument(
        "--client-id",
        required=True,
        help="Non-admin SP client ID (workspace member, Execute Queries)",
    )
    parser.add_argument("--client-secret", required=True, help="Non-admin SP secret")
    parser.add_argument(
        "--admin-client-id",
        required=True,
        help="Admin SP client ID (Admin Scanner API)",
    )
    parser.add_argument("--admin-client-secret", required=True, help="Admin SP secret")
    parser.add_argument(
        "--catalog",
        default="main",
        help="Target UC catalog name (default: main)",
    )
    parser.add_argument(
        "--schema",
        default="default",
        help="Target UC schema name (default: default)",
    )
    parser.add_argument(
        "--output",
        default="proposed_pipeline_config.json",
        help="Output file path (default: proposed_pipeline_config.json)",
    )
    parser.add_argument(
        "--report-id",
        default=None,
        help="Optional PBI report GUID for report-layer metadata",
    )

    args = parser.parse_args()

    print("=" * 60)
    print("Pipeline Config Generator — PBI API Extraction")
    print("=" * 60)

    # ── Step 1: Auth ──────────────────────────────────────────────
    print("\n[1/4] Authenticating...")
    token = _config.get_token(args.tenant_id, args.client_id, args.client_secret)
    admin_token = _config.get_token(
        args.tenant_id, args.admin_client_id, args.admin_client_secret
    )
    print("  OK — both tokens acquired")

    # ── Step 2: Extract from APIs ────────────────────────────────
    print("\n[2/4] Extracting from Power BI APIs...")

    print("  API 1: INFO.VIEW.RELATIONSHIPS()...")
    relationships = _config.extract_relationships(
        token, args.workspace_id, args.dataset_id
    )
    print(f"    → {len(relationships)} relationships")

    print("  API 2: $SYSTEM.MDSCHEMA_MEASURES...")
    measures = _config.extract_measures(token, args.workspace_id, args.dataset_id)
    print(f"    → {len(measures)} measures")

    print("  API 3: Admin Scanner (workspace scan)...")
    scan_result = _config.trigger_admin_scan(admin_token, args.workspace_id)
    admin_tables = _config.parse_admin_tables(scan_result, dataset_id=args.dataset_id)
    print(f"    → {len(admin_tables)} tables in admin scan")

    report_def = None
    if args.report_id:
        print("  API 4: Report Definition...")
        report_def = _config.extract_report_definition(
            token,
            args.workspace_id,
            args.report_id,
            tenant_id=args.tenant_id,
            client_id=args.client_id,
            client_secret=args.client_secret,
        )
        if report_def:
            print("    → Report definition retrieved")
    else:
        print("  API 4: Report Definition — skipped (no --report-id)")

    # ── Step 3: Build config ─────────────────────────────────────
    print("\n[3/4] Deriving config keys...")
    config = _config.build_config(
        relationships,
        measures,
        admin_tables,
        report_def,
        catalog=args.catalog,
        schema=args.schema,
    )

    # Print per-key summary
    for key, val in config.items():
        if val is None:
            status = "null"
        elif isinstance(val, dict):
            status = f"{len(val)} entries"
        elif isinstance(val, list):
            status = f"{len(val)} items"
        elif isinstance(val, str) and "TODO" in val:
            status = "TODO"
        elif isinstance(val, str):
            status = f'"{val}"'
        else:
            status = str(val)
        print(f"  {key}: {status}")

    # ── Step 4: Output ───────────────────────────────────────────
    print(f"\n[4/4] Writing {args.output}...")
    with open(args.output, "w") as f:
        json.dump(config, f, indent=2, default=str)

    # ── Summary ──────────────────────────────────────────────────
    config_json = json.dumps(config, default=str)
    auto_count = 0
    todo_count = 0
    for key, val in config.items():
        val_str = json.dumps(val, default=str)
        if "TODO" in val_str:
            todo_count += 1
        elif val:
            auto_count += 1

    print(f"\n{'=' * 60}")
    print("CONFIG SUMMARY")
    print(f"{'=' * 60}")
    print(f"  Total keys: {len(config)}")
    print(f"  Auto-filled: {auto_count}")
    print(f"  Need human review (TODO): {todo_count}")
    print(f"  Empty/null: {len(config) - auto_count - todo_count}")
    print(f"  Output: {args.output}")
    print(f"\nTotal TODO markers in output: {config_json.count('TODO')}")
    print()


if __name__ == "__main__":
    main()
