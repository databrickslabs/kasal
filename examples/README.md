# Kasal Examples

## Quick Start — BI Specialist Workspace

The fastest way to get started with Power BI → Databricks migration is the **bi-specialist workspace**
built into Kasal. On startup, Kasal automatically seeds a workspace with 9 pre-configured crew
templates covering the full pipeline — no JSON import required.

1. Open Kasal → **Workspaces** → switch to **BI Specialist**
2. Go to **Crews** — all 9 pipeline crews are already there with tools pre-selected
3. Fill in your credentials in each crew's tool config
4. Build your flow by dragging crews onto the **Flows** canvas

For importable JSON crew definitions, see **[`src/docs/examples/`](../src/docs/examples/)**.

---

## UC Metric View Migration — Local Scripts

The `uc_metric_view_migration/` directory contains **standalone Python utilities** for running
the UC Metric View pipeline locally without the Kasal UI. These are useful for development,
debugging, and batch processing.

| Script | Purpose |
|--------|---------|
| `run_locally.py` | Run the UCMV Generator against a live Power BI dataset |
| `config_scaffold.py` | Auto-propose `pipeline_config.json` from PBI extraction JSONs |
| `gap_analyzer.py` | Analyze config gaps to unlock more measure translations |
| `generate_config.py` | Generate a complete `pipeline_config.json` |
| `deploy_test.py` | Test deployment of generated UC Metric Views |
| `run_all_limitations_demo.py` | Demo showing current conversion limitations |

See [`uc_metric_view_migration/README.md`](uc_metric_view_migration/README.md) for setup and usage.

---

## Security

All example files use placeholder credentials (`<YOUR_...>` or empty strings).
Never commit real tokens, client secrets, or tenant IDs.
Store credentials in Kasal's **Settings → API Keys** store.
