# Kasal documentation

Kasal is an AI agent workflow orchestration platform for Databricks. This hub links every active doc, grouped by what you're trying to do.

New here? Start with [Why Kasal](./WHY_KASAL.md) for the problem it solves, then the [end-user tutorial](./END_USER_TUTORIAL_CATALOG.md) to build your first workflow. The docs are organized in the four [Diátaxis](https://diataxis.fr/) modes (tutorials, how-to guides, reference, and concepts), plus dedicated sections for Power BI migration and security.

## In this hub

- [Get started and tutorials](#get-started-and-tutorials)
- [How-to guides](#how-to-guides)
- [Reference](#reference)
- [Concepts and architecture](#concepts-and-architecture)
- [Power BI to Unity Catalog migration](#power-bi-to-unity-catalog-migration)
- [Security and compliance](#security-and-compliance)
- [Run Kasal locally](#run-kasal-locally)
- [Archive](#archive)
- [For contributors](#for-contributors)

## Get started and tutorials

Learn Kasal by building something end to end.

- [End-user tutorial: build a blog workflow](./END_USER_TUTORIAL_CATALOG.md): build and run a multi-agent blog-production workflow, then customize it from the Catalog, with no admin setup.
- [Genie superstore insights blueprint](./Blueprints/Genie_as_Backend%20_for_Agent_Workflows/README.md): wire Databricks Genie into an agent workflow to retrieve enterprise data and generate business insights.
- [Example crews and flows](./examples/README.md): ready-to-import JSON definitions for the Power BI to UCMV migration pipeline.

## How-to guides

Reach a specific goal, assuming you already know the basics.

- [Crew export and deployment](./crew-export-deployment.md): export CrewAI crews to Python projects, Databricks notebooks, or deployable Databricks Apps, and ship them to Model Serving.
- [Lakebase setup for Kasal](./lakebase-deployment.md): configure managed Lakebase PostgreSQL so crews, agents, tasks, and run history survive Databricks Apps restarts.
- [MLflow tracing setup](./mlflow-tracing-setup.md): export every crew and flow execution to MLflow Tracing for observability.
- [Developer guide](./DEVELOPER_GUIDE.md): day-to-day workflows for building, extending, and debugging Kasal.

## Reference

Look up exact facts: endpoints, config keys, and repository layout.

- [API endpoints reference](./api_endpoints.md): complete reference for every Kasal REST API endpoint.
- [Code structure](./CODE_STRUCTURE_GUIDE.md): a skimmable map of the repository to find the right place fast.
- [UCMV pipeline config guide](./UCMV_PIPELINE_CONFIG_GUIDE.md): every config key in the UCMV pipeline, and which are auto-extracted versus human-supplied.
- [Third-party notices](./THIRD_PARTY_NOTICES.md): attributions for included and conforming open-source work.

## Concepts and architecture

Understand why Kasal is built the way it is.

- [Why Kasal](./WHY_KASAL.md): the problems Kasal solves and who it's for on Databricks.
- [Solution architecture](./ARCHITECTURE_GUIDE.md): platform layers, request lifecycle, and the security model.
- [PBI / UCMV roadmap and architecture assessment](./README_ARCHITECTURE_PBI_UCMV_ROADMAP.md): assessment of the Power BI to UCMV/Genie tooling with a prioritized next-release plan.
- [CrewAI engine refactor proposal](./crewai-engine-refactor-proposal.md): the restructure of `src/engines/crewai` into path, kernel, and infra packages, with the dead-code audit and migration log.

## Power BI to Unity Catalog migration

Migrate Power BI semantic models to Unity Catalog Metric Views and run live analytics against Power BI data.

- [Power BI integration](./powerbi/README.md): section hub covering the tool map, authentication, and the full UCMV migration guide.
- [UCMV pipeline config guide](./UCMV_PIPELINE_CONFIG_GUIDE.md): config-key reference for the migration pipeline.
- [Example crews and flows](./examples/README.md): importable pipeline crews and flows for the migration.

## Security and compliance

How Kasal protects workflows, dependencies, and tenant data.

- [Security compliance](./README_SECURITY_COMPLIANCE.md): mapping of Databricks AI security guidance to its Kasal implementation, with runtime log evidence.
- [Security guardrails test guide](./README_SECURITY_GUARDRAILS_TESTGUIDE.md): verify all five phases of security measures via automated tests and manual inspection.
- [Supply chain security](./README_SECURITY_SUPPLY_CHAIN.md): impact of the litellm supply chain compromise and the dependency-layer defenses proposed in response.

## Run Kasal locally

Run the backend and frontend on your machine. The backend uses `uv` for dependencies and auto-reloads; the frontend uses hot module replacement.

```bash
git clone https://github.com/databrickslabs/kasal

# Start the backend (uv syncs dependencies automatically)
cd kasal/src/backend && ./run.sh

# In another terminal, start the frontend
cd kasal/src/frontend && npm install && npm start
```

The app is served at `http://localhost:3000`. For deeper setup, see the [developer guide](./DEVELOPER_GUIDE.md).

## Archive

Superseded pages are kept for reference but are no longer maintained.

- [Archived documentation](./archive/README.md): legacy technical, security, and guide docs from before the documentation redesign.

## For contributors

Writing or editing docs in `src/docs/`? Follow the [documentation style guide](./DOCUMENTATION_STYLE_GUIDE.md), which covers the Diátaxis modes, page anatomy, linking rules, and the per-page checklist. Each subfolder (`powerbi/`, `examples/`, `archive/`, `Blueprints/`) has its own `README.md` index; keep this hub's links in sync when you add or move a page.
