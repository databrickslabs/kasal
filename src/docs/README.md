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
- [Internal notes](#internal-notes)
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
- [Prompt optimization (GEPA) setup](./prompt-optimization-setup.md): the Unity Catalog grant the app service principal needs to register prompts before an optimization run.
- [Measuring workflow-recipe effectiveness](./workflow-recipe-measurement.md): run a controlled holdout to find out whether reusing past crews actually improves generated ones.
- [Developer guide](./DEVELOPER_GUIDE.md): local setup, local authentication, the architecture rules, and how to test your change.

## Reference

Look up exact facts: endpoints, config keys, and repository layout.

- [API endpoints reference](./api_endpoints.md): complete reference for every Kasal REST API endpoint, including authentication and status codes.
- [Configuration reference](./CONFIGURATION.md): the environment variables Kasal reads, with defaults and where each is read.
- [Continuous integration](./continuous-integration.md): the CI workflows, what gates a pull request, and how to run the same checks locally.
- [Code structure](./CODE_STRUCTURE_GUIDE.md): a skimmable map of the repository to find the right place fast.
- [UCMV pipeline config guide](./UCMV_PIPELINE_CONFIG_GUIDE.md): every config key in the UCMV pipeline, and which are auto-extracted versus human-supplied.
- [Third-party notices](./THIRD_PARTY_NOTICES.md): attributions for included and conforming open-source work.

## Concepts and architecture

Understand why Kasal is built the way it is.

- [Why Kasal](./WHY_KASAL.md): the problems Kasal solves and who it's for on Databricks.
- [Solution architecture](./ARCHITECTURE_GUIDE.md): platform layers, request lifecycle, and the security model.
- [LLM architecture](./LLM_ARCHITECTURE.md): the four layers behind a model call — facade, configuration, endpoint policy, transport — and which one owns what.
- [Memory](./MEMORY.md): what a memory record is, how it is written and recalled, the passes that keep the store true, and which tuning knobs actually do something.
- [Flows](./flows.md): what a flow is, how one is authored and compiled, and how state, routing, checkpoints and approval gates work end to end.
- [Conversational flow state](./conversational-flow-state.md): channels, reducers, and the thread that lets a flow answer a follow-up question instead of starting over.
- [Workflow recipes](./workflow-recipes.md): why Kasal keeps the crews you have already run, and why it refuses to reuse them until a person says they were any good.
- [PBI → UCMV pipeline architecture](./powerbi/ucmv-pipeline-architecture.md): end-to-end walkthrough of how a Power BI model becomes UC Metric Views — extraction, config generation, the M-query path, and the LLM-first DAX translation with skill files, with the code location of each stage.
- [Harnesses](./harnesses.md): Kasal's own agent runtime and the CrewAI harness, and how a run picks one.

## Power BI to Unity Catalog migration

Migrate Power BI semantic models to Unity Catalog Metric Views and run live analytics against Power BI data.

- [Power BI integration](./powerbi/README.md): section hub covering the tool map, authentication, and the full UCMV migration guide.
- [UCMV pipeline config guide](./UCMV_PIPELINE_CONFIG_GUIDE.md): config-key reference for the migration pipeline.
- [Example crews and flows](./examples/README.md): importable pipeline crews and flows for the migration.

## Security and compliance

How Kasal protects workflows, dependencies, and tenant data.

- [Security](./SECURITY.md): identity, request authentication, authorization boundaries, and the security controls.
- [Security compliance](./README_SECURITY_COMPLIANCE.md): mapping of Databricks AI security guidance to its Kasal implementation, with runtime log evidence.
- [Security guardrails test guide](./README_SECURITY_GUARDRAILS_TESTGUIDE.md): verify all five phases of security measures via automated tests and manual inspection.
- [Supply chain security](./README_SECURITY_SUPPLY_CHAIN.md): impact of the litellm supply chain compromise and the dependency-layer defenses proposed in response.

## Run Kasal locally

Run the backend and frontend on your machine. You need Python 3.11, [uv](https://docs.astral.sh/uv/) and Node.js 22.

```bash
git clone https://github.com/nehmetohme/kasal.git

# Start the backend: SQLite, http://127.0.0.1:8000, dependencies synced by uv
cd kasal/src/backend && ./run.sh

# In another terminal, start the frontend
cd kasal/src/frontend && npm ci && npm start
```

The app is served at `http://localhost:3000`. `run.sh` turns on a local development identity (`LOCAL_DEV_AUTH`); without it, API calls get 401. For details, see the [quick start](./QUICK_START.md#run-locally) and the [developer guide](./DEVELOPER_GUIDE.md).

## Internal notes

Engineering proposals, backlogs and records. They ship with the docs but are not user documentation; each carries a status banner.

- [CrewAI engine refactor proposal](./crewai-engine-refactor-proposal.md): historical record of the engine restructure; its paths describe the deleted engines layout.
- [Conversational flow state proposal](./conversational-flow-state-proposal.md): the design record behind [conversational flow state](./conversational-flow-state.md).
- [Dual-harness backlog](./dual-harness-backlog.md): open items from running Kasal and CrewAI side by side.
- [Internal DBU tagging plan](./internal-dbu-tagging-plan.md): a plan, not built, to tag Kasal-created resources for cost attribution.
- [Platform feedback for product teams](./kasal-platform-feedback-for-product-teams.md): field-proven asks for Databricks product teams.

## Archive

Superseded pages are kept for reference but are no longer maintained.

- [Archived documentation](./archive/README.md): legacy technical, security, and guide docs from before the documentation redesign.

## For contributors

Writing or editing docs in `src/docs/`? Follow the [documentation style guide](./DOCUMENTATION_STYLE_GUIDE.md), which covers the Diátaxis modes, page anatomy, linking rules, and the per-page checklist. Each subfolder (`powerbi/`, `examples/`, `archive/`, `Blueprints/`) has its own `README.md` index; keep this hub's links in sync when you add or move a page.
