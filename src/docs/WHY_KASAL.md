# Why Kasal

Kasal lets you build, run, and govern multi-agent AI workflows **inside your Databricks workspace**, using the data, models, and identity you already have, instead of standing up a separate AI stack and wiring it back to Databricks by hand.

- [The problem](#the-problem)
- [Who it's for](#who-its-for)
- [What makes Kasal different](#what-makes-kasal-different)
- [How it works](#how-it-works)
- [Built on Databricks](#built-on-databricks)
- [Getting started](#getting-started)
- [Licensing](#licensing)

---

## The problem

Multi-agent AI is easy to demo and hard to operationalize.

- **Build complexity.** Coordinating several agents, tools, and an LLM into something reliable takes a lot of bespoke code.
- **Glue-code overload.** Teams hand-stitch LLMs, vector search, tools, logging, and schedulers, then maintain that plumbing forever.
- **Enterprise friction.** The result still has to respect your identity, permissions, secrets, and data boundaries before it can touch production.

Kasal turns that into a repeatable, visual workflow that already speaks Databricks.

## Who it's for

- **Data and AI engineers** build reliable multi-agent workflows on top of the lakehouse.
- **Analysts and builders** design flows visually and reuse templates without deep infrastructure knowledge.
- **Platform teams** standardize how AI workflows are built, governed, and observed across the org.

## What makes Kasal different

- **Runs where your data lives.** Agents query Unity Catalog, search your vector indexes, and call your models without data leaving the workspace.
- **Uses your identity, not a service account.** OAuth and on-behalf-of (OBO) mean every action runs with the calling user's Databricks permissions.
- **Visual, not code-first.** A drag-and-drop designer lets non-engineers assemble and operate agent workflows, with AI assistance to generate agents, tasks, and crews.
- **Governed by default.** Group-aware multi-tenancy isolates each team's data and workflows, with centralized permissions and full execution history.
- **Pluggable engine.** CrewAI powers orchestration today, behind an engine abstraction so the execution layer can evolve without rebuilding your workflows.

## How it works

1. **Design.** Open the visual designer and drop agents onto the canvas. Each agent is a worker with specific skills and tools.
2. **Configure.** Describe what each agent should do in plain language (for example, "find Q4 revenue in the sales tables and summarize the top 3 trends").
3. **Connect.** Point agents at your lakehouse: Unity Catalog tables, SQL warehouses, Volumes for documents, and vector indexes for semantic search.
4. **Run and monitor.** Start the workflow and watch agents work in real time, with live logs, execution traces, and run history for every step.

## Built on Databricks

Kasal deploys as a native **Databricks App** and integrates with the platform end to end, with no separate infrastructure to run:

- **Identity and security.** OAuth sign-in, on-behalf-of execution, secret scopes for credentials, and isolation that honors your workspace's groups and permissions.
- **Data.** Unity Catalog and Delta tables queried through your SQL warehouses, with governance enforced by the platform.
- **Knowledge and retrieval.** Databricks Vector Search for semantic search, and Volumes for storing source documents.
- **Models.** Any Databricks Foundation Model API or Model Serving endpoint, plus your own MLflow-tracked models. External providers such as OpenAI and Anthropic are also supported.
- **Tools.** Purpose-built agent tools for Genie (natural-language data questions), SQL execution, Unity Catalog access, Vector Search, and Databricks Jobs.
- **Observability.** Execution traces and run history, with optional MLflow tracing for deeper inspection.

## Getting started

1. Install Kasal from the **Databricks Apps Marketplace**, or deploy it to your workspace with the included deploy tooling.
2. Launch it from your Databricks Apps. It inherits your workspace identity automatically, so there is no separate connection to configure.
3. Start from a template or a blank canvas, point the agents at your data, and run your first workflow.

## Licensing

Kasal is available under the **Databricks License** and runs on your own Databricks compute, so there are no separate Kasal licensing fees. You pay only standard Databricks rates for the compute, storage, and model usage your workflows consume.

---

## Related

- [Solution architecture guide](./ARCHITECTURE_GUIDE.md)
- [Developer guide](./DEVELOPER_GUIDE.md)
- [End-user tutorial](./END_USER_TUTORIAL_CATALOG.md)
- [API endpoints reference](./api_endpoints.md)
- [Code structure guide](./CODE_STRUCTURE_GUIDE.md)

Back to the [documentation hub](./README.md).
