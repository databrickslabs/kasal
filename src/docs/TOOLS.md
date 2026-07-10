# Tools

Tools give agents capabilities beyond text generation: searching the web, querying data, reading documents, calling Databricks endpoints, and migrating Power BI assets. An agent calls a tool when its task needs an action it cannot perform from the model alone, and the tool result feeds back into the agent's reasoning.

## When to use a tool

Add a tool to an agent when the task requires:

- Live or external information (web search, Power BI data, a database).
- Retrieval from your own documents (Knowledge Search).
- Calling an existing Databricks asset (a Genie space, an Agent Bricks endpoint, a job).
- A multi-step data engineering pipeline (the Power BI to Unity Catalog migration tools).

If a task can be answered from the model's own knowledge, it does not need a tool. Tools add latency and external dependencies, so attach only the ones an agent actually needs.

## Enabling tools

Tools are assigned to agents, not to tasks. In an agent's configuration you list the tools it may use by their registered name:

```json
{
  "name": "researcher",
  "role": "Research Specialist",
  "goal": "Find relevant information",
  "tools": ["SerperDevTool", "DatabricksKnowledgeSearchTool"]
}
```

At execution time the tool factory (`src/engines/crewai/tools/tool_factory.py`) looks up each name in the seeded tool registry, builds the tool with its configuration, and hands it to the agent. Each tool is configured automatically with the values it needs for tenant isolation and authentication, such as `group_id`, `execution_id`, and a user token for On-Behalf-Of (OBO) auth where supported.

### The registry

The set of available tools is seeded into the `tools` table from `src/backend/src/seeds/tools.py`. Each entry has an id, a title (the name agents reference), a description, an icon category, and a default configuration block. Tools are global by default (available to all groups) and enabled on seed. Configuration values, such as API parameters and Databricks or Power BI credentials, are set per tool either in the UI or in the seeded defaults.

### Native vs custom tools

- Native tools wrap capabilities provided by the CrewAI framework or third-party SDKs (for example `SerperDevTool`, `ScrapeWebsiteTool`, `Dall-E Tool`).
- Custom tools are implemented in this codebase under `src/backend/src/engines/crewai/tools/custom/` (for example the Genie, Knowledge Search, Agent Bricks, Gmail, Databricks Jobs, and Power BI tools). They follow CrewAI's `BaseTool` interface and call into Kasal's own service layer.

## Tool categories

The icon field in `tools.py` groups tools into the categories below. Names are exactly as registered.

| Category | Tools |
|----------|-------|
| ai | Dall-E Tool |
| development | SerperDevTool |
| web | ScrapeWebsiteTool |
| search | PerplexityTool, DatabricksKnowledgeSearchTool |
| database | GenieTool, DatabricksJobsTool, Power BI Comprehensive Analysis Tool, Power BI Semantic Model Fetcher, Power BI Semantic Model DAX Generator, Power BI Metadata Reducer, Power BI DAX Executor, Genie Space Generator, Databricks Dashboard Creator |
| databricks | AgentBricksTool |
| communication | Gmail |
| integration | MCPTool |
| transform | Measure Conversion Pipeline, M-Query Conversion Pipeline, Power BI Relationships Tool, Power BI Hierarchies Tool, Power BI Field Parameters & Calculation Groups Tool, Power BI Report References Tool, DAX to SQL Translator, UC Metric View Generator, PBI Measure Allocator, Metric View Deployer, Config Generator, Pipeline Config Generator, Metric View Validator, UCMV Genie Space Config Generator, PBI Visual-UCMV Mapper |

## Genie

`GenieTool` gives agents natural language access to a Databricks AI/BI Genie space. The agent asks a plain-language question, Genie translates it into SQL against the configured space, runs it, and returns the result. This is the right tool when you want non-technical, conversational access to governed data without writing SQL. Defaults cap the work per call (`max_calls` and `max_result_rows`) to keep runs bounded. The space id can be auto-filled when a Genie MCP server is picked.

A separate `Genie Space Generator` tool (and the `UCMV Genie Space Config Generator`) builds or updates a Genie space from deployed UC Metric Views, as the final step of the Power BI migration pipeline. Use `GenieTool` to query a space; use the generator tools to create one.

## Knowledge Search

`DatabricksKnowledgeSearchTool` performs semantic (vector) search over documents you have uploaded to Databricks Vector Search, providing Retrieval-Augmented Generation (RAG) for your agents. Unlike automatic knowledge sources, this is an explicit tool: the agent decides when to search, which makes behavior predictable and easy to debug.

Input fields:

- `query` (required): the search string.
- `limit` (optional): maximum number of results (default 5).
- `file_paths` (optional): restrict results to specific uploaded files.

The tool is configured automatically with `group_id`, `execution_id`, and a user token for OBO authentication, and it returns formatted results with a similarity score and source per match. See the [Knowledge Search tool guide](./archive/guides/knowledge-search-tool.md) and the [UI knowledge tool usage guide](./archive/guides/ui-knowledge-tool-usage.md).

## Agent Bricks

`AgentBricksTool` calls a Databricks Agent Bricks (Mosaic AI Agent Bricks) serving endpoint. Agent Bricks is Databricks' no-code agent builder, so this tool lets a CrewAI agent delegate a subtask to a pre-built, domain-specific Databricks agent and use its answer. It supports OBO, PAT, and Service Principal authentication, returns the endpoint response as the agent's answer (`result_as_answer` is true by default), and has a configurable `timeout`. Use it to compose Kasal's orchestration with specialized agents already running in your Databricks workspace.

## Web search

Several tools cover live web access:

- `SerperDevTool`: structured web search via the Serper.dev API, with configurable result count and geographic targeting. Use for current information and general research.
- `PerplexityTool`: question answering over the web with citations, useful for fact-checking and detailed explanations on specialized topics.
- `ScrapeWebsiteTool`: extracts the content of a specific web page for an agent to read.

Web tools require their respective API credentials in the tool configuration.

## Power BI

Kasal includes a full Power BI toolkit for two goals: answering business questions against live Power BI data, and migrating Power BI semantic models to Databricks Unity Catalog (UC Metric Views). These tools chain together into analytics and migration pipelines and use Service Principal or user OAuth authentication.

Rather than duplicate the details here, see the Power BI section: [Power BI integration](./powerbi/README.md). It covers authentication setup, the per-tool guides, and the end-to-end UCMV migration flow.

## Databricks Jobs

`DatabricksJobsTool` is read and monitor oriented. It lists jobs, fetches job and run details, analyzes notebook parameters, runs an existing job with parameters, and monitors run status. By design it must never create, submit, or otherwise execute arbitrary code: destructive and code-execution actions are excluded outright for agent safety, not hidden behind a flag. Use it to inspect and monitor jobs, not to author or launch new workloads.

## Custom tools and MCP

Beyond the seeded registry, agents can reach the wider Model Context Protocol (MCP) ecosystem through `MCPTool`, an adapter that connects to MCP servers (over SSE or stdio) and exposes their tools to agents without custom integration work. This is how you extend Kasal with domain-specific tools that are not built in.

For configuring MCP servers, per-teamspace overrides, and the Genie MCP integration, see the [MCP guide](./MCP.md).
