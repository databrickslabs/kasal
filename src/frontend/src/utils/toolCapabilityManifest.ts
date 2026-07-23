/**
 * Frontend replica of src/backend/src/engines/crewai/security/tool_capability_manifest.py
 *
 * Capability flags and trifecta assessment logic for the pre-flight security warning.
 * Keep in sync with the backend manifest when adding new tools.
 */

// Capability bit-flags
const CAP_SENSITIVE   = 1 << 0;  // reads internal/confidential data
const CAP_UNTRUSTED   = 1 << 1;  // fetches external, attacker-reachable content
const CAP_EXTERNAL    = 1 << 2;  // makes outbound network requests
const CAP_DESTRUCTIVE = 1 << 3;  // triggers irreversible actions (write, delete, run)

const _S = CAP_SENSITIVE;
const _U = CAP_UNTRUSTED;
const _E = CAP_EXTERNAL;
const _D = CAP_DESTRUCTIVE;

/** Tool capability registry — keys are display names (tool.title) */
const TOOL_CAPABILITIES: Record<string, number> = {
  // Databricks / internal data tools
  'GenieTool':                                              _S | _E,
  'genie_tool':                                             _S | _E,
  'DatabricksJobsTool':                                     _S | _E,
  'databricks_jobs_tool':                                   _S | _E,
  'DatabricksKnowledgeSearchTool':                          _S | _E,
  'databricks_knowledge_search_tool':                       _S | _E,
  // Agent Bricks endpoints are opaque Mosaic AI agents: behind one endpoint
  // there may be Genie/Vector Search/UC reads AND web browsing AND outbound
  // calls — we can't see inside, so we assume all three capabilities. One
  // selected endpoint can therefore trip the trifecta on its own.
  'AgentBricksTool':                                        _S | _U | _E,
  'agent_bricks_tool':                                      _S | _U | _E,

  // Web / external search tools (ingest untrusted content)
  'SerperDevTool':                                          _U | _E,
  'search_the_internet_with_serper':                        _U | _E,
  'Search the internet with Serper':                        _U | _E,
  'PerplexityTool':                                         _U | _E,
  'perplexity_tool':                                        _U | _E,
  'ScrapeWebsiteTool':                                      _U | _E,
  'scrape_website':                                         _U | _E,

  // Image generation
  'Dall-E Tool':                                                 _E,

  // MCP
  'MCPTool':                                                _U | _E,

  // Power BI tools
  'PowerBIAnalysisTool':                                    _S | _E,
  'Power BI Comprehensive Analysis Tool':                   _S | _E,
  'Power BI Intelligent Analysis (Copilot-Style)':          _S | _E,
  'PowerBIConnectorTool':                                   _S | _E,
  'Power BI Connector':                                     _S | _E,
  'Measure Conversion Pipeline':                            _S | _E,
  'M-Query Conversion Pipeline':                            _S | _E,
  'Power BI Relationships Tool':                            _S | _E,
  'Power BI Hierarchies Tool':                              _S | _E,
  'Power BI Field Parameters & Calculation Groups Tool':    _S | _E,
  'Power BI Report References Tool':                        _S | _E,
  'Power BI Semantic Model Fetcher':                        _S | _E,
  'Power BI Semantic Model DAX Generator':                  _S | _E,
  'Power BI Metadata Reducer':                              _S | _E,
  'Power BI DAX Executor':                                  _S | _E,

  // Databricks Jobs runtime name
  'Databricks Jobs Manager':                                _S | _E,
};

export interface TrifectaAssessment {
  hasTrifecta: boolean;
  readsSensitive: boolean;
  ingestsUntrusted: boolean;
  communicatesExternally: boolean;
  sensitiveTools: string[];
  untrustedTools: string[];
  externalTools: string[];
  /** Tools that can write/delete/run (irreversible). Populated by
   *  assessChatModeTrifecta; optional so existing callers are unaffected. */
  destructiveTools?: string[];
}

/**
 * Classify a *managed/selected MCP server by display name*.
 *
 * Internet access (and untrusted-content ingestion) can be reached through ANY
 * MCP endpoint, and all we ever see is a server name — so we never try to
 * enumerate which servers reach the web. Instead we recognise only the finite,
 * knowable set of Databricks-managed *internal data sources* and flag those as
 * sensitive readers; EVERY other server name defaults to untrusted + external
 * (assume-the-worst), because we can't prove it's inert.
 *
 * Matched names come from /mcp/databricks/available (mcp_router.py). UC Functions
 * carry a dynamic "(catalog.schema)" suffix, so they're matched by substring.
 */
export function classifyMcpServer(name: string): number {
  const n = (name || '').trim().toLowerCase();
  if (!n) return 0;
  // Databricks SQL executes arbitrary statements with the caller's warehouse
  // permissions — it reads sensitive data AND can mutate/delete it.
  if (n === 'databricks sql') return _S | _E | _D;
  // Unity Catalog Functions run server-side UC functions; system.ai includes
  // python_exec (arbitrary code execution) so it's also treated as destructive.
  if (n.includes('unity catalog function')) {
    return n.includes('system.ai') ? (_S | _E | _D) : (_S | _E);
  }
  if (n.includes('genie')) return _S | _E;                 // Genie managed MCP
  if (n.includes('ai search') || n.includes('vector search')) return _S | _E;
  // Unknown / external / custom MCP server: could ingest untrusted content and
  // reach any external endpoint. We cannot prove otherwise, so assume both.
  return _U | _E;
}

export interface ChatModeTrifectaInput {
  /** Tool TITLES the generated crew is equipped with (resolved via toolNameMap). */
  toolTitles?: string[];
  /** MCP server display names selected in the chat picker. */
  mcpServers?: string[];
  /** Agent Bricks endpoint names selected in the chat picker (each equips AgentBricksTool). */
  agentBricksEndpoints?: string[];
}

/**
 * Assess the lethal trifecta for a ChatMode run, fusing the three capability
 * sources a chat crew is equipped from: catalog tool titles, selected MCP
 * servers (classified by name), and selected Agent Bricks endpoints.
 *
 * The trifecta still requires all three dimensions (sensitive + untrusted +
 * external). Because unrecognised MCP servers default to untrusted+external,
 * a known internal source combined with ANY unknown/web channel trips it —
 * while two purely-internal sources together (e.g. Genie + Databricks SQL) do
 * not, since neither contributes the untrusted dimension.
 */
export function assessChatModeTrifecta(input: ChatModeTrifectaInput): TrifectaAssessment {
  const sensitive = new Set<string>();
  const untrusted = new Set<string>();
  const external = new Set<string>();
  const destructive = new Set<string>();

  const add = (label: string, caps: number) => {
    if (caps & CAP_SENSITIVE)   sensitive.add(label);
    if (caps & CAP_UNTRUSTED)   untrusted.add(label);
    if (caps & CAP_EXTERNAL)    external.add(label);
    if (caps & CAP_DESTRUCTIVE) destructive.add(label);
  };

  for (const name of input.toolTitles ?? []) {
    add(name, TOOL_CAPABILITIES[name] ?? 0);
  }
  for (const name of input.mcpServers ?? []) {
    add(name, classifyMcpServer(name));
  }
  const agentBricksCaps = TOOL_CAPABILITIES['AgentBricksTool'] ?? 0;
  for (const ep of input.agentBricksEndpoints ?? []) {
    add(ep, agentBricksCaps);
  }

  const readsSensitive = sensitive.size > 0;
  const ingestsUntrusted = untrusted.size > 0;
  const communicatesExternally = external.size > 0;

  return {
    hasTrifecta: readsSensitive && ingestsUntrusted && communicatesExternally,
    readsSensitive,
    ingestsUntrusted,
    communicatesExternally,
    sensitiveTools: [...sensitive],
    untrustedTools: [...untrusted],
    externalTools: [...external],
    destructiveTools: [...destructive],
  };
}

/**
 * Assess whether a set of tool names satisfies the lethal-trifecta condition.
 * Mirrors assess_trifecta() from the backend manifest.
 */
export function assessTrifecta(toolNames: string[]): TrifectaAssessment {
  const sensitiveTools: string[] = [];
  const untrustedTools: string[] = [];
  const externalTools: string[] = [];

  for (const name of toolNames) {
    const caps = TOOL_CAPABILITIES[name] ?? 0;
    if (caps & CAP_SENSITIVE)  sensitiveTools.push(name);
    if (caps & CAP_UNTRUSTED)  untrustedTools.push(name);
    if (caps & CAP_EXTERNAL)   externalTools.push(name);
  }

  const readsSensitive      = sensitiveTools.length > 0;
  const ingestsUntrusted    = untrustedTools.length > 0;
  const communicatesExternally = externalTools.length > 0;
  const hasTrifecta = readsSensitive && ingestsUntrusted && communicatesExternally;

  return {
    hasTrifecta,
    readsSensitive,
    ingestsUntrusted,
    communicatesExternally,
    sensitiveTools,
    untrustedTools,
    externalTools,
  };
}
