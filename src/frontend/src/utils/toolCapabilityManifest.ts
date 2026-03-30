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

const _S = CAP_SENSITIVE;
const _U = CAP_UNTRUSTED;
const _E = CAP_EXTERNAL;

/** Tool capability registry — keys are display names (tool.title) */
const TOOL_CAPABILITIES: Record<string, number> = {
  // Databricks / internal data tools
  'GenieTool':                                              _S | _E,
  'genie_tool':                                             _S | _E,
  'DatabricksJobsTool':                                     _S | _E,
  'databricks_jobs_tool':                                   _S | _E,
  'DatabricksKnowledgeSearchTool':                          _S | _E,
  'databricks_knowledge_search_tool':                       _S | _E,
  'AgentBricksTool':                                             _E,
  'agent_bricks_tool':                                           _E,

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
