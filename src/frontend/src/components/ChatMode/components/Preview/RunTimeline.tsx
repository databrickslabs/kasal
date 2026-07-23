/**
 * Shared primitives for the run-activity views. The activity is rendered as a
 * single flowing "thinking" stream ({@link ThinkingStream}) both while the run is
 * live (PreviewSkeleton) and after it finishes (PreviewPanel) — this module just
 * holds the step shape and the label→phase mapping both of them use.
 */

/** One step on the run activity (a tool/activity the agent ran). Sourced from
 *  the persistent chat trace messages. */
export interface RunStep {
  id: string;
  label: string;
  sublabel?: string;
  detail?: string;
  durationMs?: number;
  timestamp?: number;
}

/**
 * True only for genuine web-search tools (Perplexity, Serper, Tavily, Brave,
 * DuckDuckGo, an explicit `web_search`/`search_web`, …). The bare word "search"
 * is deliberately NOT enough: many catalog/data tools (e.g. `search_data_products`)
 * contain "search" but never touch the web — mislabeling them as a web search is
 * both wrong and misleading in the activity stream.
 */
export function isWebSearch(label: string): boolean {
  const l = (label || '').toLowerCase();
  return /perplex|serper|tavily|duckduckgo|brave[\s_-]?search|exa[\s_-]?search|web[\s_-]?search|search[\s_-]?(?:the[\s_-]?)?web|google[\s_-]?search|internet[\s_-]?search|search[\s_-]?internet/.test(l);
}

/** Map a raw tool/activity label to a friendly, business-readable phase verb. */
export function friendlyStep(label: string): string {
  const l = (label || '').toLowerCase();
  if (l.includes('memory')) return 'Recalling context';
  if (l.includes('genie')) return 'Querying your data';
  // SQL/warehouse BEFORE the file/read branch — "..._read_only" contains "read".
  if (l.includes('sql') || l.includes('warehouse')) return 'Running a query';
  if (isWebSearch(l)) return 'Searching the web';
  if (l.includes('agentbricks') || l.includes('agent_bricks') || l.includes('agent bricks')) return 'Consulting an agent';
  if (l.includes('scrape') || l.includes('crawl') || l.includes('website') || l.includes('content') || l.includes('url')) return 'Reading sources';
  if (l.includes('file') || l.includes('read')) return 'Reading files';
  if (l.includes('query')) return 'Running a query';
  // Any other search/lookup (catalog, data products, knowledge, …) — a neutral
  // verb that doesn't claim a source (the web) it never touched.
  if (l.includes('search') || l.includes('lookup') || l.includes('find') || l.includes('retriev')) return 'Searching';
  return label || 'Working';
}
