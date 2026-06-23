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

/** Map a raw tool/activity label to a friendly, business-readable phase verb. */
export function friendlyStep(label: string): string {
  const l = (label || '').toLowerCase();
  if (l.includes('memory')) return 'Recalling context';
  if (l.includes('genie')) return 'Querying your data';
  // SQL/warehouse BEFORE the file/read branch — "..._read_only" contains "read".
  if (l.includes('sql') || l.includes('warehouse')) return 'Running a query';
  if (l.includes('perplex') || l.includes('serper') || l.includes('search') || l.includes('tavily')) return 'Searching the web';
  if (l.includes('agentbricks') || l.includes('agent_bricks') || l.includes('agent bricks')) return 'Consulting an agent';
  if (l.includes('scrape') || l.includes('crawl') || l.includes('website') || l.includes('content') || l.includes('url')) return 'Reading sources';
  if (l.includes('file') || l.includes('read')) return 'Reading files';
  if (l.includes('query')) return 'Running a query';
  return label || 'Working';
}
