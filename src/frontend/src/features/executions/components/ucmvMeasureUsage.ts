/**
 * Pure parsers for the measure comment suffixes emitted by the backend:
 * reference counts, direct report-visual usage and indirect usage via other measures.
 */

/** Extract the "referenced by N measures" count the backend appends to a
 *  measure's comment. Returns null when the measure is referenced by nothing
 *  (backend omits the suffix at 0). Lets reviewers see + sort by usage so they
 *  prioritize high-impact measures/gaps. */
export const extractReferencedBy = (comment?: string): number | null => {
  if (!comment) return null;
  const m = comment.match(/referenced by (\d+) measures?/i);
  return m ? parseInt(m[1], 10) : null;
};

/** One report page a measure is used on: how many visuals on that page (count),
 *  HOW (drawn vs filter) and WHERE (the visual type(s) on that page). */
export interface UsedOnEntry {
  page: string;
  types: string[];
  role: 'drawn' | 'filter' | '';
  /** Number of visuals on this page (the "×N" tally; 1 when not shown). */
  count: number;
}
export interface UsedOnInfo {
  /** HOW OFTEN — total visuals across the report (from the suffix). */
  count: number;
  entries: UsedOnEntry[];
}

/** Parse the measure comment's visual-usage suffix
 *  (yaml_emitter._visual_usage_suffix). This is the signal reviewers use to
 *  judge which measures actually matter — a measure shown on a report page is
 *  business-relevant; one referenced by nothing and drawn nowhere is likely an
 *  intermediate building block.
 *
 *  Current format carries a total count + per-page "×N" tally + type/role, e.g.
 *    "· Used on 7 visuals: OTC Scorecard ×7 (pivotTable/clusteredBarChart·filter)"
 *    "· Used on 3 visuals: OTC Scorecard ×2 (card/tableEx·drawn), OTC NPS (slicer·filter) (+1 more)"
 *  Also tolerates earlier forms: no per-page "×N", and the bare
 *  "· Used on: OTC Scorecard, OTC NPS" (no count/annotation). The suffix is
 *  always last on the comment, so the page list captures to end of string. */
export const extractUsedOn = (comment?: string): UsedOnInfo => {
  if (!comment) return { count: 0, entries: [] };
  const m = comment.match(/Used on(?: (\d+) visuals?)?:\s*(.+)$/);
  if (!m) return { count: 0, entries: [] };
  // The indirect-usage clause ("· Indirectly used via …") can follow the direct
  // one on the same comment — stop before it so its text isn't parsed as pages.
  let list = m[2].split(/\s*·\s*Indirectly used/)[0].trim();
  const more = list.match(/\s*\(\+\d+ more\)\s*$/);
  if (more?.index != null) list = list.slice(0, more.index).trim();
  const entries: UsedOnEntry[] = list
    .split(',')
    .map((tok) => tok.trim())
    .filter(Boolean)
    .map((tok) => {
      // Peel the trailing "(annotation)", then a trailing "×N", leaving the page.
      const paren = tok.match(/\(([^)]*)\)\s*$/);
      const annot = paren ? paren[1] : '';
      let prefix = paren?.index != null ? tok.slice(0, paren.index).trim() : tok.trim();
      const tally = prefix.match(/[×x](\d+)$/);
      const count = tally ? parseInt(tally[1], 10) : 1;
      if (tally?.index != null) prefix = prefix.slice(0, tally.index).trim();
      const [typesPart, rolePart] = annot.split('·');
      const role =
        rolePart === 'filter' || rolePart === 'drawn'
          ? rolePart
          : typesPart === 'drawn' || typesPart === 'filter'
            ? (typesPart as 'drawn' | 'filter')
            : '';
      const types =
        rolePart === undefined
          ? typesPart === 'drawn' || typesPart === 'filter' || typesPart === ''
            ? []
            : typesPart.split('/').filter(Boolean)
          : typesPart.split('/').filter(Boolean);
      return { page: prefix, types, role, count };
    });
  const total = m[1]
    ? parseInt(m[1], 10)
    : entries.reduce((sum, e) => sum + e.count, 0);
  return { count: total, entries };
};

/** One backtraced indirect-usage entry: a visual-placed measure (`via`) whose
 *  DAX reaches this measure, and the report page(s) it's shown on. */
export interface IndirectUsedOnEntry {
  via: string;
  pages: string[];
}

/** Parse the measure comment's indirect-usage clause
 *  (yaml_emitter._indirect_visual_usage_suffix):
 *    "· Indirectly used via [OTC Health Score] on: OTC Scorecard, Exec Summary; [X] on: P (+1 more)"
 *  Returns one entry per `via` measure. Empty when the measure has no indirect
 *  usage (the common case). This measure isn't drawn in a visual itself — a
 *  visual-placed KPI depends on it — so it's shown distinctly from direct use. */
export const extractIndirectUsedOn = (comment?: string): IndirectUsedOnEntry[] => {
  if (!comment) return [];
  const m = comment.match(/Indirectly used via (.+)$/);
  if (!m) return [];
  let rest = m[1].trim();
  const more = rest.match(/\s*\(\+\d+ more\)\s*$/);
  if (more?.index != null) rest = rest.slice(0, more.index).trim();
  return rest
    .split(';')
    .map((tok) => tok.trim())
    .filter(Boolean)
    .map((tok) => {
      const vm = tok.match(/^\[([^\]]+)\](?:\s*on:\s*(.+))?$/);
      if (!vm) return { via: tok.replace(/^\[|\]$/g, ''), pages: [] };
      const pages = vm[2] ? vm[2].split(',').map((p) => p.trim()).filter(Boolean) : [];
      return { via: vm[1].trim(), pages };
    });
};
