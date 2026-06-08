import React, { useState } from 'react';
import MessageContent from '../MessageContent';
import { ChartView, ChartPoint } from '../../Preview/UiRenderer';

/**
 * Genie-specific trace-detail renderer. Turns the GenieTool's labeled text
 * output (see backend genie_tool.py) into visual sections: Question / SQL /
 * Answer / a real results table + bar chart / follow-ups / link.
 *
 * Self-contained and registered in ./registry — ChatMessage never imports the
 * internals, so adding/changing a tool's rendering stays isolated here.
 */

// Section headers the GenieTool emits. When a trace's detail contains these,
// we render each part in its own labeled block instead of one raw <pre>.
const GENIE_DETAIL_HEADERS = [
  'Question',
  'What the query does',
  'SQL Query',
  'Answer',
  'Query Results',
  'Suggested follow-up questions',
  'Open in Genie',
];

interface GenieSection {
  label: string;
  body: string;
}

/** Split a GenieTool detail string into its labeled sections, or null if it
 *  isn't a Genie output (so the caller falls back to a plain <pre>). */
function parseGenieDetail(detail: string): GenieSection[] | null {
  if (!detail || (!detail.includes('SQL Query:') && !detail.includes('Query Results:'))) {
    return null;
  }
  const headerSet = new Set(GENIE_DETAIL_HEADERS.map((h) => `${h}:`));
  const sections: { label: string; lines: string[] }[] = [];
  for (const line of detail.split('\n')) {
    if (headerSet.has(line.trim())) {
      sections.push({ label: line.trim().replace(/:$/, ''), lines: [] });
    } else if (sections.length) {
      sections[sections.length - 1].lines.push(line);
    }
  }
  const out = sections
    .map((s) => ({ label: s.label, body: s.lines.join('\n').replace(/^\n+|\n+$/g, '') }))
    .filter((s) => s.body.length > 0);
  return out.length ? out : null;
}

/** Parse a Markdown table ("| a | b |\n| --- | --- |\n| 1 | 2 |") into
 *  columns + rows, or null if `body` isn't a table. */
function parseMarkdownTable(body: string): { columns: string[]; rows: string[][] } | null {
  const lines = body.split('\n').map((l) => l.trim()).filter((l) => l.startsWith('|'));
  if (lines.length < 2) return null;
  const split = (l: string) =>
    l.replace(/^\|/, '').replace(/\|$/, '').split('|').map((c) => c.replace(/\\\|/g, '|').trim());
  const columns = split(lines[0]);
  const isSep = lines[1].includes('-') && /^[\s|:-]+$/.test(lines[1]);
  const rows = (isSep ? lines.slice(2) : lines.slice(1)).map(split);
  return { columns, rows };
}

/** Parse a numeric value out of a cell ("$1,234", "19.8%", "1234"). */
function cellToNumber(s: string | undefined): number | null {
  if (s == null) return null;
  const cleaned = s.replace(/[$,%\s]/g, '');
  if (cleaned === '') return null;
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

/** A styled HTML table for the query result rows. */
const ResultTable: React.FC<{ columns: string[]; rows: string[][] }> = ({ columns, rows }) => (
  <div className="overflow-auto rounded max-h-64" style={{ border: '1px solid var(--border-color)' }}>
    <table className="text-[11px]" style={{ borderCollapse: 'collapse', width: '100%' }}>
      <thead>
        <tr>
          {columns.map((c, i) => (
            <th
              key={i}
              className="text-left px-2 py-1 font-semibold whitespace-nowrap"
              style={{
                color: 'var(--text-muted)',
                backgroundColor: 'var(--bg-secondary)',
                borderBottom: '1px solid var(--border-color)',
                position: 'sticky',
                top: 0,
              }}
            >
              {c}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.map((r, ri) => (
          <tr key={ri}>
            {columns.map((_, ci) => (
              <td
                key={ci}
                className="px-2 py-1 whitespace-nowrap"
                style={{ color: 'var(--text-primary)', borderTop: ri ? '1px solid var(--border-color)' : 'none' }}
              >
                {r[ci] ?? ''}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  </div>
);

/** Labels that read as a time axis → render a line chart instead of bars. */
function isTemporalLabel(s: string): boolean {
  const t = (s || '').trim();
  return (
    /^\d{4}$/.test(t) || // year
    /^\d{4}[-/]\d{1,2}/.test(t) || // 2024-03, 2024/3
    /^\d{1,2}[-/]\d{1,2}[-/]\d{2,4}$/.test(t) || // 03/2024
    /^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|q[1-4])/i.test(t) // month / quarter
  );
}

/**
 * Advanced visualization of the result rows: an SVG bar / line chart (reusing
 * the design-system ChartView) picked from the shape of the data — a line when
 * the label column reads as a time axis, otherwise the top values as bars.
 * Renders nothing if the data isn't chartable.
 */
const ResultChart: React.FC<{ columns: string[]; rows: string[][] }> = ({ columns, rows }) => {
  if (rows.length < 2 || columns.length < 2) return null;
  const numericThreshold = Math.max(1, Math.ceil(rows.length * 0.6));
  const colNumeric = columns.map(
    (_, ci) => rows.filter((r) => cellToNumber(r[ci]) !== null).length >= numericThreshold,
  );
  const metricIdx = colNumeric.findIndex(Boolean);
  const labelIdx = colNumeric.findIndex((x) => !x);
  if (metricIdx < 0 || labelIdx < 0) return null;

  let points: ChartPoint[] = rows.map((r) => ({
    label: r[labelIdx] ?? '',
    value: cellToNumber(r[metricIdx]) ?? 0,
  }));
  if (points.every((p) => p.value === 0)) return null;

  // Time-like x axis → line (chronological); otherwise rank by value → bars.
  const temporal =
    points.filter((p) => isTemporalLabel(p.label)).length >= Math.ceil(points.length * 0.6);
  const chartType = temporal ? 'line' : 'bar';
  points = temporal
    ? points.slice(0, 30)
    : [...points].sort((a, b) => b.value - a.value).slice(0, 12);

  return (
    <ChartView
      chartType={chartType}
      data={points}
      title={`${columns[metricIdx]} by ${columns[labelIdx]}`}
      // Genie renders on the (light) chat background, not the dark preview
      // stage — use theme tokens so the title + labels stay readable.
      colors={{ text: 'var(--text-primary)', muted: 'var(--text-muted)' }}
    />
  );
};

/** Render one Genie section by kind: SQL as a code block, RESULTS as a real
 *  table + bar chart, the link as a hyperlink, follow-ups as a list, everything
 *  else as labeled text. */
const GenieSectionView: React.FC<{ section: GenieSection }> = ({ section }) => {
  const { label, body } = section;
  const labelEl = (
    <div
      className="text-[10px] font-semibold uppercase tracking-wider"
      style={{ color: 'var(--text-muted)' }}
    >
      {label}
    </div>
  );

  if (label === 'SQL Query') {
    return (
      <div className="flex flex-col gap-1">
        {labelEl}
        <pre
          className="text-[11px] font-mono whitespace-pre rounded p-2 overflow-x-auto"
          style={{
            color: 'var(--text-primary)',
            backgroundColor: 'var(--bg-secondary)',
            border: '1px solid var(--border-color)',
          }}
        >
          {body}
        </pre>
      </div>
    );
  }

  if (label === 'Query Results') {
    const table = parseMarkdownTable(body);
    return (
      <div className="flex flex-col gap-1.5">
        {labelEl}
        {table ? (
          <>
            <ResultTable columns={table.columns} rows={table.rows} />
            <ResultChart columns={table.columns} rows={table.rows} />
          </>
        ) : (
          // Fallback: render whatever text we got (e.g. a non-tabular result).
          <pre
            className="text-[11px] font-mono whitespace-pre rounded p-2 overflow-x-auto"
            style={{
              color: 'var(--text-primary)',
              backgroundColor: 'var(--bg-secondary)',
              border: '1px solid var(--border-color)',
            }}
          >
            {body}
          </pre>
        )}
      </div>
    );
  }

  if (label === 'Open in Genie') {
    const url = body.trim();
    return (
      <div className="flex flex-col gap-1">
        {labelEl}
        <a
          href={url}
          target="_blank"
          rel="noopener noreferrer"
          className="text-[11px] underline break-all"
          style={{ color: 'var(--accent)' }}
        >
          {url}
        </a>
      </div>
    );
  }

  if (label === 'Suggested follow-up questions') {
    const items = body
      .split('\n')
      .map((l) => l.replace(/^-\s*/, '').trim())
      .filter(Boolean);
    return (
      <div className="flex flex-col gap-1">
        {labelEl}
        <ul className="text-[11px] list-disc ml-4" style={{ color: 'var(--text-primary)' }}>
          {items.map((it, i) => (
            <li key={i}>{it}</li>
          ))}
        </ul>
      </div>
    );
  }

  // Question / What the query does / Answer
  return (
    <div className="flex flex-col gap-1">
      {labelEl}
      <div
        className="text-[11px] whitespace-pre-wrap break-words"
        style={{ color: 'var(--text-primary)' }}
      >
        {body}
      </div>
    </div>
  );
};

/** Whether this detail string is GenieTool output this renderer can handle. */
export function matchesGenieDetail(detail: string): boolean {
  return parseGenieDetail(detail) !== null;
}

/** Expanded Genie trace detail: the labeled sections (Question / SQL / Results
 *  table + chart / follow-ups / link). */
export const GenieTraceDetail: React.FC<{ detail: string; indentClass?: string }> = ({
  detail,
  indentClass = 'ml-3',
}) => {
  const sections = parseGenieDetail(detail);
  if (!sections) return null;
  return (
    <div
      className={`mt-1 ${indentClass} max-w-[85%] rounded p-2 max-h-96 overflow-y-auto flex flex-col gap-2.5`}
      style={{ backgroundColor: 'var(--bg-primary)', border: '1px solid var(--border-color)' }}
    >
      {sections.map((s, i) => (
        <GenieSectionView key={i} section={s} />
      ))}
    </div>
  );
};

// Sections that describe HOW the answer was produced. These are collapsed by
// default in the chat card — the user wants the answer, not the plumbing.
const GENIE_QUERY_DETAIL_LABELS = new Set(['Question', 'What the query does', 'SQL Query']);

/** Format a duration in ms as "2.93s" / "640ms" (null when absent). */
function formatGenieDuration(durationMs?: number): string | null {
  if (typeof durationMs !== 'number') return null;
  return durationMs >= 1000 ? `${(durationMs / 1000).toFixed(2)}s` : `${Math.round(durationMs)}ms`;
}

/**
 * Inline, chat-native Genie answer card (Perplexity-style). The ANSWER renders
 * directly in the conversation as Markdown; the data table / chart, follow-ups
 * and the Genie link stay visible; the Question + "what the query does" + SQL
 * collapse behind a "Show query & SQL" toggle so the plumbing is one click away
 * but never in the way. Returns null for non-Genie output (caller falls back).
 */
export const GenieAnswerCard: React.FC<{ detail: string; durationMs?: number; indentClass?: string }> = ({
  detail,
  durationMs,
  indentClass = 'ml-3',
}) => {
  const [showQuery, setShowQuery] = useState(false);
  const sections = parseGenieDetail(detail);
  if (!sections) return null;

  const answer = sections.find((s) => s.label === 'Answer');
  const results = sections.find((s) => s.label === 'Query Results');
  const followups = sections.find((s) => s.label === 'Suggested follow-up questions');
  const link = sections.find((s) => s.label === 'Open in Genie');
  const querySections = sections.filter((s) => GENIE_QUERY_DETAIL_LABELS.has(s.label));
  const durationLabel = formatGenieDuration(durationMs);

  return (
    // No card wrapper, no indent — the answer + diagrams flow directly in the
    // chat like a normal message (the table keeps its own border, the chart its
    // own bars). Only the question + SQL hide behind a toggle.
    <div className="max-w-[85%] flex flex-col gap-3">
      {/* Header: a small Genie label + (optional) latency. */}
      <div className="flex items-center gap-2">
        <svg
          className="w-3.5 h-3.5 flex-shrink-0"
          style={{ color: 'var(--accent)' }}
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
          strokeWidth={2}
        >
          <path strokeLinecap="round" strokeLinejoin="round" d="M5 3v4M3 5h4M6 17v4m-2-2h4m5-16l2.286 6.857L21 12l-5.714 2.143L13 21l-2.286-6.857L5 12l5.714-2.143L13 3z" />
        </svg>
        <span className="text-xs font-semibold uppercase tracking-wider" style={{ color: 'var(--text-muted)' }}>
          Genie
        </span>
        {durationLabel && (
          <span className="text-[10px] font-mono" style={{ color: 'var(--text-muted)' }}>
            · {durationLabel}
          </span>
        )}
      </div>

      {/* The answer — rendered as Markdown, front and center. */}
      {answer && (
        <div className="text-sm leading-relaxed" style={{ color: 'var(--text-primary)' }}>
          <MessageContent content={answer.body} />
        </div>
      )}

      {/* The data behind the answer: table + chart. */}
      {results && <GenieSectionView section={results} />}

      {/* Suggested follow-ups. */}
      {followups && <GenieSectionView section={followups} />}

      {/* Question + SQL — collapsed by default. */}
      {querySections.length > 0 && (
        <div className="flex flex-col gap-2">
          <button
            type="button"
            onClick={() => setShowQuery((v) => !v)}
            className="flex items-center gap-1.5 text-left self-start text-[11px] font-medium transition-colors hover:opacity-80"
            style={{ color: 'var(--text-muted)' }}
          >
            <svg
              className="w-3 h-3 flex-shrink-0 transition-transform"
              style={{ transform: showQuery ? 'rotate(90deg)' : 'rotate(0deg)' }}
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
              strokeWidth={2}
            >
              <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
            </svg>
            {showQuery ? 'Hide question & SQL' : 'Show question & SQL'}
          </button>
          {showQuery && (
            <div className="flex flex-col gap-2.5">
              {querySections.map((s, i) => (
                <GenieSectionView key={i} section={s} />
              ))}
            </div>
          )}
        </div>
      )}

      {/* Open in Genie link. */}
      {link && <GenieSectionView section={link} />}
    </div>
  );
};
