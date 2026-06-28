import React, { useState } from 'react';
import Box from '@mui/material/Box';
import { useTheme, type Theme } from '@mui/material/styles';
import type { SystemStyleObject } from '@mui/system';
import MessageContent from '../MessageContent';
import { ChartView, ChartPoint } from '../../Preview/ChartView';
import { buttonResetSx } from '../../../chatSx';

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

/** Shared styling for an authored code block. Rendered as a <Box> (div), not a
 *  <pre>, so the chat's global `pre` rule (a dark code-block style) doesn't
 *  override the lighter token background used here. */
const codeBlockSx: SystemStyleObject<Theme> = {
  fontSize: 11,
  fontFamily: 'monospace',
  whiteSpace: 'pre',
  overflowX: 'auto',
  borderRadius: '4px',
  p: 1,
  my: 1.5,
  lineHeight: 1.6,
  color: 'text.primary',
  backgroundColor: (t) => t.chat.bgSecondary,
  border: 1,
  borderColor: 'divider',
};

/** A styled HTML table for the query result rows. */
const ResultTable: React.FC<{ columns: string[]; rows: string[][] }> = ({ columns, rows }) => (
  <Box sx={{ overflow: 'auto', borderRadius: '4px', maxHeight: 256, border: 1, borderColor: 'divider' }}>
    <Box component="table" sx={{ fontSize: 11, borderCollapse: 'collapse', width: '100%' }}>
      <thead>
        <tr>
          {columns.map((c, i) => (
            <Box
              component="th"
              key={i}
              sx={{
                textAlign: 'left',
                px: 1,
                py: 0.5,
                fontWeight: 600,
                whiteSpace: 'nowrap',
                color: 'text.disabled',
                backgroundColor: (t) => t.chat.bgSecondary,
                borderBottom: 1,
                borderColor: 'divider',
                position: 'sticky',
                top: 0,
              }}
            >
              {c}
            </Box>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.map((r, ri) => (
          <tr key={ri}>
            {columns.map((_, ci) => (
              <Box
                component="td"
                key={ci}
                sx={{
                  px: 1,
                  py: 0.5,
                  whiteSpace: 'nowrap',
                  color: 'text.primary',
                  borderTop: ri ? 1 : 0,
                  borderColor: 'divider',
                }}
              >
                {r[ci] ?? ''}
              </Box>
            ))}
          </tr>
        ))}
      </tbody>
    </Box>
  </Box>
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
  const theme = useTheme();
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
      colors={{ text: theme.palette.text.primary, muted: theme.palette.text.disabled }}
    />
  );
};

/** Render one Genie section by kind: SQL as a code block, RESULTS as a real
 *  table + bar chart, the link as a hyperlink, follow-ups as a list, everything
 *  else as labeled text. */
const GenieSectionView: React.FC<{ section: GenieSection }> = ({ section }) => {
  const { label, body } = section;
  const labelEl = (
    <Box sx={{ fontSize: 10, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.05em', color: 'text.disabled' }}>
      {label}
    </Box>
  );

  if (label === 'SQL Query') {
    return (
      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5 }}>
        {labelEl}
        <Box sx={codeBlockSx}>{body}</Box>
      </Box>
    );
  }

  if (label === 'Query Results') {
    const table = parseMarkdownTable(body);
    return (
      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.75 }}>
        {labelEl}
        {table ? (
          <>
            <ResultTable columns={table.columns} rows={table.rows} />
            <ResultChart columns={table.columns} rows={table.rows} />
          </>
        ) : (
          // Fallback: render whatever text we got (e.g. a non-tabular result).
          <Box sx={codeBlockSx}>{body}</Box>
        )}
      </Box>
    );
  }

  if (label === 'Open in Genie') {
    const url = body.trim();
    return (
      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5 }}>
        {labelEl}
        <Box
          component="a"
          href={url}
          target="_blank"
          rel="noopener noreferrer"
          sx={{ fontSize: 11, textDecoration: 'underline', wordBreak: 'break-all', color: 'primary.main' }}
        >
          {url}
        </Box>
      </Box>
    );
  }

  if (label === 'Suggested follow-up questions') {
    const items = body
      .split('\n')
      .map((l) => l.replace(/^-\s*/, '').trim())
      .filter(Boolean);
    return (
      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5 }}>
        {labelEl}
        <Box component="ul" sx={{ fontSize: 11, listStyleType: 'disc', ml: 2, color: 'text.primary' }}>
          {items.map((it, i) => (
            <li key={i}>{it}</li>
          ))}
        </Box>
      </Box>
    );
  }

  // Question / What the query does / Answer
  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.5 }}>
      {labelEl}
      <Box sx={{ fontSize: 11, whiteSpace: 'pre-wrap', overflowWrap: 'break-word', color: 'text.primary' }}>
        {body}
      </Box>
    </Box>
  );
};

/** Whether this detail string is GenieTool output this renderer can handle. */
export function matchesGenieDetail(detail: string): boolean {
  return parseGenieDetail(detail) !== null;
}

/** Expanded Genie trace detail: the labeled sections (Question / SQL / Results
 *  table + chart / follow-ups / link). `indent` = left indent in MUI spacing
 *  units (8px each): 1.5 = 12px. */
export const GenieTraceDetail: React.FC<{ detail: string; indent?: number }> = ({
  detail,
  indent = 1.5,
}) => {
  const sections = parseGenieDetail(detail);
  if (!sections) return null;
  return (
    <Box
      sx={{
        mt: 0.5,
        ml: indent,
        maxWidth: '85%',
        borderRadius: '4px',
        p: 1,
        maxHeight: 384,
        overflowY: 'auto',
        display: 'flex',
        flexDirection: 'column',
        gap: 1.25,
        backgroundColor: 'background.default',
        border: 1,
        borderColor: 'divider',
      }}
    >
      {sections.map((s, i) => (
        <GenieSectionView key={i} section={s} />
      ))}
    </Box>
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
export const GenieAnswerCard: React.FC<{ detail: string; durationMs?: number }> = ({
  detail,
  durationMs,
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
    <Box sx={{ maxWidth: '85%', display: 'flex', flexDirection: 'column', gap: 1.5 }}>
      {/* Header: a small Genie label + (optional) latency. */}
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
        <Box
          component="svg"
          sx={{ width: 14, height: 14, flexShrink: 0, color: 'primary.main' }}
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
          strokeWidth={2}
        >
          <path strokeLinecap="round" strokeLinejoin="round" d="M5 3v4M3 5h4M6 17v4m-2-2h4m5-16l2.286 6.857L21 12l-5.714 2.143L13 21l-2.286-6.857L5 12l5.714-2.143L13 3z" />
        </Box>
        <Box component="span" sx={{ fontSize: 12, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.05em', color: 'text.disabled' }}>
          Genie
        </Box>
        {durationLabel && (
          <Box component="span" sx={{ fontSize: 10, fontFamily: 'monospace', color: 'text.disabled' }}>
            · {durationLabel}
          </Box>
        )}
      </Box>

      {/* The answer — rendered as Markdown, front and center. */}
      {answer && (
        <Box sx={{ fontSize: 14, lineHeight: 1.625, color: 'text.primary' }}>
          <MessageContent content={answer.body} />
        </Box>
      )}

      {/* The data behind the answer: table + chart. */}
      {results && <GenieSectionView section={results} />}

      {/* Suggested follow-ups. */}
      {followups && <GenieSectionView section={followups} />}

      {/* Question + SQL — collapsed by default. */}
      {querySections.length > 0 && (
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
          <Box
            component="button"
            type="button"
            onClick={() => setShowQuery((v) => !v)}
            sx={{
              ...buttonResetSx,
              display: 'flex',
              alignItems: 'center',
              gap: 0.75,
              textAlign: 'left',
              alignSelf: 'flex-start',
              fontSize: 11,
              fontWeight: 500,
              transition: 'color 0.15s',
              color: 'text.disabled',
              '&:hover': { opacity: 0.8 },
            }}
          >
            <Box
              component="svg"
              sx={{ width: 12, height: 12, flexShrink: 0, transition: 'transform 0.15s', transform: showQuery ? 'rotate(90deg)' : 'rotate(0deg)' }}
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
              strokeWidth={2}
            >
              <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
            </Box>
            {showQuery ? 'Hide question & SQL' : 'Show question & SQL'}
          </Box>
          {showQuery && (
            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1.25 }}>
              {querySections.map((s, i) => (
                <GenieSectionView key={i} section={s} />
              ))}
            </Box>
          )}
        </Box>
      )}

      {/* Open in Genie link. */}
      {link && <GenieSectionView section={link} />}
    </Box>
  );
};
