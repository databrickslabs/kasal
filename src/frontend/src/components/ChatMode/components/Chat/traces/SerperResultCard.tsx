import React from 'react';
import Box from '@mui/material/Box';
import MessageContent from '../MessageContent';

/** Whether this trace is a Serper ("Search the internet") JSON result. */
export function matchesSerper(detail: string, label?: string): boolean {
  if (/serper|search the internet/i.test(label || '')) return true;
  return (
    typeof detail === 'string' &&
    detail.includes('"searchParameters"') &&
    detail.includes('"organic"')
  );
}

interface SerperOrganic {
  title?: string;
  link?: string;
  snippet?: string;
}

/**
 * Turn the Serper Google-search JSON into a readable Markdown answer: an
 * optional answer box, the organic results as a linked list, and "people also
 * ask". Returns null if `detail` isn't parseable Serper JSON.
 */
function formatSerper(detail: string): string | null {
  let data: Record<string, unknown>;
  try {
    data = JSON.parse(detail) as Record<string, unknown>;
  } catch {
    return null;
  }
  if (!data || typeof data !== 'object') return null;

  const lines: string[] = [];

  const answerBox = data.answerBox as Record<string, unknown> | undefined;
  if (answerBox) {
    const ans = (answerBox.answer || answerBox.snippet || answerBox.title) as string | undefined;
    if (ans) lines.push(`**${ans}**`, '');
  }

  const organic = (Array.isArray(data.organic) ? data.organic : []) as SerperOrganic[];
  organic.slice(0, 8).forEach((r, i) => {
    const title = (r.title || r.link || `Result ${i + 1}`).trim();
    const head = r.link ? `[${title}](${r.link})` : title;
    const snippet = r.snippet ? ` — ${r.snippet.trim()}` : '';
    lines.push(`${i + 1}. ${head}${snippet}`);
  });

  const paa = (Array.isArray(data.peopleAlsoAsk) ? data.peopleAlsoAsk : []) as Record<string, unknown>[];
  if (paa.length) {
    lines.push('', '**People also ask**');
    paa.slice(0, 4).forEach((q) => {
      const question = q.question as string | undefined;
      if (question) {
        const snip = q.snippet ? ` — ${String(q.snippet).trim()}` : '';
        lines.push(`- ${question}${snip}`);
      }
    });
  }

  const out = lines.join('\n').trim();
  return out || null;
}

/**
 * Serper result shown INSIDE the collapsible trace pill: the search results
 * rendered as Markdown (linked titles + snippets) in an indented, scrollable
 * panel — readable but tucked away, never raw JSON. Returns null when the
 * content isn't Serper JSON (caller falls back).
 */
export const SerperResultCard: React.FC<{
  detail: string;
  label?: string;
  sublabel?: string;
  durationMs?: number;
  indent?: number;
}> = ({ detail, indent = 1.5 }) => {
  const md = formatSerper(detail);
  if (!md) return null;
  return (
    <Box
      sx={{
        mt: 0.5,
        ml: indent,
        maxWidth: '85%',
        borderRadius: '4px',
        p: 1.5,
        maxHeight: 384,
        overflowY: 'auto',
        fontSize: 14,
        lineHeight: 1.625,
        color: 'text.primary',
        backgroundColor: 'background.default',
        border: 1,
        borderColor: 'divider',
      }}
    >
      <MessageContent content={md} />
    </Box>
  );
};
