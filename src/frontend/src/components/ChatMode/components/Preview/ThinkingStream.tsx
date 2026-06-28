import React, { useEffect, useRef } from 'react';
import Box from '@mui/material/Box';
import { friendlyStep, type RunStep } from './RunTimeline';
import { contextSummary, humanizeToolJson } from './runActivitySurface';
import { buttonResetSx, pulseSx } from '../../chatSx';

/** Kasal brand mark — the hexagon-K logo (transparent), shown in the live "getting started" state. */
const kasalMark = `${import.meta.env.BASE_URL}logo192.png`;

/**
 * First-person, present-tense narration of a step — the "thinking" voice a
 * non-technical user reads (no tool names, no args, no JSON). The cleaned
 * `sublabel` (a search query, …) is woven in when it reads naturally.
 */
export function narrate(step: RunStep): string {
  const l = (step.label || '').toLowerCase();
  const sub = (step.sublabel || '').trim();
  if (l.includes('memory')) return 'I’m recalling relevant background I already know, so the answer stays grounded in what we’ve established.';
  if (l.includes('genie') || l.includes('sql') || l.includes('warehouse') || l.includes('query')) {
    return sub ? `I’m looking up your data for ${sub}, then I’ll read through the results.` : 'I’m looking up your data, then I’ll read through the results.';
  }
  if (l.includes('perplex') || l.includes('serper') || l.includes('search') || l.includes('tavily')) {
    return sub ? `I’m searching the web for “${sub}” to gather the most relevant, up-to-date information.` : 'I’m searching the web to gather the most relevant, up-to-date information.';
  }
  if (l.includes('scrape') || l.includes('crawl') || l.includes('website') || l.includes('content') || l.includes('url')) {
    return 'I’m reading through the most relevant sources and pulling out the key details.';
  }
  if (l.includes('agentbricks') || l.includes('agent_bricks') || l.includes('agent bricks')) {
    return 'I’m consulting a specialist agent for an expert take on this.';
  }
  if (l.includes('file') || l.includes('read')) return 'I’m reading the documents I need for this.';
  return sub ? `I’m working on ${sub}.` : 'I’m working through this step.';
}

export interface Source {
  domain: string;
  title?: string;
}

/** Best-effort hostname (no protocol / www) for a chip label. */
function hostnameOf(url: string): string {
  try {
    return new URL(url).hostname.replace(/^www\./, '');
  } catch {
    return url.replace(/^https?:\/\//, '').replace(/^www\./, '').split(/[/?#]/)[0];
  }
}

/**
 * Pull the web sources a step touched out of its content — paired with a nearby
 * "Title:" when present — so a search/read step can show a tidy grid of source
 * chips (favicon-style initial + domain + title) instead of a raw URL dump.
 */
export function extractSources(detail?: string): Source[] {
  if (!detail) return [];
  const text = humanizeToolJson(detail) ?? detail;
  const sources: Source[] = [];
  const seen = new Set<string>();
  let lastTitle: string | undefined;
  for (const rawLine of text.split('\n')) {
    const line = rawLine.trim();
    const tm = line.match(/^title:\s*(.+)$/i);
    if (tm) {
      lastTitle = tm[1].trim();
      continue;
    }
    const um = line.match(/(https?:\/\/[^\s"'<>)\]]+)/i);
    if (um) {
      const domain = hostnameOf(um[1]);
      const key = `${domain}::${lastTitle ?? ''}`;
      if (!seen.has(key) && domain) {
        seen.add(key);
        sources.push({ domain, title: lastTitle });
      }
      lastTitle = undefined;
      if (sources.length >= 12) break;
    }
  }
  return sources;
}

/** A short, readable excerpt of what a step gathered (for content with no sources). */
function excerptOf(detail?: string): string {
  if (!detail) return '';
  return contextSummary(detail, 260);
}

/** A four-point sparkle marker, à la a "thinking" stream. */
const Sparkle: React.FC = () => (
  <Box component="svg" viewBox="0 0 24 24" fill="currentColor" sx={{ width: 14, height: 14 }} aria-hidden="true">
    <path d="M12 2l1.7 6.1c.2.7.5 1 1.2 1.2L21 11l-6.1 1.7c-.7.2-1 .5-1.2 1.2L12 20l-1.7-6.1c-.2-.7-.5-1-1.2-1.2L3 11l6.1-1.7c.7-.2 1-.5 1.2-1.2z" />
  </Box>
);

const SourceGrid: React.FC<{ sources: Source[] }> = ({ sources }) => (
  <Box
    data-testid="thinking-sources"
    sx={{
      display: 'grid',
      gridTemplateColumns: '1fr',
      gap: 1,
      mt: 1.5,
      '@media (min-width:640px)': { gridTemplateColumns: 'repeat(2, minmax(0, 1fr))' },
    }}
  >
    {sources.map((s, i) => (
      <Box
        key={`${s.domain}-${i}`}
        sx={{
          display: 'flex',
          alignItems: 'center',
          gap: 1,
          px: 1.25,
          py: 0.75,
          borderRadius: '8px',
          overflow: 'hidden',
          border: 1,
          borderColor: 'divider',
          backgroundColor: (t) => t.chat.bgSecondary,
        }}
        title={s.title ? `${s.title} — ${s.domain}` : s.domain}
      >
        <Box
          component="span"
          aria-hidden="true"
          sx={{
            flexShrink: 0,
            width: 16,
            height: 16,
            borderRadius: '9999px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            fontSize: 9,
            fontWeight: 700,
            backgroundColor: 'primary.main',
            color: '#fff',
          }}
        >
          {s.domain.charAt(0).toUpperCase()}
        </Box>
        <Box component="span" sx={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', fontSize: 12, color: 'text.secondary' }}>
          {s.title ? <span>{s.title} </span> : null}
          <Box component="span" sx={{ color: 'text.disabled' }}>{s.domain}</Box>
        </Box>
      </Box>
    ))}
  </Box>
);

const StepItem: React.FC<{ step: RunStep; isLast: boolean; onSelect?: (step: RunStep) => void }> = ({ step, isLast, onSelect }) => {
  const sources = extractSources(step.detail);
  const excerpt = sources.length === 0 ? excerptOf(step.detail) : '';
  // Context worth opening on its own page = detail beyond the short summary line.
  const clickable = Boolean(step.detail && step.detail !== step.sublabel && onSelect);
  const inner = (
    <>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
        <Box sx={{ fontSize: 15, fontWeight: 600, fontStyle: 'italic', flex: 1, minWidth: 0, color: 'text.primary' }}>
          {friendlyStep(step.label)}
        </Box>
        {clickable && (
          <Box
            component="svg"
            className="ts-chevron"
            sx={{ width: 16, height: 16, flexShrink: 0, transition: 'transform 0.15s', color: 'text.disabled' }}
            fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}
            aria-hidden="true"
          >
            <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
          </Box>
        )}
      </Box>
      {/* The actual tool behind this step, so it's traceable (the heading is a
          friendly phase name; this is the raw tool that ran). */}
      {step.label && friendlyStep(step.label) !== step.label && (
        <Box sx={{ fontSize: 11, mt: 0.25, fontFamily: 'monospace', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: 'text.disabled', opacity: 0.75 }} title={step.label} data-testid="thinking-tool">
          {step.label}
        </Box>
      )}
      <Box sx={{ fontSize: 14, fontStyle: 'italic', mt: 0.5, lineHeight: 1.625, color: 'text.secondary' }} data-testid="thinking-narrative">
        {narrate(step)}
      </Box>
      {sources.length > 0 && <SourceGrid sources={sources} />}
      {sources.length === 0 && excerpt && (
        <Box component="p" sx={{ fontSize: 13, mt: 1, lineHeight: 1.625, color: 'text.disabled' }}>
          {excerpt}
        </Box>
      )}
    </>
  );
  return (
    <Box component="li" sx={{ position: 'relative', pl: 4, pb: 3, '&:last-child': { pb: 1 } }}>
      {/* Left rail connecting the phases. */}
      {!isLast && <Box component="span" aria-hidden="true" sx={{ position: 'absolute', left: '6px', top: 20, bottom: 0, width: '1px', backgroundColor: 'divider' }} />}
      <Box component="span" sx={{ position: 'absolute', left: 0, top: '2px', color: 'primary.main' }}>
        <Sparkle />
      </Box>
      {clickable ? (
        <Box
          component="button"
          type="button"
          onClick={() => onSelect?.(step)}
          aria-label={`Open the full context for ${friendlyStep(step.label)}`}
          sx={{
            ...buttonResetSx,
            width: '100%',
            textAlign: 'left',
            borderRadius: '8px',
            px: 1,
            py: 0.75,
            mx: -1,
            transition: 'opacity 0.15s',
            '&:hover': { opacity: 0.8, '& .ts-chevron': { transform: 'translateX(2px)' } },
          }}
        >
          {inner}
        </Box>
      ) : (
        <Box>{inner}</Box>
      )}
    </Box>
  );
};

interface ThinkingStreamProps {
  /** The run's steps, oldest → newest (sourced from the persistent trace). */
  steps: RunStep[];
  /** Live run: show the "getting started" state and a trailing "Thinking…" pulse. */
  live?: boolean;
  /** Click a step that has context → open its full elegant context on its own page. */
  onSelect?: (step: RunStep) => void;
}

/**
 * The run monitor rendered as a flowing "thinking" stream (à la Gemini's "Show
 * thinking"): each step is a bold-italic phase heading + a first-person narrative
 * of what the agent is doing, on a left rail with sparkle markers. Web searches
 * surface a grid of source chips; other steps show a short readable excerpt of
 * what they found. This is the SINGLE activity format — used both while the run is
 * live ({@link PreviewSkeleton}, `live` = auto-scroll + "Thinking…" pulse) and
 * after it finishes ({@link PreviewPanel}, with `onSelect` so each step opens its
 * full elegant context on its own page).
 */
const ThinkingStream: React.FC<ThinkingStreamProps> = ({ steps, live = false, onSelect }) => {
  const scrollRef = useRef<HTMLDivElement>(null);
  const prevCount = useRef(0);
  // Keep the newest thinking in view as steps arrive ("switch to the next").
  useEffect(() => {
    if (steps.length > prevCount.current && scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
    prevCount.current = steps.length;
  }, [steps.length]);

  if (steps.length === 0) {
    if (!live) return null;
    return (
      <Box sx={{ height: '100%', display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', textAlign: 'center', px: 4 }} data-testid="thinking-empty">
        <Box component="img" src={kasalMark} alt="Kasal" sx={{ width: 48, height: 48, mb: 2, opacity: 0.95, ...pulseSx }} />
        <Box sx={{ fontSize: 16, fontWeight: 600, fontStyle: 'italic', mb: 0.5, color: 'text.primary' }}>
          Getting started…
        </Box>
        <Box sx={{ fontSize: 13, fontStyle: 'italic', color: 'text.disabled' }}>
          I’m setting up and gathering what I need to answer you.
        </Box>
      </Box>
    );
  }

  return (
    <Box ref={scrollRef} sx={{ height: '100%', overflowY: 'auto', pr: 0.5 }} data-testid="thinking-stream">
      <Box component="ol" sx={{ listStyle: 'none' }}>
        {steps.map((step, i) => (
          <StepItem key={step.id} step={step} isLast={i === steps.length - 1 && !live} onSelect={onSelect} />
        ))}
      </Box>
      {/* The live "still thinking" pulse at the tail of the stream (run in flight). */}
      {live && (
        <Box sx={{ position: 'relative', pl: 4, pb: 1 }}>
          <Box component="span" sx={{ position: 'absolute', left: 0, top: '4px', width: 12, height: 12, borderRadius: '9999px', backgroundColor: 'primary.main', ...pulseSx }} />
          <Box sx={{ fontSize: 14, fontStyle: 'italic', color: 'text.disabled' }}>Thinking…</Box>
        </Box>
      )}
    </Box>
  );
};

export default ThinkingStream;
