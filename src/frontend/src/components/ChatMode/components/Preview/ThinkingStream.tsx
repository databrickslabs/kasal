import React, { useEffect, useRef } from 'react';
import { friendlyStep, isWebSearch, type RunStep } from './RunTimeline';
import { contextSummary, humanizeToolJson } from './runActivitySurface';

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
  if (isWebSearch(l)) {
    return sub ? `I’m searching the web for “${sub}” to gather the most relevant, up-to-date information.` : 'I’m searching the web to gather the most relevant, up-to-date information.';
  }
  if (l.includes('scrape') || l.includes('crawl') || l.includes('website') || l.includes('content') || l.includes('url')) {
    return 'I’m reading through the most relevant sources and pulling out the key details.';
  }
  if (l.includes('agentbricks') || l.includes('agent_bricks') || l.includes('agent bricks')) {
    return 'I’m consulting a specialist agent for an expert take on this.';
  }
  if (l.includes('file') || l.includes('read')) return 'I’m reading the documents I need for this.';
  // Any other search/lookup (catalog, data products, knowledge, …) — don't claim
  // it's the web; just say we're looking it up.
  if (l.includes('search') || l.includes('lookup') || l.includes('find') || l.includes('retriev')) {
    return sub ? `I’m searching for “${sub}” to find the most relevant information.` : 'I’m searching for the most relevant information.';
  }
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
  <svg viewBox="0 0 24 24" fill="currentColor" className="w-3.5 h-3.5" aria-hidden="true">
    <path d="M12 2l1.7 6.1c.2.7.5 1 1.2 1.2L21 11l-6.1 1.7c-.7.2-1 .5-1.2 1.2L12 20l-1.7-6.1c-.2-.7-.5-1-1.2-1.2L3 11l6.1-1.7c.7-.2 1-.5 1.2-1.2z" />
  </svg>
);

const SourceGrid: React.FC<{ sources: Source[] }> = ({ sources }) => (
  <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 mt-3" data-testid="thinking-sources">
    {sources.map((s, i) => (
      <div
        key={`${s.domain}-${i}`}
        className="flex items-center gap-2 px-2.5 py-1.5 rounded-lg overflow-hidden"
        style={{ border: '1px solid var(--border-color)', backgroundColor: 'var(--bg-secondary)' }}
        title={s.title ? `${s.title} — ${s.domain}` : s.domain}
      >
        <span
          aria-hidden="true"
          className="flex-shrink-0 w-4 h-4 rounded-full flex items-center justify-center text-[9px] font-bold"
          style={{ backgroundColor: 'var(--accent)', color: '#fff' }}
        >
          {s.domain.charAt(0).toUpperCase()}
        </span>
        <span className="truncate text-[12px]" style={{ color: 'var(--text-secondary)' }}>
          {s.title ? <span>{s.title} </span> : null}
          <span style={{ color: 'var(--text-muted)' }}>{s.domain}</span>
        </span>
      </div>
    ))}
  </div>
);

const StepItem: React.FC<{ step: RunStep; isLast: boolean; onSelect?: (step: RunStep) => void }> = ({ step, isLast, onSelect }) => {
  const sources = extractSources(step.detail);
  const excerpt = sources.length === 0 ? excerptOf(step.detail) : '';
  // Context worth opening on its own page = detail beyond the short summary line.
  const clickable = Boolean(step.detail && step.detail !== step.sublabel && onSelect);
  const inner = (
    <>
      <div className="flex items-center gap-2">
        <div className="text-[15px] font-semibold italic flex-1 min-w-0" style={{ color: 'var(--text-primary)' }}>
          {friendlyStep(step.label)}
        </div>
        {clickable && (
          <svg
            className="w-4 h-4 flex-shrink-0 transition-transform group-hover:translate-x-0.5"
            style={{ color: 'var(--text-muted)' }}
            fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}
            aria-hidden="true"
          >
            <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
          </svg>
        )}
      </div>
      {/* The actual tool behind this step, so it's traceable (the heading is a
          friendly phase name; this is the raw tool that ran). */}
      {step.label && friendlyStep(step.label) !== step.label && (
        <div className="text-[11px] mt-0.5 font-mono truncate" style={{ color: 'var(--text-muted)', opacity: 0.75 }} title={step.label} data-testid="thinking-tool">
          {step.label}
        </div>
      )}
      <div className="text-[14px] italic mt-1 leading-relaxed" style={{ color: 'var(--text-secondary)' }} data-testid="thinking-narrative">
        {narrate(step)}
      </div>
      {sources.length > 0 && <SourceGrid sources={sources} />}
      {sources.length === 0 && excerpt && (
        <p className="text-[13px] mt-2 leading-relaxed" style={{ color: 'var(--text-muted)' }}>
          {excerpt}
        </p>
      )}
    </>
  );
  return (
    <li className="relative pl-8 pb-6 last:pb-2">
      {/* Left rail connecting the phases. */}
      {!isLast && <span aria-hidden="true" className="absolute left-[6px] top-5 bottom-0 w-px" style={{ backgroundColor: 'var(--border-color)' }} />}
      <span className="absolute left-0 top-0.5" style={{ color: 'var(--accent)' }}>
        <Sparkle />
      </span>
      {clickable ? (
        <button
          type="button"
          onClick={() => onSelect?.(step)}
          className="group w-full text-left rounded-lg px-2 py-1.5 -mx-2 transition-opacity hover:opacity-80"
          style={{ cursor: 'pointer' }}
          aria-label={`Open the full context for ${friendlyStep(step.label)}`}
        >
          {inner}
        </button>
      ) : (
        <div>{inner}</div>
      )}
    </li>
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
      <div className="h-full flex flex-col items-center justify-center text-center px-8" data-testid="thinking-empty">
        <img src={kasalMark} alt="Kasal" className="w-12 h-12 mb-4 animate-pulse" style={{ opacity: 0.95 }} />
        <div className="text-base font-semibold italic mb-1" style={{ color: 'var(--text-primary)' }}>
          Getting started…
        </div>
        <div className="text-[13px] italic" style={{ color: 'var(--text-muted)' }}>
          I’m setting up and gathering what I need to answer you.
        </div>
      </div>
    );
  }

  return (
    <div ref={scrollRef} className="h-full overflow-y-auto pr-1" data-testid="thinking-stream">
      <ol className="list-none">
        {steps.map((step, i) => (
          <StepItem key={step.id} step={step} isLast={i === steps.length - 1 && !live} onSelect={onSelect} />
        ))}
      </ol>
      {/* The live "still thinking" pulse at the tail of the stream (run in flight). */}
      {live && (
        <div className="relative pl-8 pb-2">
          <span className="absolute left-0 top-1 w-3 h-3 rounded-full animate-pulse" style={{ backgroundColor: 'var(--accent)' }} />
          <div className="text-[14px] italic" style={{ color: 'var(--text-muted)' }}>Thinking…</div>
        </div>
      )}
    </div>
  );
};

export default ThinkingStream;
