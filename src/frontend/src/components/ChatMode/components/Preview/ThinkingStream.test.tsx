import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import ThinkingStream, { narrate, extractSources } from './ThinkingStream';
import type { RunStep } from './RunTimeline';

// useWorkspaceThemes (used by nested LogSurface) hits UIConfigService — but a
// thinking-stream step shows narrative/chips/excerpt, not LogSurface, so no mock
// is needed here.

describe('narrate — first-person, plain-English, no jargon', () => {
  it('describes each phase without tool names or args', () => {
    expect(narrate({ id: '1', label: 'Memory' })).toMatch(/recalling relevant background/i);
    expect(narrate({ id: '2', label: 'GenieTool' })).toMatch(/looking up your data/i);
    expect(narrate({ id: '3', label: 'PerplexityTool', sublabel: 'Switzerland news today' }))
      .toBe('I’m searching the web for “Switzerland news today” to gather the most relevant, up-to-date information.');
    expect(narrate({ id: '4', label: 'ScrapeWebsiteTool' })).toMatch(/reading through the most relevant sources/i);
  });

  it('never leaks the raw cryptic sublabel into a search narrative when absent', () => {
    expect(narrate({ id: '5', label: 'SerperTool' })).not.toMatch(/“”/);
  });
});

describe('extractSources — tidy source chips, not a URL dump', () => {
  it('pairs a URL with its nearby Title and de-dupes', () => {
    const detail = [
      'Title: Iran-US agree 60-day roadmap',
      'Url: https://www.yahoo.com/news/politics/articles/iran-us-agree.html',
      'Title: Bosnia vs Qatar preview',
      'Url: https://www.example-sports.com/match/123',
    ].join('\n');
    const sources = extractSources(detail);
    expect(sources).toHaveLength(2);
    expect(sources[0]).toEqual({ domain: 'yahoo.com', title: 'Iran-US agree 60-day roadmap' });
    expect(sources[1].domain).toBe('example-sports.com');
  });

  it('extracts domains from a bare URL list (no titles)', () => {
    const detail = JSON.stringify(['https://www.swissinfo.ch/eng/x', 'https://blick.ch/y']);
    const domains = extractSources(detail).map((s) => s.domain);
    expect(domains).toContain('swissinfo.ch');
    expect(domains).toContain('blick.ch');
  });

  it('returns nothing for content with no URLs', () => {
    expect(extractSources('just recalled some context, no links')).toEqual([]);
    expect(extractSources(undefined)).toEqual([]);
  });
});

describe('ThinkingStream rendering', () => {
  it('shows a getting-started state with no steps while live', () => {
    render(<ThinkingStream steps={[]} live />);
    expect(screen.getByTestId('thinking-empty')).toBeInTheDocument();
    expect(screen.getByText('Getting started…')).toBeInTheDocument();
  });

  it('renders nothing when finished with no steps (not live)', () => {
    const { container } = render(<ThinkingStream steps={[]} />);
    expect(container).toBeEmptyDOMElement();
  });

  it('renders a phase heading + narrative, source chips for a web search, and a live pulse', () => {
    const steps: RunStep[] = [
      {
        id: '1',
        label: 'PerplexityTool',
        sublabel: 'Switzerland news today',
        detail: 'Title: SwissInfo\nUrl: https://www.swissinfo.ch/eng/x\nDescription: ...',
      },
    ];
    render(<ThinkingStream steps={steps} live />);
    expect(screen.getByText('Searching the web')).toBeInTheDocument();
    expect(screen.getByTestId('thinking-narrative')).toHaveTextContent('Switzerland news today');
    expect(screen.getByTestId('thinking-sources')).toBeInTheDocument();
    expect(screen.getByText('swissinfo.ch')).toBeInTheDocument();
    expect(screen.getByText('Thinking…')).toBeInTheDocument(); // live pulse
  });

  it('shows a readable excerpt (no chips) for content without URLs', () => {
    const steps: RunStep[] = [
      { id: '1', label: 'Memory', detail: 'High-priority AI updates today. Markets rose 12%.' },
    ];
    render(<ThinkingStream steps={steps} live />);
    expect(screen.getByText('Recalling context')).toBeInTheDocument();
    expect(screen.queryByTestId('thinking-sources')).not.toBeInTheDocument();
    expect(screen.getByText(/High-priority AI updates today/)).toBeInTheDocument();
  });

  it('finished view: no pulse, and a step with context opens its full context via onSelect', () => {
    const onSelect = vi.fn();
    const steps: RunStep[] = [
      { id: '1', label: 'Memory', sublabel: 'context retrieved', detail: 'Recalled the project goals and constraints.' },
    ];
    render(<ThinkingStream steps={steps} onSelect={onSelect} />);
    expect(screen.queryByText('Thinking…')).not.toBeInTheDocument(); // not live
    fireEvent.click(screen.getByText('Recalling context').closest('button')!);
    expect(onSelect).toHaveBeenCalledWith(expect.objectContaining({ id: '1' }));
  });
});
