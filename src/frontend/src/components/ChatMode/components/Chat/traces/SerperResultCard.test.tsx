import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { SerperResultCard, matchesSerper } from './SerperResultCard';
import { ToolResultCard } from './ToolResultCard';

describe('matchesSerper', () => {
  it('matches by label (serper / search the internet)', () => {
    expect(matchesSerper('', 'SerperDevTool')).toBe(true);
    expect(matchesSerper('', 'Search the internet')).toBe(true);
  });
  it('matches by JSON shape (searchParameters + organic)', () => {
    expect(matchesSerper('{"searchParameters":{"q":"x"},"organic":[]}')).toBe(true);
  });
  it('does not match plain text / partial JSON / no label', () => {
    expect(matchesSerper('just a tool answer', 'PerplexityTool')).toBe(false);
    expect(matchesSerper('{"organic":[]}')).toBe(false); // missing searchParameters
  });
});

describe('SerperResultCard', () => {
  it('renders the answer box, organic results and "people also ask"', () => {
    const detail = JSON.stringify({
      searchParameters: { q: 'capital of switzerland' },
      answerBox: { answer: 'Bern' },
      organic: [
        { title: 'T1', link: 'https://a.example', snippet: 'first snippet' },
        { title: 'T2' }, // no link → plain title, no snippet
      ],
      peopleAlsoAsk: [
        { question: 'Why Bern?', snippet: 'because' },
        { foo: 'bar' }, // no question → skipped
      ],
    });
    const { container } = render(<SerperResultCard detail={detail} />);
    expect(screen.getByText('Bern')).toBeInTheDocument(); // answer box
    expect(screen.getByText('T1')).toBeInTheDocument(); // linked result
    expect(container.textContent).toContain('first snippet');
    expect(screen.getByText('T2')).toBeInTheDocument(); // unlinked result
    expect(screen.getByText('People also ask')).toBeInTheDocument();
    expect(screen.getByText(/Why Bern\?/)).toBeInTheDocument();
  });

  it('handles an empty answer box and a link-only organic result', () => {
    const detail = JSON.stringify({ answerBox: {}, organic: [{ link: 'https://only.example' }] });
    const { container } = render(<SerperResultCard detail={detail} />);
    expect(container.textContent).toContain('https://only.example'); // title falls back to link
  });

  it('falls back to "Result N" with no title/link, and omits empty PAA snippets', () => {
    const detail = JSON.stringify({
      organic: [{ snippet: 'no title or link here' }],
      peopleAlsoAsk: [{ question: 'Plain question' }], // no snippet
    });
    const { container } = render(<SerperResultCard detail={detail} />);
    expect(container.textContent).toContain('Result 1');
    expect(screen.getByText('Plain question')).toBeInTheDocument();
  });

  it('returns null for invalid JSON', () => {
    const { container } = render(<SerperResultCard detail={'not json {'} />);
    expect(container).toBeEmptyDOMElement();
  });

  it('returns null when the JSON is not an object', () => {
    const { container } = render(<SerperResultCard detail={'5'} />);
    expect(container).toBeEmptyDOMElement();
  });

  it('returns null when there is nothing renderable (empty object)', () => {
    const { container } = render(<SerperResultCard detail={'{}'} />);
    expect(container).toBeEmptyDOMElement();
  });
});

describe('ToolResultCard', () => {
  it('renders the answer body as markdown', () => {
    const { container } = render(<ToolResultCard detail={'Hello **world**'} />);
    expect(container.textContent).toContain('world');
  });
  it('returns null for empty / whitespace-only content', () => {
    const { container, rerender } = render(<ToolResultCard detail={''} />);
    expect(container).toBeEmptyDOMElement();
    rerender(<ToolResultCard detail={'   '} />);
    expect(container).toBeEmptyDOMElement();
  });
});
