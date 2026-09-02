import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import MessageContent from './MessageContent';

describe('MessageContent', () => {
  it('renders plain text in a <p> with whitespace-pre-wrap when content has no markdown', () => {
    const { container } = render(<MessageContent content="just plain text" />);

    const p = container.querySelector('p.whitespace-pre-wrap');
    expect(p).not.toBeNull();
    expect(p?.textContent).toBe('just plain text');
    // Should not render the markdown prose wrapper.
    expect(container.querySelector('.prose')).toBeNull();
  });

  it('renders markdown content inside the prose wrapper using ReactMarkdown', () => {
    const { container } = render(<MessageContent content="# Heading" />);

    const wrapper = container.querySelector('div.prose');
    expect(wrapper).not.toBeNull();
    // ReactMarkdown should produce an actual heading element.
    const heading = container.querySelector('h1');
    expect(heading).not.toBeNull();
    expect(heading?.textContent).toBe('Heading');
    // The plain-text fallback paragraph should not be present.
    expect(container.querySelector('p.whitespace-pre-wrap')).toBeNull();
  });

  it('renders markdown code blocks (GFM enabled) for fenced code content', () => {
    const md = '```\nconst x = 1;\n```';
    const { container } = render(<MessageContent content={md} />);

    expect(container.querySelector('div.prose')).not.toBeNull();
    const code = container.querySelector('pre code');
    expect(code).not.toBeNull();
    expect(code?.textContent).toContain('const x = 1;');
  });

  it('renders empty plain content as an empty paragraph', () => {
    const { container } = render(<MessageContent content="" />);

    const p = container.querySelector('p.whitespace-pre-wrap');
    expect(p).not.toBeNull();
    expect(p?.textContent).toBe('');
    expect(container.querySelector('.prose')).toBeNull();
  });

  it('renders bold markdown inside the prose wrapper', () => {
    render(<MessageContent content="**bold text**" />);
    const strong = screen.getByText('bold text');
    expect(strong.tagName.toLowerCase()).toBe('strong');
  });

  it('renders an ```html block as a sandboxed diagram iframe (not a code block)', () => {
    const md = '```html\n<svg><rect width="10" height="10"/></svg>\n```';
    const { container } = render(<MessageContent content={md} />);

    const iframe = container.querySelector('iframe');
    expect(iframe).not.toBeNull();
    // Sandboxed WITHOUT allow-same-origin — the key isolation guarantee.
    expect(iframe?.getAttribute('sandbox')).toBe('allow-scripts');
    // The %md-sandbox copy affordance is present (icon-only button, by title).
    expect(screen.getByTitle('Copy as a Databricks %md-sandbox notebook cell')).not.toBeNull();
    // It is NOT rendered as a plain markdown code block.
    expect(container.querySelector('pre code')).toBeNull();
  });

  it('labels an unclosed ```html block as building only while streaming', () => {
    const md = 'here:\n```html\n<svg><rect';
    render(<MessageContent content={md} streaming />);
    expect(screen.getByText('Building diagram…')).not.toBeNull();
  });

  it('still renders a bare (non-html) code fence as a markdown code block', () => {
    const { container } = render(<MessageContent content={'```\nconst x = 1;\n```'} />);
    expect(container.querySelector('iframe')).toBeNull();
    expect(container.querySelector('pre code')).not.toBeNull();
  });

  it('renders an ```html deck (slide sections) as a paged deck, not a single diagram', () => {
    const deck =
      '```html\n<section class="slide"><h1>One</h1></section>' +
      '<section class="slide"><h1>Two</h1></section>\n```';
    const { container } = render(<MessageContent content={deck} />);
    // Deck renders in an iframe with slide navigation ("Slide 1 / 2").
    expect(container.querySelector('iframe')).not.toBeNull();
    expect(screen.getByText('Slide 1 / 2')).not.toBeNull();
    // Not the diagram card (no %md-sandbox copy affordance for a deck).
    expect(screen.queryByText('Copy %md-sandbox cell')).toBeNull();
  });
});


describe('unclosed fence lifecycle', () => {
  const unclosedDeck =
    'Here is the deck:\n```html\n<section class="slide">One</section>' +
    '<section class="slide">Two</section><div style="margin-bottom:';

  it('shows Building deck while the message is still streaming', () => {
    render(<MessageContent content={unclosedDeck} streaming />);
    expect(screen.getByText('Building deck…')).toBeInTheDocument();
  });

  it('finalizes a truncated deck when the message has ended', () => {
    // The fence never closed but the stream is over — the deck must NOT build
    // forever; it pages normally and is labelled incomplete.
    render(<MessageContent content={unclosedDeck} />);
    expect(screen.queryByText('Building deck…')).not.toBeInTheDocument();
    expect(screen.getByText(/incomplete/)).toBeInTheDocument();
  });

  it('renders a ```skill block as a skill card, with the surrounding text', () => {
    const md = '---\nname: writing-release-notes\ndescription: Use when drafting release notes.\n---\n\n# Body\n';
    render(<MessageContent content={`Here is the draft:\n\n\`\`\`skill\n${md}\`\`\`\n\nI left out nothing.`} />);
    expect(screen.getByRole('group', { name: /Skill draft writing-release-notes/ })).toBeInTheDocument();
    expect(screen.getByText('Use when drafting release notes.')).toBeInTheDocument();
    expect(screen.getByText(/Here is the draft/)).toBeInTheDocument();
    expect(screen.getByText(/I left out nothing/)).toBeInTheDocument();
  });
});
