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
});
