import React from 'react';
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRecordMarkdown } from './MemoryRecordMarkdown';

const md = '# Title\n\nSome **bold** text.\n\n| a | b |\n|---|---|\n| 1 | 2 |';

describe('MemoryRecordMarkdown', () => {
  it('renders headings, emphasis and tables', () => {
    render(<MemoryRecordMarkdown content={md} expanded />);
    expect(screen.getByRole('heading', { name: 'Title' })).toBeInTheDocument();
    expect(screen.getByText('bold').tagName).toBe('STRONG');
    expect(screen.getByRole('table')).toBeInTheDocument();
  });

  it('collapsed cards are clipped under a fade; expanded ones are not', () => {
    const { container, rerender } = render(<MemoryRecordMarkdown content={md} expanded={false} />);
    const box = container.querySelector('.kasal-memory-md') as HTMLElement;
    expect(box.getAttribute('data-expanded')).toBe('false');
    expect(box.style.overflow).toBe('hidden');
    rerender(<MemoryRecordMarkdown content={md} expanded />);
    expect(box.getAttribute('data-expanded')).toBe('true');
    expect(box.style.overflow).toBe('');
  });

  it('plain text still reads as plain text', () => {
    render(<MemoryRecordMarkdown content="User asked for swiss news." expanded={false} />);
    expect(screen.getByText('User asked for swiss news.')).toBeInTheDocument();
  });
});
