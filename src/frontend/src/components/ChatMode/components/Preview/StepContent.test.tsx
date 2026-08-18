import React from 'react';
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import StepContent from './StepContent';

describe('StepContent', () => {
  it('renders a plan as a checklist, not as its bracket markers', () => {
    render(
      <StepContent
        step={{
          id: '1',
          label: 'todo (output)',
          detail: '[>] 1. Create the table',
          plan: [{ id: '1', content: 'Create the table', status: 'in_progress' }],
        }}
      />,
    );
    expect(screen.getByText('Plan')).toBeInTheDocument();
    expect(screen.getByText('Create the table')).toBeInTheDocument();
    expect(screen.queryByText('[>] 1. Create the table')).not.toBeInTheDocument();
  });

  it('renders a code payload as a labelled, copyable block', () => {
    render(
      <StepContent
        step={{
          id: '2',
          label: 'postgres_execute_sql',
          code: { language: 'sql', text: 'CREATE TABLE t (\n  id SERIAL\n);' },
        }}
      />,
    );
    expect(screen.getByText('sql')).toBeInTheDocument();
    expect(screen.getByLabelText('Copy code')).toBeInTheDocument();
    // Highlighted, so the statement is split across token spans — assert on the
    // block's text rather than on one node.
    expect(document.querySelector('pre')?.textContent).toContain('CREATE TABLE');
  });

  it('renders markdown output as markdown, with headings sized for the pane', () => {
    // A page-reading tool returns markdown. Rendered as plain text it was a
    // wall of literal "##"; rendered through the (uninstalled) typography
    // plugin the headings fell back to the user agent's 2em and overlapped.
    render(
      <StepContent
        step={{
          id: '3',
          label: 'browser_search_and_read (output)',
          detail: '## Sources\n\n[1] Something — a source\n\n### Page contents\n\nBody text here.',
        }}
      />,
    );
    const heading = screen.getByText('Sources');
    expect(heading.tagName).toBe('H2');
    expect(heading).toHaveClass('leading-snug');
    expect(screen.getByText('Page contents').tagName).toBe('H3');
    expect(screen.getByText('Body text here.')).toBeInTheDocument();
  });

  it('renders plain text as plain text', () => {
    render(<StepContent step={{ id: '4', label: 'x', detail: 'just a sentence' }} />);
    expect(screen.getByText('just a sentence')).toBeInTheDocument();
  });
});
