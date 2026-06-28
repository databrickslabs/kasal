import React from 'react';
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import LogSurface from './LogSurface';

// useWorkspaceThemes hits UIConfigService.getConfig; with no mock it rejects and
// the hook falls back to null, so LogSurface renders with the built-in LOGS_THEME.

describe('LogSurface', () => {
  it('renders prose content as a themed, readable A2UI surface', () => {
    render(<LogSurface body="Recalled the project goals and constraints." />);
    expect(screen.getByText(/Recalled the project goals/)).toBeInTheDocument();
  });

  it('renders a MemoryMatch repr (search_memory recall) down to its content text', () => {
    // Shape the search_memory tool emits — a Python repr of MemoryMatch records.
    // Synthetic, non-PII. Regression: the run-activity step detail rendered empty.
    const body =
      "[MemoryMatch(record=MemoryRecord(id='a1', content='Key sample developments: item one, item two', scope='/g/x', categories=['News'], metadata={'entities': []})), " +
      "MemoryMatch(record=MemoryRecord(id='b2', content='Secondary sample note: item three', scope='/g/x', categories=['News'], metadata={'entities': []}))]";
    render(<LogSurface body={body} />);
    expect(screen.getByText(/Key sample developments/)).toBeInTheDocument();
    expect(screen.getByText(/Secondary sample note/)).toBeInTheDocument();
  });

  it('renders a SQL/Genie statement_response as a data table', () => {
    const body = JSON.stringify({
      statement_id: 'x',
      manifest: { schema: { columns: [{ name: 'ticker' }, { name: 'price' }] } },
      result: { data_array: [{ values: [{ string_value: 'TSLA' }, { string_value: '420' }] }] },
    });
    const { container } = render(<LogSurface body={body} />);
    expect(container.querySelector('table')).not.toBeNull();
    expect(screen.getByText('TSLA')).toBeInTheDocument();
  });
});
