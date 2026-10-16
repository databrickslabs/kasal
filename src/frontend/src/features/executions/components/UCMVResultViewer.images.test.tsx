/**
 * The migration report is markdown written by the migration crew, so it gets
 * the chat answer's image policy: a remote image waits for a click.
 */
import { render } from '@testing-library/react';
import { describe, it, expect } from 'vitest';
import UCMVResultViewer, { type UCMVResult } from './UCMVResultViewer';

const remote = 'https://images.example.org/c.png?d=1';

const result: UCMVResult = {
  yaml: { mv_fact_test: "version: '1.1'\nsource: cat.sch.fact_test\n" },
  sql: {},
  stats: {},
  views_generated: 1,
  migration_report: `## Report\n\n![chart](${remote}) ![logo](/logo192.png)\n\n[details](https://example.com/page)`,
};

describe('UCMVResultViewer migration report images', () => {
  it('shows a remote image as a placeholder; same-origin images and links render as before', () => {
    // The report sits in a collapsed accordion: mounted, but hidden from roles.
    const { container } = render(<UCMVResultViewer result={result} />);
    expect(container.querySelector(`img[src="${remote}"]`)).toBeNull();
    const load = container.querySelector(`a[href="${remote}"]`);
    expect(load?.textContent).toBe('Load image');
    expect(load).toHaveAttribute('target', '_blank');
    expect(container.querySelector('img[src="/logo192.png"]')).not.toBeNull();
    expect(container.querySelector('a[href="https://example.com/page"]')?.textContent).toBe('details');
  });
});
