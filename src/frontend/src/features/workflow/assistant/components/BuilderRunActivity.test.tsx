import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { makeProcessedTraces } from '../../../chat/components/Preview/testTraceFixture';
import { useRunTimeline } from '../../../chat/hooks/useRunTimeline';
import { BuilderPreviewContext } from './BuilderPreviewContext';
import BuilderRunActivity from './BuilderRunActivity';
vi.mock('../../../chat/hooks/useRunTimeline', () => ({ useRunTimeline: vi.fn() }));

describe('Builder run activity', () => {
  it('opens during execution, requests live traces, and opens the selected call in preview', () => {
    vi.mocked(useRunTimeline).mockReturnValue({ processed: makeProcessedTraces(), loading: false });
    const openStep = vi.fn();
    render(<BuilderPreviewContext.Provider value={{ openMemory: vi.fn(), openStep }}><BuilderRunActivity jobId="live-run" running /></BuilderPreviewContext.Provider>);
    expect(screen.getByRole('button', { name: 'Collapse run activity' })).toBeVisible();
    expect(useRunTimeline).toHaveBeenCalledWith('live-run', true, true);
    fireEvent.click(screen.getByRole('button', { name: /postgres_execute_sql/ }));
    expect(openStep).toHaveBeenCalledWith('live-run', expect.objectContaining({ detail: 'CREATE TABLE', label: 'postgres_execute_sql (output)' }));
  });
});
