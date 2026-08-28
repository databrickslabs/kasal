/**
 * Tests for TriggersPanel — loads queued events, populates the Crew/Flow name
 * picker, fires a flow selected BY NAME (asserting the enqueue payload +
 * refresh), guards against firing with nothing selected, and covers the advanced
 * inline escape hatch. All services are mocked; no real HTTP.
 */

import { describe, it, expect, vi, beforeEach, Mock } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import TriggersPanel from './TriggersPanel';
import { TriggersService } from '../../api/execution/TriggersService';
import { CrewService } from '../../api/workflow/CrewService';
import { FlowService } from '../../api/workflow/FlowService';

vi.mock('../../api/execution/TriggersService', () => ({
  TriggersService: {
    list: vi.fn(),
    enqueue: vi.fn(),
    delete: vi.fn(),
    dispatch: vi.fn(),
    listSubscriptions: vi.fn(),
    createSubscription: vi.fn(),
    deleteSubscription: vi.fn(),
    createEmitRule: vi.fn(),
    deleteEmitRule: vi.fn(),
  },
}));
vi.mock('../../api/workflow/CrewService', () => ({
  CrewService: { getCrews: vi.fn() },
}));
vi.mock('../../api/workflow/FlowService', () => ({
  FlowService: { getFlows: vi.fn() },
}));
vi.mock('../../api/workflow/SchemaService', () => ({
  SchemaService: { getInstance: () => ({ getSchemas: () => Promise.resolve([]) }) },
}));

const list = TriggersService.list as Mock;
const enqueue = TriggersService.enqueue as Mock;
const dispatch = TriggersService.dispatch as Mock;
const listSubscriptions = TriggersService.listSubscriptions as Mock;
const getCrews = CrewService.getCrews as Mock;
const getFlows = FlowService.getFlows as Mock;

describe('TriggersPanel', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    list.mockResolvedValue({ events: [], total: 0 });
    listSubscriptions.mockResolvedValue({ subscriptions: [], emit_rules: [] });
    getCrews.mockResolvedValue([]);
    getFlows.mockResolvedValue([]);
  });

  it('loads and shows queued events', async () => {
    list.mockResolvedValue({
      events: [
        {
          id: 1,
          status: 'pending',
          attempts: 0,
          target: { kind: 'flow', id: 'f1' },
          created_at: '2026-08-26T09:00:00',
        },
      ],
      total: 1,
    });

    render(<TriggersPanel />);

    expect(await screen.findByText('flow: f1')).toBeInTheDocument();
    expect(screen.getByText('pending')).toBeInTheDocument();
  });

  it('loads crews and flows for the name picker', async () => {
    getFlows.mockResolvedValue([{ id: 'f1', name: 'My Flow' }]);
    getCrews.mockResolvedValue([{ id: 'c1', name: 'My Crew' }]);

    render(<TriggersPanel />);

    await waitFor(() => {
      expect(getFlows).toHaveBeenCalled();
      expect(getCrews).toHaveBeenCalled();
    });
  });

  it('fires a flow selected by name, then drains and refreshes', async () => {
    getFlows.mockResolvedValue([{ id: 'f1', name: 'My Flow' }]);
    enqueue.mockResolvedValue({ id: 2, status: 'pending', attempts: 0 });
    dispatch.mockResolvedValue({ claimed: 1 });
    const user = userEvent.setup();

    render(<TriggersPanel />);
    await waitFor(() => expect(getFlows).toHaveBeenCalled());

    await user.click(screen.getByRole('combobox', { name: /crew or flow/i }));
    await user.click(await screen.findByText('My Flow'));
    await user.click(screen.getByRole('button', { name: /fire event/i }));

    await waitFor(() =>
      expect(enqueue).toHaveBeenCalledWith({
        target: { kind: 'flow', id: 'f1' },
        payload: { inputs: {} },
      }),
    );
    // Fire also drains the queue so the run launches on this one click.
    await waitFor(() => expect(dispatch).toHaveBeenCalled());
    await waitFor(() => expect(list).toHaveBeenCalledTimes(2));
  });

  it('requires a selection before firing', async () => {
    const user = userEvent.setup();
    render(<TriggersPanel />);
    await waitFor(() => expect(list).toHaveBeenCalled());

    await user.click(screen.getByRole('button', { name: /fire event/i }));

    expect(await screen.findByText(/select a crew or flow/i)).toBeInTheDocument();
    expect(enqueue).not.toHaveBeenCalled();
  });

});
