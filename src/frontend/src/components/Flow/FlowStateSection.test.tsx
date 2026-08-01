/**
 * The flow-level declaration, hosted inside the edge dialog.
 *
 * It lives beside the per-edge checkpoint switch because the two are only
 * useful together: a conversation needs somewhere to live (this) and something
 * to write it (checkpoint). Having them behind two different controls is how a
 * flow ends up with one of the two and silently never continues.
 *
 * The regression this pins: opening a saved flow on a new canvas showed the
 * toggle OFF even though the flow had it on. The declaration used to be seeded
 * by the flow-load handler, which wrote to whatever tab was active at that
 * instant — and a flow opening into a NEW canvas activates its tab at about the
 * same moment, so it landed on the wrong one. It now hydrates itself.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import FlowStateSection from './FlowStateSection';
import { useFlowStateStore } from '../../store/flowState';
import { useTabManagerStore } from '../../store/tabManager';
import { FlowService } from '../../api/workflow/FlowService';

vi.mock('../../api/workflow/FlowService', () => ({
  FlowService: { getFlow: vi.fn(), updateFlowState: vi.fn() },
}));

const TAB = 'tab-1';

/** A tab the store will accept — it reads the date fields when persisting. */
const tab = (extra: Record<string, unknown> = {}) => ({
  id: TAB,
  name: 'Flow',
  nodes: [],
  edges: [],
  flowNodes: [],
  flowEdges: [],
  viewMode: 'flow' as const,
  isActive: true,
  isDirty: false,
  createdAt: new Date(),
  lastModified: new Date(),
  ...extra,
});

beforeEach(() => {
  vi.clearAllMocks();
  useFlowStateStore.setState({ declared: {} });
  useTabManagerStore.setState({
    activeTabId: TAB,
    tabs: [tab({ savedFlowId: 'flow-1' })],
  } as never);
  (FlowService.getFlow as ReturnType<typeof vi.fn>).mockResolvedValue(null);
  (FlowService.updateFlowState as ReturnType<typeof vi.fn>).mockResolvedValue(true);
});

describe('hydration from the saved flow', () => {
  it('shows the toggle ON for a flow that declares a conversation', async () => {
    (FlowService.getFlow as ReturnType<typeof vi.fn>).mockResolvedValue({
      flowConfig: { state: { conversational: true } },
    });

    render(<FlowStateSection />);

    await waitFor(() =>
      expect(screen.getByRole('checkbox', { name: /hold a conversation/i })).toBeChecked(),
    );
  });

  it('leaves it off for a flow that declares nothing', async () => {
    (FlowService.getFlow as ReturnType<typeof vi.fn>).mockResolvedValue({
      flowConfig: {},
    });

    render(<FlowStateSection />);

    await waitFor(() => expect(FlowService.getFlow).toHaveBeenCalled());
    expect(
      screen.getByRole('checkbox', { name: /hold a conversation/i }),
    ).not.toBeChecked();
  });

  it('does not overwrite an unsaved edit', async () => {
    // Opening the dialog twice must not silently discard a change the user made
    // and has not saved yet.
    useFlowStateStore.getState().setConversational(TAB, true);

    render(<FlowStateSection />);

    await waitFor(() =>
      expect(screen.getByRole('checkbox', { name: /hold a conversation/i })).toBeChecked(),
    );
    expect(FlowService.getFlow).not.toHaveBeenCalled();
  });

  it('does not call the API for an unsaved flow', async () => {
    useTabManagerStore.setState({ tabs: [tab()] } as never);

    render(<FlowStateSection />);

    await waitFor(() => expect(FlowService.getFlow).not.toHaveBeenCalled());
  });

  it('still renders when the flow cannot be read', async () => {
    // Failing to read the saved flow must not block editing.
    (FlowService.getFlow as ReturnType<typeof vi.fn>).mockRejectedValue(
      new Error('offline'),
    );

    render(<FlowStateSection />);

    expect(
      await screen.findByRole('checkbox', { name: /hold a conversation/i }),
    ).toBeInTheDocument();
  });
});

describe('editing', () => {
  it('records the toggle against the active tab', async () => {
    render(<FlowStateSection />);

    await userEvent.click(
      screen.getByRole('checkbox', { name: /hold a conversation/i }),
    );

    expect(useFlowStateStore.getState().getDeclared(TAB)?.conversational).toBe(true);
  });

  it('writes to the flow the moment it changes', async () => {
    // The edge dialog's Save commits the EDGE. Leaving this to ride along on a
    // later flow save is what made the toggle look like it did nothing.
    render(<FlowStateSection />);

    await userEvent.click(
      screen.getByRole('checkbox', { name: /hold a conversation/i }),
    );

    await waitFor(() =>
      expect(FlowService.updateFlowState).toHaveBeenCalledWith(
        'flow-1',
        expect.objectContaining({ conversational: true }),
      ),
    );
  });

  it('does not try to write an unsaved flow', async () => {
    // Nothing to write to yet; the store carries it and the first save picks
    // it up.
    useTabManagerStore.setState({ tabs: [tab()] } as never);
    render(<FlowStateSection />);

    await userEvent.click(
      screen.getByRole('checkbox', { name: /hold a conversation/i }),
    );

    expect(FlowService.updateFlowState).not.toHaveBeenCalled();
  });

  it('says the setting belongs to the whole flow, not this edge', async () => {
    // It is hosted in an EDGE dialog next to a per-edge switch; without saying
    // so, "applies to everything" is invisible.
    render(<FlowStateSection />);

    // The copy wraps across JSX lines, and ancestors match too — assert that
    // SOMETHING says it rather than pinning one element.
    expect(
      screen.getAllByText((_, el) => /WHOLE flow/i.test(el?.textContent ?? '')).length,
    ).toBeGreaterThan(0);
  });
});
