import { describe, it, expect, vi, beforeEach, Mock } from 'vitest';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';

import SubscriptionsSection from './SubscriptionsSection';
import { TriggersService } from '../../api/execution/TriggersService';
import { CrewService } from '../../api/workflow/CrewService';
import { FlowService } from '../../api/workflow/FlowService';
import { SchemaService } from '../../api/workflow/SchemaService';

vi.mock('../../api/execution/TriggersService', () => ({
  TriggersService: {
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
  SchemaService: { getInstance: vi.fn() },
}));

const getSchemas = vi.fn();
(SchemaService.getInstance as Mock).mockReturnValue({ getSchemas });

const listSubscriptions = TriggersService.listSubscriptions as Mock;
const createSubscription = TriggersService.createSubscription as Mock;
const createEmitRule = TriggersService.createEmitRule as Mock;
const getCrews = CrewService.getCrews as Mock;
const getFlows = FlowService.getFlows as Mock;

describe('SubscriptionsSection', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    (SchemaService.getInstance as Mock).mockReturnValue({ getSchemas });
    getSchemas.mockResolvedValue([]);
    listSubscriptions.mockResolvedValue({ subscriptions: [], emit_rules: [] });
    getCrews.mockResolvedValue([]);
    getFlows.mockResolvedValue([]);
  });

  it('renders a fully-wired chain by friendly name + type, no warning', async () => {
    getFlows.mockResolvedValue([{ id: 'f1', name: 'Producer Flow' }]);
    getCrews.mockResolvedValue([{ id: 'c1', name: 'Consumer Crew' }]);
    listSubscriptions.mockResolvedValue({
      // Producer Flow opts into "completed" → emits flow:f1:completed
      emit_rules: [
        {
          id: 2,
          on_target: { kind: 'flow', id: 'f1' },
          event_type: 'completed',
          enabled: true,
        },
      ],
      // Consumer Crew listens for that exact canonical event
      subscriptions: [
        {
          id: 1,
          event_type: 'flow:f1:completed',
          target: { kind: 'crew', id: 'c1' },
          enabled: true,
        },
      ],
    });

    render(<SubscriptionsSection />);

    // Emit-rule row: producer name + lifecycle type chip.
    await waitFor(() =>
      expect(screen.getByText('Producer Flow')).toBeInTheDocument(),
    );
    expect(screen.getByText('completed')).toBeInTheDocument();
    // Subscription row: canonical event rendered as "producer · type" + target.
    expect(screen.getByText('Producer Flow · completed')).toBeInTheDocument();
    expect(screen.getByText('Consumer Crew')).toBeInTheDocument();
    // Wired on flow:f1:completed → no dangling warning anywhere.
    expect(screen.queryByTestId('dangling-warning')).not.toBeInTheDocument();
  });

  it('flags an emit rule with no subscriber and an orphaned subscription', async () => {
    getFlows.mockResolvedValue([{ id: 'f1', name: 'Producer Flow' }]);
    getCrews.mockResolvedValue([{ id: 'c1', name: 'Consumer Crew' }]);
    listSubscriptions.mockResolvedValue({
      // Emits flow:f1:completed which nothing subscribes to → no subscriber.
      emit_rules: [
        {
          id: 2,
          on_target: { kind: 'flow', id: 'f1' },
          event_type: 'completed',
          enabled: true,
        },
      ],
      // Listens for crew:cZ:completed which no emit rule produces → orphaned.
      subscriptions: [
        {
          id: 1,
          event_type: 'crew:cZ:completed',
          target: { kind: 'crew', id: 'c1' },
          enabled: true,
        },
      ],
    });

    render(<SubscriptionsSection />);

    await waitFor(() =>
      expect(screen.getAllByTestId('dangling-warning')).toHaveLength(2),
    );
  });

  it('shows empty-state text when nothing is configured', async () => {
    render(<SubscriptionsSection />);
    await waitFor(() =>
      expect(screen.getByText('No event triggers yet.')).toBeInTheDocument(),
    );
    expect(screen.getByText('No emit rules yet.')).toBeInTheDocument();
  });

  it('validates a subscription needs an event and a target', async () => {
    render(<SubscriptionsSection />);
    await waitFor(() => expect(listSubscriptions).toHaveBeenCalled());

    fireEvent.click(screen.getByRole('button', { name: /add subscription/i }));

    await waitFor(() =>
      expect(
        screen.getByText(
          'Pick an event to listen for and the crew/flow to run',
        ),
      ).toBeInTheDocument(),
    );
    expect(createSubscription).not.toHaveBeenCalled();
  });

  it('validates an emit rule needs a source crew/flow', async () => {
    render(<SubscriptionsSection />);
    await waitFor(() => expect(listSubscriptions).toHaveBeenCalled());

    fireEvent.click(screen.getByRole('button', { name: /add emit rule/i }));

    await waitFor(() =>
      expect(
        screen.getByText('Pick the crew/flow that emits and an event type'),
      ).toBeInTheDocument(),
    );
    expect(createEmitRule).not.toHaveBeenCalled();
  });
});
