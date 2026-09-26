import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import SystemSettingsPanel from './SystemSettingsPanel';
import { EngineConfigService } from '../../../../api/config/EngineConfigService';
import type { EngineSettings } from '../../../../types/config/engines';

vi.mock('../../../../api/config/EngineConfigService', () => ({
  EngineConfigService: { getSettings: vi.fn(), updateSettings: vi.fn() },
}));

const settings: EngineSettings = {
  jev_api_base: null,
  agent_max_execution_time: 900,
  agent_max_execution_time_default: 900,
  budgets: {},
  budget_defaults: {},
  advanced: { a2ui_streaming: true, a2ui_compose_retries: 2, event_triggers_max_hops: 5 },
  advanced_specs: {
    a2ui_streaming: { default: true, minimum: null, maximum: null },
    a2ui_compose_retries: { default: 2, minimum: 1, maximum: 10 },
    event_triggers_max_hops: { default: 5, minimum: 1, maximum: 100 },
  },
};

describe('SystemSettingsPanel', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(EngineConfigService.getSettings).mockResolvedValue(settings);
    vi.mocked(EngineConfigService.updateSettings).mockResolvedValue(settings);
  });

  it('renders nothing for someone who may not read system settings', async () => {
    vi.mocked(EngineConfigService.getSettings).mockRejectedValue({ response: { status: 403 } });
    const { container } = render(<SystemSettingsPanel group="a2ui" />);
    await waitFor(() => expect(EngineConfigService.getSettings).toHaveBeenCalled());
    expect(container).toBeEmptyDOMElement();
  });

  it("shows only its own group's settings and saves them", async () => {
    render(<SystemSettingsPanel group="a2ui" />);
    fireEvent.click(await screen.findByText(/Advanced: Rich answers/));
    expect(screen.queryByLabelText('Chain depth limit')).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole('checkbox', { name: 'Stream surfaces as they are composed' }));
    await waitFor(() =>
      expect(EngineConfigService.updateSettings).toHaveBeenCalledWith({ advanced: { a2ui_streaming: false } }),
    );

    fireEvent.change(screen.getByLabelText('Compose attempts'), { target: { value: '4' } });
    fireEvent.click(screen.getByRole('button', { name: 'Save' }));
    await waitFor(() =>
      expect(EngineConfigService.updateSettings).toHaveBeenCalledWith({ advanced: { a2ui_compose_retries: 4 } }),
    );
  });
});
