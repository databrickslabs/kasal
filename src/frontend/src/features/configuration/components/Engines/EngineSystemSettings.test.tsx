import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import EngineSystemSettings from './EngineSystemSettings';
import { EngineConfigService } from '../../../../api/config/EngineConfigService';
import type { EngineSettings } from '../../../../types/config/engines';

vi.mock('../../../../api/config/EngineConfigService', () => ({
  EngineConfigService: { getSettings: vi.fn(), updateSettings: vi.fn() },
}));

const deep = { max_iter: 30, max_execution_time: 1200, run_wall_clock: 3600, guardrail_max_retries: 3 };

function settings(overrides: Partial<EngineSettings> = {}): EngineSettings {
  return {
    jev_api_base: null,
    agent_max_execution_time: 900,
    agent_max_execution_time_default: 900,
    budgets: { deep },
    budget_defaults: { deep },
    advanced: { memory_sweep_enabled: true, knowledge_min_score: 0.35 },
    advanced_specs: {
      memory_sweep_enabled: { default: true, minimum: null, maximum: null },
      knowledge_min_score: { default: 0.35, minimum: 0, maximum: 1 },
    },
    ...overrides,
  };
}

describe('EngineSystemSettings', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(EngineConfigService.getSettings).mockResolvedValue(settings());
  });

  it('renders nothing when the viewer may not read system settings', async () => {
    vi.mocked(EngineConfigService.getSettings).mockRejectedValue({ response: { status: 403 } });
    const { container } = render(<EngineSystemSettings />);
    await waitFor(() => expect(EngineConfigService.getSettings).toHaveBeenCalled());
    expect(container).toBeEmptyDOMElement();
  });

  it('saves the Jev API URL and refuses plain http', async () => {
    vi.mocked(EngineConfigService.updateSettings).mockResolvedValue(
      settings({ jev_api_base: 'https://jev.example.com' }),
    );
    render(<EngineSystemSettings />);
    const field = await screen.findByLabelText('Jev API URL');

    fireEvent.change(field, { target: { value: 'http://jev.example.com' } });
    expect(screen.getByText('Must start with https://')).toBeInTheDocument();

    fireEvent.change(field, { target: { value: ' https://jev.example.com ' } });
    fireEvent.click(screen.getAllByRole('button', { name: 'Save' })[0]);
    await waitFor(() =>
      expect(EngineConfigService.updateSettings).toHaveBeenCalledWith({
        jev_api_base: 'https://jev.example.com',
      }),
    );
  });

  it('resets a run budget field to its default by sending null', async () => {
    vi.mocked(EngineConfigService.updateSettings).mockResolvedValue(settings());
    render(<EngineSystemSettings />);
    fireEvent.click(await screen.findByText('Advanced'));
    const resets = screen.getAllByRole('button', { name: 'Reset to default' });
    // [0] is the agent time limit; [1] the first deep budget field (max_iter).
    fireEvent.click(resets[1]);
    await waitFor(() =>
      expect(EngineConfigService.updateSettings).toHaveBeenCalledWith({
        budgets: { deep: { max_iter: null } },
      }),
    );
  });

  it('shows the server reason when a save is refused', async () => {
    vi.mocked(EngineConfigService.updateSettings).mockRejectedValue({
      response: { data: { detail: 'deep.max_iter must be at least 1' } },
    });
    render(<EngineSystemSettings />);
    fireEvent.click(await screen.findByText('Advanced'));
    fireEvent.click(screen.getAllByRole('button', { name: 'Reset to default' })[0]);
    expect(await screen.findByText('deep.max_iter must be at least 1')).toBeInTheDocument();
  });

  it('turns off the memory sweep and accepts a decimal relevance floor', async () => {
    vi.mocked(EngineConfigService.updateSettings).mockResolvedValue(settings());
    render(<EngineSystemSettings />);
    fireEvent.click(await screen.findByText('Advanced'));

    fireEvent.click(screen.getByRole('checkbox', { name: 'Background memory sweep' }));
    await waitFor(() =>
      expect(EngineConfigService.updateSettings).toHaveBeenCalledWith({
        advanced: { memory_sweep_enabled: false },
      }),
    );

    fireEvent.change(screen.getByLabelText('Minimum relevance'), { target: { value: '0.5' } });
    const saves = screen.getAllByRole('button', { name: 'Save' });
    fireEvent.click(saves[saves.length - 1]);
    await waitFor(() =>
      expect(EngineConfigService.updateSettings).toHaveBeenCalledWith({
        advanced: { knowledge_min_score: 0.5 },
      }),
    );
  });
});
