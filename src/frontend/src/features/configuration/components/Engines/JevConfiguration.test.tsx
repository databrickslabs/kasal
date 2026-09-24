import React from 'react';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import JevConfiguration from './JevConfiguration';
import { DecisionConfigService } from '../../../../api/config/DecisionConfigService';

vi.mock('../../../../api/config/DecisionConfigService', () => ({
  DecisionConfigService: { getConfig: vi.fn(), saveConfig: vi.fn(), recommend: vi.fn() },
}));
vi.mock('../../../../store/groups', () => ({
  useGroupStore: (selector: (state: { currentGroupId: string }) => unknown) => selector({ currentGroupId: 'one' }),
}));

describe('JevConfiguration', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(DecisionConfigService.getConfig).mockResolvedValue({
      enabled: false, api_key_configured: false,
    });
  });

  it('uses the existing API Keys screen instead of collecting another key', async () => {
    render(<JevConfiguration />);
    await screen.findByText(/Add JEV_API_KEY in Configuration/);
    const toggle = screen.getByRole('checkbox', { name: 'Use Jev for decisions' });
    expect(toggle).not.toBeChecked();
    expect(toggle).toBeDisabled();
    expect(screen.queryByLabelText('Jev API key')).not.toBeInTheDocument();
  });

  it('enables using a key already configured in the API key service', async () => {
    vi.mocked(DecisionConfigService.getConfig).mockResolvedValue({
      enabled: false, api_key_configured: true,
    });
    vi.mocked(DecisionConfigService.saveConfig).mockResolvedValue({
      enabled: true, api_key_configured: true,
    });
    render(<JevConfiguration />);
    const toggle = screen.getByRole('checkbox', { name: 'Use Jev for decisions' });
    await waitFor(() => expect(toggle).toBeEnabled());
    fireEvent.click(toggle);
    await screen.findByText('Jev settings saved.');
    expect(DecisionConfigService.saveConfig).toHaveBeenCalledWith(true);
  });

  it('disables without overwriting the stored key', async () => {
    vi.mocked(DecisionConfigService.getConfig).mockResolvedValue({
      enabled: true, api_key_configured: true,
    });
    vi.mocked(DecisionConfigService.saveConfig).mockResolvedValue({
      enabled: false, api_key_configured: true,
    });
    render(<JevConfiguration />);
    const toggle = screen.getByRole('checkbox', { name: 'Use Jev for decisions' });
    await waitFor(() => expect(toggle).toBeChecked());
    fireEvent.click(toggle);
    await screen.findByText('Jev settings saved.');
    expect(DecisionConfigService.saveConfig).toHaveBeenCalledWith(false);
  });

  it('reverts the toggle and shows an error when saving fails', async () => {
    vi.mocked(DecisionConfigService.getConfig).mockResolvedValue({
      enabled: false, api_key_configured: true,
    });
    vi.mocked(DecisionConfigService.saveConfig).mockRejectedValue(new Error('denied'));
    render(<JevConfiguration />);
    const toggle = screen.getByRole('checkbox', { name: 'Use Jev for decisions' });
    await waitFor(() => expect(toggle).toBeEnabled());
    fireEvent.click(toggle);
    await screen.findByText(/Could not save Jev settings/);
    expect(screen.queryByText('Jev settings saved.')).not.toBeInTheDocument();
    expect(toggle).not.toBeChecked();
  });
  it('restores the saved setting after reopening the panel', async () => {
    let stored = false;
    vi.mocked(DecisionConfigService.getConfig).mockImplementation(async () => ({
      enabled: stored, api_key_configured: true,
    }));
    vi.mocked(DecisionConfigService.saveConfig).mockImplementation(async (enabled) => {
      stored = enabled;
      return { enabled, api_key_configured: true };
    });
    const first = render(<JevConfiguration />);
    const toggle = screen.getByRole('checkbox', { name: 'Use Jev for decisions' });
    await waitFor(() => expect(toggle).toBeEnabled());
    fireEvent.click(toggle);
    await screen.findByText('Jev settings saved.');
    first.unmount();
    render(<JevConfiguration />);
    await waitFor(() => expect(screen.getByRole('checkbox', { name: 'Use Jev for decisions' })).toBeChecked());
    expect(DecisionConfigService.saveConfig).toHaveBeenCalledTimes(1);
  });

});
