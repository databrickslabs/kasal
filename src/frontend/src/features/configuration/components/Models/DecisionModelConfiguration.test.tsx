import React from 'react';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import DecisionModelConfiguration from './DecisionModelConfiguration';
import { DecisionConfigService } from '../../../../api/config/DecisionConfigService';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({
    t: (_key: string, options: Record<string, string>) =>
      (options.defaultValue ?? _key).replace(/{{(\w+)}}/g, (_m, name: string) => options[name] ?? ''),
  }),
}));
vi.mock('../../../../api/config/DecisionConfigService', () => ({
  DecisionConfigService: { getConfig: vi.fn(), saveConfig: vi.fn(), recommend: vi.fn() },
}));
vi.mock('../../../../store/groups', () => ({
  useGroupStore: (selector: (state: { currentGroupId: string }) => unknown) => selector({ currentGroupId: 'one' }),
}));

const TOGGLE = { name: 'Use a decision model' };

describe('DecisionModelConfiguration', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(DecisionConfigService.getConfig).mockResolvedValue({
      enabled: false, api_key_configured: false,
    });
  });

  it('names the setting generically and Jev only as its provider', async () => {
    render(<DecisionModelConfiguration />);
    expect(screen.getByText('Decision model')).toBeInTheDocument();
    expect(screen.getByText('Provider: Jev')).toBeInTheDocument();
    expect(screen.getByText(/sent to the decision model provider when enabled/)).toBeInTheDocument();
    expect(screen.queryByText(/Jev decisions/)).not.toBeInTheDocument();
  });

  it('uses the existing API Keys screen instead of collecting another key', async () => {
    const navigate = vi.fn();
    window.addEventListener('kasal:navigate-config', navigate);
    render(<DecisionModelConfiguration />);
    await screen.findByText(/Add JEV_API_KEY in Configuration/);
    const toggle = screen.getByRole('checkbox', TOGGLE);
    expect(toggle).not.toBeChecked();
    expect(toggle).toBeDisabled();
    expect(screen.queryByLabelText(/API key/i)).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Open API Keys' }));
    expect((navigate.mock.calls[0][0] as CustomEvent).detail).toEqual({ section: 'api-keys' });
    window.removeEventListener('kasal:navigate-config', navigate);
  });

  it('enables using a key already configured in the API key service', async () => {
    vi.mocked(DecisionConfigService.getConfig).mockResolvedValue({
      enabled: false, api_key_configured: true,
    });
    vi.mocked(DecisionConfigService.saveConfig).mockResolvedValue({
      enabled: true, api_key_configured: true,
    });
    render(<DecisionModelConfiguration />);
    const toggle = screen.getByRole('checkbox', TOGGLE);
    await waitFor(() => expect(toggle).toBeEnabled());
    expect(screen.getByText('Uses JEV_API_KEY from Configuration → API Keys.')).toBeInTheDocument();
    fireEvent.click(toggle);
    await screen.findByText('Decision model settings saved.');
    expect(DecisionConfigService.saveConfig).toHaveBeenCalledWith(true);
  });

  it('disables without overwriting the stored key', async () => {
    vi.mocked(DecisionConfigService.getConfig).mockResolvedValue({
      enabled: true, api_key_configured: true,
    });
    vi.mocked(DecisionConfigService.saveConfig).mockResolvedValue({
      enabled: false, api_key_configured: true,
    });
    render(<DecisionModelConfiguration />);
    const toggle = screen.getByRole('checkbox', TOGGLE);
    await waitFor(() => expect(toggle).toBeChecked());
    fireEvent.click(toggle);
    await screen.findByText('Decision model settings saved.');
    expect(DecisionConfigService.saveConfig).toHaveBeenCalledWith(false);
  });

  it('reverts the toggle and shows the fallback when the server gives no reason', async () => {
    vi.mocked(DecisionConfigService.getConfig).mockResolvedValue({
      enabled: false, api_key_configured: true,
    });
    vi.mocked(DecisionConfigService.saveConfig).mockRejectedValue(new Error('denied'));
    render(<DecisionModelConfiguration />);
    const toggle = screen.getByRole('checkbox', TOGGLE);
    await waitFor(() => expect(toggle).toBeEnabled());
    fireEvent.click(toggle);
    await screen.findByText(/Could not save decision model settings/);
    expect(screen.queryByText('Decision model settings saved.')).not.toBeInTheDocument();
    expect(toggle).not.toBeChecked();
  });

  it('shows the server reason when a save is refused', async () => {
    vi.mocked(DecisionConfigService.getConfig).mockResolvedValue({
      enabled: false, api_key_configured: true,
    });
    const detail = 'No decision model is available on this deployment: a system admin must set the Jev API URL in System administration → Models';
    vi.mocked(DecisionConfigService.saveConfig).mockRejectedValue({ response: { status: 400, data: { detail } } });
    render(<DecisionModelConfiguration />);
    const toggle = screen.getByRole('checkbox', TOGGLE);
    await waitFor(() => expect(toggle).toBeEnabled());
    fireEvent.click(toggle);
    expect(await screen.findByText(detail)).toBeInTheDocument();
    expect(screen.queryByText(/A workspace admin and an API key are required/)).not.toBeInTheDocument();
    expect(toggle).not.toBeChecked();
  });

  it('shows the server reason when loading fails', async () => {
    vi.mocked(DecisionConfigService.getConfig).mockRejectedValue({
      response: { status: 400, data: { detail: 'A workspace is required' } },
    });
    render(<DecisionModelConfiguration />);
    expect(await screen.findByText('A workspace is required')).toBeInTheDocument();
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
    const first = render(<DecisionModelConfiguration />);
    const toggle = screen.getByRole('checkbox', TOGGLE);
    await waitFor(() => expect(toggle).toBeEnabled());
    fireEvent.click(toggle);
    await screen.findByText('Decision model settings saved.');
    first.unmount();
    render(<DecisionModelConfiguration />);
    await waitFor(() => expect(screen.getByRole('checkbox', TOGGLE)).toBeChecked());
    expect(DecisionConfigService.saveConfig).toHaveBeenCalledTimes(1);
  });
});
