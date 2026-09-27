import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import DecisionModelSystemSettings from './DecisionModelSystemSettings';
import { EngineConfigService } from '../../../../api/config/EngineConfigService';
import type { EngineSettings } from '../../../../types/config/engines';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({
    t: (_key: string, options: Record<string, string>) =>
      (options.defaultValue ?? _key).replace(/{{(\w+)}}/g, (_m, name: string) => options[name] ?? ''),
  }),
}));
const { notifyModelsChanged } = vi.hoisted(() => ({ notifyModelsChanged: vi.fn(async () => undefined) }));
vi.mock('../../../../store/models', () => ({ notifyModelsChanged }));
vi.mock('../../../../api/config/EngineConfigService', () => ({
  EngineConfigService: { getSettings: vi.fn(), updateSettings: vi.fn() },
}));

function settings(overrides: Partial<EngineSettings> = {}): EngineSettings {
  return {
    jev_api_base: null,
    agent_max_execution_time: 900,
    agent_max_execution_time_default: 900,
    budgets: {},
    budget_defaults: {},
    ...overrides,
  } as EngineSettings;
}

describe('DecisionModelSystemSettings', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(EngineConfigService.getSettings).mockResolvedValue(settings());
  });

  it('renders nothing when the viewer may not read system settings', async () => {
    vi.mocked(EngineConfigService.getSettings).mockRejectedValue({ response: { status: 403 } });
    const { container } = render(<DecisionModelSystemSettings />);
    await waitFor(() => expect(EngineConfigService.getSettings).toHaveBeenCalled());
    expect(container).toBeEmptyDOMElement();
  });

  it('offers the two connections, Jev API by default', async () => {
    render(<DecisionModelSystemSettings />);
    expect(await screen.findByText('Decision model')).toBeInTheDocument();
    expect(screen.getByRole('radio', { name: /Jev API: Jev chooses among your enabled models/ })).toBeChecked();
    expect(screen.getByRole('radio', { name: /OpenRouter: Requests go to Jev Router/ })).not.toBeChecked();
    expect(screen.getByText(/workspace's own JEV_API_KEY/)).toBeInTheDocument();
  });

  it('saves the OpenRouter connection with its URL and names OPENROUTER_API_KEY', async () => {
    vi.mocked(EngineConfigService.updateSettings).mockResolvedValue(
      settings({ decision_connection: 'openrouter', openrouter_api_base: 'https://openrouter.ai/api/v1' }),
    );
    render(<DecisionModelSystemSettings />);
    fireEvent.click(await screen.findByRole('radio', { name: /OpenRouter/ }));
    expect(screen.getByText(/workspace's own OPENROUTER_API_KEY/)).toBeInTheDocument();
    const field = screen.getByLabelText('OpenRouter API URL');
    fireEvent.change(field, { target: { value: 'https://openrouter.ai/api/v1' } });
    fireEvent.click(screen.getByRole('button', { name: 'Save' }));
    await waitFor(() =>
      expect(EngineConfigService.updateSettings).toHaveBeenCalledWith({
        decision_connection: 'openrouter',
        jev_api_base: null,
        openrouter_api_base: 'https://openrouter.ai/api/v1',
      }),
    );
    expect(await screen.findByText('Decision model connection saved.')).toBeInTheDocument();
    // The connection gates Auto: open chats and the live model store refresh.
    expect(notifyModelsChanged).toHaveBeenCalledTimes(1);
  });

  it('shows a migrated OpenRouter URL as the OpenRouter connection', async () => {
    vi.mocked(EngineConfigService.getSettings).mockResolvedValue(
      settings({ decision_connection: 'openrouter', openrouter_api_base: 'https://openrouter.ai/api/v1' }),
    );
    render(<DecisionModelSystemSettings />);
    expect(await screen.findByRole('radio', { name: /OpenRouter/ })).toBeChecked();
    expect(screen.getByLabelText('OpenRouter API URL')).toHaveValue('https://openrouter.ai/api/v1');
    // Nothing changed yet.
    expect(screen.getByRole('button', { name: 'Save' })).toBeDisabled();
  });

  it('accepts a plain http endpoint and warns that it is unencrypted', async () => {
    vi.mocked(EngineConfigService.updateSettings).mockResolvedValue(
      settings({ jev_api_base: 'http://jev.internal:8080' }),
    );
    render(<DecisionModelSystemSettings />);
    const field = await screen.findByLabelText('Jev API URL');

    fireEvent.change(field, { target: { value: 'http://jev.internal:8080' } });
    expect(screen.getByText(/Plain http is not encrypted/)).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Save' }));
    await waitFor(() =>
      expect(EngineConfigService.updateSettings).toHaveBeenCalledWith({
        decision_connection: 'jev',
        jev_api_base: 'http://jev.internal:8080',
        openrouter_api_base: null,
      }),
    );
  });

  it('saves the Jev API URL to the existing jev_api_base setting and refuses non-http addresses', async () => {
    vi.mocked(EngineConfigService.updateSettings).mockResolvedValue(
      settings({ jev_api_base: 'https://jev.example.com' }),
    );
    render(<DecisionModelSystemSettings />);
    const field = await screen.findByLabelText('Jev API URL');

    fireEvent.change(field, { target: { value: 'jev.example.com' } });
    expect(screen.getByText('Must start with http:// or https://')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Save' })).toBeDisabled();

    fireEvent.change(field, { target: { value: ' https://jev.example.com ' } });
    fireEvent.click(screen.getByRole('button', { name: 'Save' }));
    await waitFor(() =>
      expect(EngineConfigService.updateSettings).toHaveBeenCalledWith({
        decision_connection: 'jev',
        jev_api_base: 'https://jev.example.com',
        openrouter_api_base: null,
      }),
    );
    expect(await screen.findByText('Decision model connection saved.')).toBeInTheDocument();
    // The URL gates Auto in every workspace: open chats and tabs refresh.
    expect(notifyModelsChanged).toHaveBeenCalledTimes(1);
  });

  it('clears the URL by sending null', async () => {
    vi.mocked(EngineConfigService.getSettings).mockResolvedValue(
      settings({ jev_api_base: 'https://jev.example.com' }),
    );
    vi.mocked(EngineConfigService.updateSettings).mockResolvedValue(settings());
    render(<DecisionModelSystemSettings />);
    const field = await screen.findByLabelText('Jev API URL');
    expect(field).toHaveValue('https://jev.example.com');
    fireEvent.change(field, { target: { value: '' } });
    fireEvent.click(screen.getByRole('button', { name: 'Save' }));
    await waitFor(() =>
      expect(EngineConfigService.updateSettings).toHaveBeenCalledWith({
        decision_connection: 'jev',
        jev_api_base: null,
        openrouter_api_base: null,
      }),
    );
  });

  it('shows the server reason when a save is refused', async () => {
    vi.mocked(EngineConfigService.updateSettings).mockRejectedValue({
      response: { data: { detail: 'The Jev API URL must use https://' } },
    });
    render(<DecisionModelSystemSettings />);
    fireEvent.change(await screen.findByLabelText('Jev API URL'), {
      target: { value: 'https://jev.example.com' },
    });
    fireEvent.click(screen.getByRole('button', { name: 'Save' }));
    expect(await screen.findByText('The Jev API URL must use https://')).toBeInTheDocument();
    expect(notifyModelsChanged).not.toHaveBeenCalled();
  });
});
