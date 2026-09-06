import { fireEvent, render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import EffortPicker, { effortChoices } from './EffortPicker';
import { apiClient } from '../api/client';

vi.mock('../api/client', () => ({ apiClient: { get: vi.fn() } }));
const profiles = Object.fromEntries(['none', 'minimal', 'low', 'medium', 'high', 'xhigh', 'max'].map(tier => [tier, {
  max_iter: 15, max_execution_time: 300, run_max_seconds: 600, max_output_tokens: 16384,
}]));
beforeEach(() => vi.mocked(apiClient.get).mockResolvedValue({ data: { effort_profiles: profiles } }));

describe('Effort picker', () => {
  it('offers only the selected endpoint’s native tiers', async () => {
    const change = vi.fn();
    render(<EffortPicker models={[{ allowed_efforts: ['low', 'high', 'xhigh'] }]}
      value={{ tier: 'high' }} onChange={change} />);
    expect(await screen.findAllByRole('menuitemradio')).toHaveLength(3);
    expect(screen.queryByRole('menuitemradio', { name: /^Max/ })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('menuitemradio', { name: /^Extra high/ }));
    expect(change).toHaveBeenCalledWith({ tier: 'xhigh' }, profiles.xhigh);
  });

  it('keeps runtime limits available on models without reasoning', async () => {
    render(<EffortPicker models={[{ name: 'Custom model' }]} value={{ tier: 'medium' }} onChange={vi.fn()} />);
    expect(await screen.findAllByRole('menuitemradio')).toHaveLength(3);
    expect(screen.getByText(/no native effort control/)).toBeVisible();
  });

  it('validates custom limits and preserves reasoning depth', async () => {
    const change = vi.fn();
    render(<EffortPicker models={[]} value={{ tier: 'high' }} onChange={change} />);
    fireEvent.click(await screen.findByRole('button', { name: 'Adjust limits' }));
    fireEvent.change(screen.getByLabelText('Whole run · minutes'), { target: { value: '0' } });
    expect(screen.getByRole('button', { name: 'Apply custom limits' })).toBeDisabled();
    fireEvent.change(screen.getByLabelText('Whole run · minutes'), { target: { value: '5' } });
    fireEvent.change(screen.getByLabelText('Rounds per agent turn'), { target: { value: '12' } });
    fireEvent.click(screen.getByRole('button', { name: 'Apply custom limits' }));
    expect(change).toHaveBeenCalledWith({ tier: 'high', run_max_seconds: 300, max_execution_time: 300, max_iter: 12 });
  });

  it('does not invent native effort support in a mixed crew', () => {
    expect(effortChoices([{ allowed_efforts: ['low', 'high'] }, { allowed_efforts: ['low', 'medium', 'high', 'max'] }]))
      .toEqual(['low', 'medium', 'high', 'max']);
    expect(effortChoices([{ thinking_mode: 'manual' }])).toEqual(['low', 'medium', 'high']);
  });

  it('provides a retry when the server cannot provide limits', async () => {
    vi.mocked(apiClient.get).mockRejectedValueOnce(new Error('offline'));
    render(<EffortPicker models={[]} value={{ tier: 'medium' }} onChange={vi.fn()} />);
    fireEvent.click(await screen.findByRole('button', { name: 'Retry' }));
    expect(await screen.findAllByRole('menuitemradio')).toHaveLength(3);
  });
});
