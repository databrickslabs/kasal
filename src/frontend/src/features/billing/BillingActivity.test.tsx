import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, expect, it, vi } from 'vitest';
import BillingActivity from './BillingActivity';
import { BillingService } from './BillingService';
import type { BillingSummary, UsageTotals } from './types';

vi.mock('./BillingService', () => ({ BillingService: { summary: vi.fn(), saveRate: vi.fn() } }));
const totals: UsageTotals = { calls: 3, measured_calls: 2, priced_calls: 1, input_tokens: 200, output_tokens: 100, cached_input_tokens: 0, total_tokens: 300, estimated_cost_usd: 0.005 };
const data: BillingSummary = {
  start: '', end: '', totals, models: [{ ...totals, id: 'example-model', label: 'example-model', execution_type: null }],
  runs: [{ ...totals, id: 'run-one', label: 'Research run', execution_type: 'crew' }], days: [], rates: [], can_manage_rates: true, currency: 'USD', basis: 'Recorded calls at current rates.',
};
beforeEach(() => { vi.clearAllMocks(); vi.mocked(BillingService.summary).mockResolvedValue(data); });

it('shows coverage and lets the user compare model and run costs', async () => {
  render(<BillingActivity executionIds={['run-one']} />);
  expect(await screen.findByText('example-model')).toBeVisible();
  expect(screen.getByText('1 of 3 calls priced')).toBeVisible();
  expect(BillingService.summary).toHaveBeenCalledWith(expect.any(String), expect.any(String), ['run-one'], expect.any(AbortSignal));
  fireEvent.click(screen.getByRole('button', { name: 'By run' }));
  expect(screen.getByText('Research run')).toBeVisible();
  expect(screen.getByText('Agent Builder')).toBeVisible();
});

it('keeps an empty session scoped and does not show unknown costs as zero', async () => {
  vi.mocked(BillingService.summary).mockResolvedValue({ ...data, totals: { ...totals, priced_calls: 0, estimated_cost_usd: null }, models: [], runs: [], can_manage_rates: false });
  render(<BillingActivity executionIds={[]} />);
  expect(await screen.findByText('Unpriced')).toBeVisible();
  expect(BillingService.summary).toHaveBeenCalledWith(expect.any(String), expect.any(String), [], expect.any(AbortSignal));
  expect(screen.queryByRole('button', { name: 'Model rates' })).not.toBeInTheDocument();
});

it('recovers from a failed load with Retry', async () => {
  vi.mocked(BillingService.summary).mockRejectedValueOnce(new Error('offline'));
  render(<BillingActivity />);
  fireEvent.click(await screen.findByRole('button', { name: 'Retry' }));
  expect(await screen.findByText('example-model')).toBeVisible();
});

it('requires explicit valid input and output rates before saving', async () => {
  render(<BillingActivity />);
  fireEvent.click(await screen.findByRole('button', { name: 'Model rates' }));
  expect(screen.getByRole('button', { name: 'Save rate' })).toBeDisabled();
  fireEvent.change(screen.getByRole('spinbutton', { name: 'Input / 1M tokens' }), { target: { value: '2.5' } });
  fireEvent.change(screen.getByRole('spinbutton', { name: 'Output / 1M tokens' }), { target: { value: '4' } });
  fireEvent.click(screen.getByRole('button', { name: 'Save rate' }));
  await waitFor(() => expect(BillingService.saveRate).toHaveBeenCalledWith({ model: 'example-model', input_per_million: '2.5', output_per_million: '4', cached_input_per_million: null }));
});
