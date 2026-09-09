export interface ModelRate {
  model: string;
  input_per_million: number | string;
  output_per_million: number | string;
  cached_input_per_million: number | string | null;
}

export interface UsageTotals {
  calls: number;
  measured_calls: number;
  priced_calls: number;
  input_tokens: number;
  output_tokens: number;
  cached_input_tokens: number;
  total_tokens: number;
  estimated_cost_usd: number | null;
}

export interface UsageBreakdown extends UsageTotals {
  id: string;
  label: string;
  execution_type: string | null;
}

export interface BillingSummary {
  start: string;
  end: string;
  totals: UsageTotals;
  models: UsageBreakdown[];
  runs: UsageBreakdown[];
  days: UsageBreakdown[];
  rates: ModelRate[];
  can_manage_rates: boolean;
  currency: string;
  basis: string;
}

export const costLabel = (value: number | null) => value === null ? 'Unpriced' : new Intl.NumberFormat(undefined, {
  style: 'currency', currency: 'USD', minimumFractionDigits: 2, maximumFractionDigits: value > 0 && value < 0.01 ? 6 : 4,
}).format(value);

