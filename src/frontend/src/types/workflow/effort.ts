export type EffortTier = 'none' | 'minimal' | 'low' | 'medium' | 'high' | 'xhigh' | 'max';
export interface EffortSettings {
  tier: EffortTier;
  max_iter?: number;
  max_execution_time?: number;
  run_max_seconds?: number;
  max_output_tokens?: number;
}
export interface EffortProfile {
  max_iter: number;
  max_execution_time: number;
  run_max_seconds: number;
  max_output_tokens: number;
}
export const DEFAULT_EFFORT: EffortSettings = { tier: 'medium' };
export const EFFORT_LABELS: Record<EffortTier, string> = {
  none: 'None', minimal: 'Minimal', low: 'Low', medium: 'Medium',
  high: 'High', xhigh: 'Extra high', max: 'Max',
};
export function effortLabel(settings: EffortSettings): string {
  const custom = Object.keys(settings).some(key => key !== 'tier');
  return `${EFFORT_LABELS[settings.tier]}${custom ? ' · Custom' : ''}`;
}
