import { load } from 'js-yaml';
import type { Run } from '../../../../types/execution/run';

type Config = Record<string, any>;
export function parseRunConfig(value: unknown): Config {
  try {
    const parsed = typeof value === 'string' ? load(value) : value;
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed as Config : {};
  } catch { return {}; }
}

export function runConfiguration(run: Run): Config {
  return parseRunConfig(run.inputs);
}

export function runEntities(primary: unknown, fallback: unknown): Config {
  const parsed = parseRunConfig(primary);
  return Object.keys(parsed).length ? parsed : parseRunConfig(fallback);
}
