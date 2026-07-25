import { describe, it, expect } from 'vitest';
import { formatDurationMs } from './formatDuration';

describe('formatDurationMs', () => {
  it('renders sub-millisecond values as <1 ms', () => {
    expect(formatDurationMs(0.4)).toBe('<1 ms');
  });

  it('renders milliseconds as integers', () => {
    expect(formatDurationMs(3.510874987114221)).toBe('4 ms');
    expect(formatDurationMs(342)).toBe('342 ms');
    expect(formatDurationMs(999)).toBe('999 ms');
  });

  it('renders seconds with one decimal', () => {
    expect(formatDurationMs(1000)).toBe('1.0s');
    expect(formatDurationMs(7049)).toBe('7.0s');
    expect(formatDurationMs(11840)).toBe('11.8s');
  });

  it('renders minutes above 60s', () => {
    expect(formatDurationMs(150000)).toBe('2.5m');
  });
});
