import { describe, it, expect } from 'vitest';
import { edgeColors, getEdgeStyle, normalizeEdgeColor } from './edgeConfig';
describe('Kasal connection colors', () => {
  it('renders old default blue edges in the neutral palette', () => {
    const style = getEdgeStyle('agent-1', 'task-1', false, { stroke: '#2196f3', strokeWidth: 3 });
    expect(style.stroke).toBe(edgeColors.primary);
    expect(style.strokeWidth).toBe(3);
    expect(normalizeEdgeColor('#1565C0')).toBe(edgeColors.primary);
  });
  it('preserves meaningful custom connection colors', () => {
    expect(getEdgeStyle('agent-1', 'task-1', false, { stroke: '#ff9800' }).stroke).toBe('#ff9800');
  });
});
