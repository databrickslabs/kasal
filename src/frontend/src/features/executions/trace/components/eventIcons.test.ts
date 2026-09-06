import { isEventClickable } from '../lib/traceEventProcessors';
import { describe, it, expect } from 'vitest';
import { getEventIcon, ICON_CONFIG } from './eventIcons';

describe('getEventIcon', () => {
  it('returns correct icon for memory_context', () => {
    const icon = getEventIcon('memory_context');
    expect(icon.Component).not.toBeNull();
    expect(icon.color).toBe('info');
  });

  it('returns correct icon for memory_write', () => {
    const icon = getEventIcon('memory_write');
    expect(icon.Component).not.toBeNull();
    expect(icon.color).toBe('primary');
  });

  it('returns correct icon for memory_retrieval', () => {
    const icon = getEventIcon('memory_retrieval');
    expect(icon.Component).not.toBeNull();
    expect(icon.color).toBe('success');
  });

  it('returns null Component for unknown type', () => {
    const icon = getEventIcon('unknown_type_xyz');
    expect(icon.Component).toBeNull();
    expect(icon.color).toBe('inherit');
  });
});

describe('ICON_CONFIG', () => {
  it('has memory_context entry', () => {
    expect(ICON_CONFIG['memory_context']).toBeDefined();
    expect(ICON_CONFIG['memory_context'].color).toBe('info');
  });

  it('has memory_write entry', () => {
    expect(ICON_CONFIG['memory_write']).toBeDefined();
  });

  it('has memory_retrieval entry', () => {
    expect(ICON_CONFIG['memory_retrieval']).toBeDefined();
  });

  it('has memory_backend_error entry', () => {
    expect(ICON_CONFIG['memory_backend_error']).toBeDefined();
    expect(ICON_CONFIG['memory_backend_error'].color).toBe('error');
  });
});

describe('plan row presentation', () => {
  it('opens the checklist — the row is clickable and has its own icon', () => {
    expect(isEventClickable('plan_updated', true)).toBe(true);
    expect(getEventIcon('plan_updated').Component).not.toBeNull();
  });
});
