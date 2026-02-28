import { renderHook } from '@testing-library/react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { useResponsiveLayout } from './useResponsiveLayout';

// Mock MUI hooks
vi.mock('@mui/material/styles', () => ({
  useTheme: () => ({
    breakpoints: {
      down: (key: string) => {
        if (key === 'md') return '(max-width:899.95px)';
        if (key === 'sm') return '(max-width:599.95px)';
        return '';
      },
    },
  }),
}));

let matchMediaMatches: Record<string, boolean> = {};

beforeEach(() => {
  matchMediaMatches = {};
  (window.matchMedia as ReturnType<typeof vi.fn>).mockImplementation((query: string) => ({
    matches: matchMediaMatches[query] ?? false,
    media: query,
    onchange: null,
    addListener: vi.fn(),
    removeListener: vi.fn(),
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(),
  }));
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe('useResponsiveLayout', () => {
  it('returns isCompact=false and isMobile=false on desktop (>= 900px)', () => {
    // Default matchMedia returns false for both queries
    const { result } = renderHook(() => useResponsiveLayout());

    expect(result.current.isCompact).toBe(false);
    expect(result.current.isMobile).toBe(false);
  });

  it('returns isCompact=true and isMobile=false on compact screens (600-899px)', () => {
    matchMediaMatches['(max-width:899.95px)'] = true;
    matchMediaMatches['(max-width:599.95px)'] = false;

    const { result } = renderHook(() => useResponsiveLayout());

    expect(result.current.isCompact).toBe(true);
    expect(result.current.isMobile).toBe(false);
  });

  it('returns isCompact=true and isMobile=true on mobile screens (< 600px)', () => {
    matchMediaMatches['(max-width:899.95px)'] = true;
    matchMediaMatches['(max-width:599.95px)'] = true;

    const { result } = renderHook(() => useResponsiveLayout());

    expect(result.current.isCompact).toBe(true);
    expect(result.current.isMobile).toBe(true);
  });
});
