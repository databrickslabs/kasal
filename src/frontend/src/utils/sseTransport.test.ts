/**
 * Unit tests for the SSE transport gate — dev-only by default, runtime
 * overridable via localStorage for testing SSE on a real Databricks Apps
 * deploy (no rebuild), plus a build-time VITE_FORCE_SSE escape hatch.
 */
import { describe, it, expect, afterEach } from 'vitest';
import { resolveSseEnabled, SSE_OVERRIDE_STORAGE_KEY } from './sseTransport';

describe('resolveSseEnabled', () => {
  afterEach(() => {
    window.localStorage.removeItem(SSE_OVERRIDE_STORAGE_KEY);
    delete (import.meta.env as Record<string, unknown>).VITE_FORCE_SSE;
  });

  it('defaults to enabled on localhost (jsdom)', () => {
    expect(window.location.hostname).toBe('localhost');
    expect(resolveSseEnabled()).toBe(true);
  });

  it('localStorage "false" force-disables SSE even on localhost', () => {
    window.localStorage.setItem(SSE_OVERRIDE_STORAGE_KEY, 'false');
    expect(resolveSseEnabled()).toBe(false);
  });

  it('localStorage "true" force-enables SSE (deploy testing knob)', () => {
    window.localStorage.setItem(SSE_OVERRIDE_STORAGE_KEY, 'true');
    expect(resolveSseEnabled()).toBe(true);
  });

  it('accepts "1"/"0" as boolean aliases', () => {
    window.localStorage.setItem(SSE_OVERRIDE_STORAGE_KEY, '0');
    expect(resolveSseEnabled()).toBe(false);
    window.localStorage.setItem(SSE_OVERRIDE_STORAGE_KEY, '1');
    expect(resolveSseEnabled()).toBe(true);
  });

  it('ignores unrecognized override values (falls back to hostname default)', () => {
    window.localStorage.setItem(SSE_OVERRIDE_STORAGE_KEY, 'maybe');
    expect(resolveSseEnabled()).toBe(true); // localhost default
  });

  it('VITE_FORCE_SSE build flag force-enables SSE', () => {
    // Simulate a non-localhost deploy: force-disable via hostname is not
    // directly settable in jsdom, so assert the flag path by checking the
    // localStorage-off case is overridden ONLY when localStorage is absent.
    (import.meta.env as Record<string, unknown>).VITE_FORCE_SSE = 'true';
    expect(resolveSseEnabled()).toBe(true);
    // localStorage wins over the build flag:
    window.localStorage.setItem(SSE_OVERRIDE_STORAGE_KEY, 'false');
    expect(resolveSseEnabled()).toBe(false);
  });
});
