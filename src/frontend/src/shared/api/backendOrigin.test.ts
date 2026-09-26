import { describe, it, expect } from 'vitest';
import { DEFAULT_BACKEND_PORT, devBackendOrigin, devBackendPort } from './backendOrigin';

describe('backendOrigin', () => {
  it('defaults to port 8000, the run.sh default', () => {
    expect(DEFAULT_BACKEND_PORT).toBe('8000');
    expect(devBackendPort({})).toBe('8000');
    expect(devBackendOrigin({})).toBe('http://localhost:8000');
  });

  it('uses VITE_KASAL_PORT when it is a valid port', () => {
    expect(devBackendPort({ VITE_KASAL_PORT: '8001' })).toBe('8001');
    expect(devBackendOrigin({ VITE_KASAL_PORT: ' 9000 ' })).toBe('http://localhost:9000');
  });

  it.each(['', 'abc', '0', '70000', '80a0', '-1'])('falls back to the default for %j', (value) => {
    expect(devBackendPort({ VITE_KASAL_PORT: value })).toBe('8000');
  });

  it('reads import.meta.env when no env is passed', () => {
    // vitest does not run vite.config.ts, so nothing sets VITE_KASAL_PORT.
    expect(devBackendOrigin()).toBe('http://localhost:8000');
  });
});
