import { useCallback, useEffect, useRef } from 'react';

/**
 * A `setTimeout` whose pending callbacks are cancelled when the component
 * unmounts.
 *
 * A bare `setTimeout` in a component outlives it. In the browser that means a
 * callback running against an unmounted component; under Vitest it means a
 * callback running after the test file's jsdom was torn down, which throws
 * `ReferenceError: window is not defined` and fails the whole run even though
 * every test passed (this happened on CI for SaveFlow).
 *
 * The returned `schedule` function is stable across renders, so it can be used
 * inside effects and event handlers without re-subscribing them. It returns the
 * timer id, which can still be passed to `clearTimeout` for an early cancel.
 */
export function useUnmountSafeTimeout(): (callback: () => void, delayMs: number) => ReturnType<typeof setTimeout> {
  const pending = useRef(new Set<ReturnType<typeof setTimeout>>());

  useEffect(() => {
    const timers = pending.current;
    return () => {
      timers.forEach(id => clearTimeout(id));
      timers.clear();
    };
  }, []);

  return useCallback((callback: () => void, delayMs: number) => {
    const id = setTimeout(() => {
      pending.current.delete(id);
      callback();
    }, delayMs);
    pending.current.add(id);
    return id;
  }, []);
}
