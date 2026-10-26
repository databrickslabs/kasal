import React, { createContext, lazy, useContext } from 'react';

/**
 * How many times the nearest `ErrorBoundary` has been reset. The boundary
 * provides it; `retryableLazy` reads it to tell a deliberate retry (the
 * boundary was reset) from React re-rendering the same failed attempt.
 */
export const LazyRetryGeneration = createContext(0);

/**
 * `React.lazy`, but a failed import can be retried.
 *
 * `React.lazy` caches the promise its factory returned, INCLUDING a rejection:
 * once a chunk fails to load, every later render of that component throws the
 * same error again without calling the factory. An error boundary that resets
 * and re-renders the child therefore just catches the cached error again.
 *
 * This wrapper keeps the failed `lazy()` while the boundary's reset generation
 * is unchanged (so the error still reaches the boundary, which shows its
 * fallback), and swaps in a fresh one once the boundary has been reset (a
 * retry, a dialog reopened, a route changed), so that render really re-imports
 * the chunk. Swapping straight away on rejection would not work: React retries
 * a failed render itself, and a new pending import every time means the error
 * never surfaces and the tree suspends forever.
 */
export function retryableLazy<P extends object>(
  factory: () => Promise<{ default: React.ComponentType<P> }>,
): React.FC<P> {
  const fresh = (generation: number) => {
    const entry = {
      component: lazy(() =>
        factory().catch((error: unknown) => {
          entry.failed = true;
          throw error;
        }),
      ),
      generation,
      failed: false,
    };
    return entry;
  };
  let current = fresh(0);

  const Retryable: React.FC<P> = (props) => {
    const generation = useContext(LazyRetryGeneration);
    if (current.failed && current.generation !== generation) {
      current = fresh(generation);
    } else if (!current.failed) {
      current.generation = generation;
    }
    // A lazy component renders like the component it wraps; the cast only
    // bridges `LazyExoticComponent`'s ref-aware props type.
    const Component = current.component as unknown as React.ComponentType<P>;
    return React.createElement(Component, props);
  };
  Retryable.displayName = 'RetryableLazy';
  return Retryable;
}
