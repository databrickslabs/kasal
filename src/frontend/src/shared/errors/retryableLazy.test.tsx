import React, { Suspense } from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import { ErrorBoundary } from './ErrorBoundary';
import { retryableLazy } from './retryableLazy';

const CHUNK_ERROR = new TypeError('Importing a module script failed.');

describe('retryableLazy', () => {
  let consoleError: ReturnType<typeof vi.spyOn>;
  beforeEach(() => {
    consoleError = vi.spyOn(console, 'error').mockImplementation(() => undefined);
  });
  afterEach(() => consoleError.mockRestore());

  it('loads the component and passes props through', async () => {
    const Greeting = retryableLazy(async () => ({
      default: ({ name }: { name: string }) => <span>hello {name}</span>,
    }));
    render(
      <Suspense fallback={null}>
        <Greeting name="kasal" />
      </Suspense>,
    );
    expect(await screen.findByText('hello kasal')).toBeInTheDocument();
  });

  it('calls the factory again on the render after a failed import', async () => {
    // Fails until the chunk "comes back" (React itself retries a failed
    // render once, so a single rejection would be swallowed by that retry).
    let available = false;
    const factory = vi.fn(async () => {
      if (!available) throw CHUNK_ERROR;
      return { default: () => <span>loaded</span> };
    });
    const Dialog = retryableLazy(factory);

    const tree = (key: number) => (
      <ErrorBoundary variant="section" resetKeys={[key]}>
        <Suspense fallback={null}>
          <Dialog />
        </Suspense>
      </ErrorBoundary>
    );
    const { rerender } = render(tree(1));
    expect(await screen.findByText('Kasal has been updated')).toBeInTheDocument();

    const failedCalls = factory.mock.calls.length;
    available = true;
    rerender(tree(2));
    expect(await screen.findByText('loaded')).toBeInTheDocument();
    expect(factory).toHaveBeenCalledTimes(failedCalls + 1);
  });

  it('does not re-import after a successful load', async () => {
    const factory = vi.fn(async () => ({ default: () => <span>once</span> }));
    const Once = retryableLazy(factory);
    const { rerender } = render(<Suspense fallback={null}><Once /></Suspense>);
    await screen.findByText('once');
    rerender(<Suspense fallback={null}><Once key="again" /></Suspense>);
    await screen.findByText('once');
    expect(factory).toHaveBeenCalledTimes(1);
  });
});
