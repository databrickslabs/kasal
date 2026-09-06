
// Add a function at the top level to intercept ResizeObserver errors globally
export const setupResizeObserverErrorHandling = (): void => {
  if (typeof window !== 'undefined' && !Object.prototype.hasOwnProperty.call(window, '_resizeObserverHandled')) {
    // Set a flag to prevent multiple handlers
    Object.defineProperty(window, '_resizeObserverHandled', {
      value: true,
      writable: false,
      configurable: false
    });

    // Add global error event handler
    window.addEventListener('error', (event) => {
      if (event.message && 
          (event.message.includes('ResizeObserver loop') || 
           event.message.includes('ResizeObserver Loop'))) {
        event.stopImmediatePropagation();
        event.preventDefault();
        return false;
      }
    });

    // Add unhandled rejection handler
    window.addEventListener('unhandledrejection', (event) => {
      if (event.reason && 
          typeof event.reason.message === 'string' && 
          (event.reason.message.includes('ResizeObserver loop') || 
           event.reason.message.includes('ResizeObserver Loop'))) {
        event.preventDefault();
        return false;
      }
    });

    // Replace console.error to filter out ResizeObserver errors
    const originalConsoleError = console.error;
    console.error = function(...args) {
      if (args.length > 0 && 
          typeof args[0] === 'string' && 
          (args[0].includes('ResizeObserver loop') || 
           args[0].includes('ResizeObserver Loop'))) {
        // Suppress this error
        return;
      }
      originalConsoleError.apply(console, args);
    };
  }
};
