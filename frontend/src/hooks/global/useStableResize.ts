import { useRef, useEffect } from 'react';

/**
 * Custom hook to safely use ResizeObserver without triggering loop errors
 * 
 * @param callback Function to call when resize is detected
 * @param element Reference to the element to observe
 * @param debounceTime Debounce time in milliseconds
 * @returns Cleanup function
 */
export const useStableResize = (
  callback: (entry: ResizeObserverEntry) => void,
  element: React.RefObject<HTMLElement>,
  debounceTime = 100
): void => {
  const timeoutRef = useRef<number | null>(null);
  const observerRef = useRef<ResizeObserver | null>(null);
  const callbackRef = useRef(callback);

  // Update the callback ref if the callback changes
  useEffect(() => {
    callbackRef.current = callback;
  }, [callback]);

  useEffect(() => {
    const currentElement = element.current;
    if (!currentElement) return;

    const handleResize = (entries: ResizeObserverEntry[]) => {
      // Clear any existing timeout
      if (timeoutRef.current) {
        window.clearTimeout(timeoutRef.current);
      }

      // Set a debounced timeout to handle the resize
      timeoutRef.current = window.setTimeout(() => {
        // Call the callback with the most recent entry
        callbackRef.current(entries[0]);
      }, debounceTime);
    };

    // Create a new ResizeObserver
    observerRef.current = new ResizeObserver(handleResize);
    observerRef.current.observe(currentElement);

    // Cleanup function
    return () => {
      if (timeoutRef.current) {
        window.clearTimeout(timeoutRef.current);
      }
      
      if (observerRef.current) {
        observerRef.current.disconnect();
        observerRef.current = null;
      }
    };
  }, [element, debounceTime]);
};

export default useStableResize; 