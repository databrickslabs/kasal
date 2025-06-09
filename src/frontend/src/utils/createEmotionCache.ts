import createCache from '@emotion/cache';

// Create a singleton cache configuration to be used throughout the app
const createEmotionCache = () => {
  return createCache({
    key: 'mui-style',
    prepend: true, // This ensures styles are injected first in the head
    // This configuration helps prevent the 'insertBefore' Node error
    // by ensuring consistent style injection
  });
};

export default createEmotionCache; 