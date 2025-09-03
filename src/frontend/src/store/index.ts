// This file is kept as a placeholder for any future Redux stores
// Currently all workflow state has been migrated to Zustand

import { configureStore } from '@reduxjs/toolkit';

// Create an empty reducer to maintain the store structure
// This allows us to keep Redux Provider in the app for future Redux slices if needed
const emptyReducer = () => ({});

export const store = configureStore({
  reducer: {
    // Add any future Redux slices here
    empty: emptyReducer,
  },
});

// Infer the `RootState` and `AppDispatch` types from the store itself
export type RootState = ReturnType<typeof store.getState>;
export type AppDispatch = typeof store.dispatch;

// Export all Zustand stores for centralized access
export * from './hooks';
export * from './nodeActions';
export * from './shortcuts';
export * from './flowConfig'; 