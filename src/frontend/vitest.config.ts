import { defineConfig } from 'vitest/config';
import react from '@vitejs/plugin-react';
import path from 'path';

export default defineConfig({
  plugins: [react()],
  test: {
    globals: true,
    environment: 'jsdom',
    setupFiles: ['./src/setupTests.ts'],
    include: ['src/**/*.test.{ts,tsx,js,jsx}'],
    exclude: ['node_modules', 'dist'],
    css: true,
    testTimeout: 10000,
    coverage: {
      provider: 'v8',
      reporter: ['text', 'json', 'html'],
      exclude: [
        'node_modules/',
        'src/setupTests.ts',
        '**/*.d.ts',
        '**/*.test.{ts,tsx}',
        '**/index.ts',
        // type-only modules compile to no runtime code
        'src/components/ChatMode/types/**',
        // pure CSS/asset module
        'src/components/ChatMode/chat.css',
      ],
      // Enforced coverage floors for the app-modes / Chat workspace work.
      // The whole embedded Chat tree and the new mode-switcher state must stay
      // fully covered. ChatWorkspace.tsx is a ~1080-line orchestrator: its lines,
      // statements and functions are (near-)fully covered, but a handful of
      // defensive `||`/`?.` fallback branches are unreachable without contrived
      // dead inputs, so its branch floor is set to the achieved level rather
      // than 100. Thresholds are checked when coverage runs (e.g. `vitest run
      // --coverage`); a regression below these floors fails the run.
      thresholds: {
        'src/components/ChatMode/{api,utils,db,hooks,store,components}/**/*.{ts,tsx}': {
          statements: 100,
          branches: 100,
          functions: 100,
          lines: 100,
        },
        'src/components/ChatMode/ChatModeHeaderSlot.tsx': {
          statements: 100,
          branches: 100,
          functions: 100,
          lines: 100,
        },
        'src/components/ChatMode/ChatWorkspace.tsx': {
          statements: 100,
          branches: 100,
          functions: 100,
          lines: 100,
        },
        'src/store/uiLayout.ts': {
          statements: 100,
          branches: 100,
          functions: 100,
          lines: 100,
        },
        'src/components/WorkflowDesigner/ModeSwitcher.tsx': {
          statements: 100,
          branches: 100,
          functions: 100,
          lines: 100,
        },
      },
    },
  },
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
});
