import js from '@eslint/js';
import globals from 'globals';
import reactHooks from 'eslint-plugin-react-hooks';
import reactRefresh from 'eslint-plugin-react-refresh';
import tseslint from 'typescript-eslint';

export default tseslint.config(
  { ignores: ['dist', 'build', 'node_modules'] },
  {
    extends: [js.configs.recommended, ...tseslint.configs.recommended],
    files: ['**/*.{ts,tsx}'],
    languageOptions: {
      ecmaVersion: 2020,
      globals: globals.browser,
    },
    plugins: {
      'react-hooks': reactHooks,
      'react-refresh': reactRefresh,
    },
    rules: {
      ...reactHooks.configs.recommended.rules,
      'react-refresh/only-export-components': [
        'warn',
        { allowConstantExport: true },
      ],
      '@typescript-eslint/no-unused-vars': ['warn', { argsIgnorePattern: '^_' }],
      '@typescript-eslint/no-explicit-any': 'warn',
      // Production builds strip console.log/info/debug; use utils/logger for
      // debug output and keep console.warn/error for real problems.
      'no-console': ['warn', { allow: ['warn', 'error'] }],
    },
  },
  {
    // react-markdown only where the shared image policy is applied (checked by
    // src/shared/a2ui/lib/imagePolicy.coverage.test.ts). Kept BEFORE the
    // file-scoped no-restricted-imports blocks below, which replace this rule
    // for their (non-UI) files rather than being replaced by it.
    files: ['src/**/*.{ts,tsx}'],
    ignores: [
      '**/*.test.*',
      'src/shared/a2ui/lib/markdown.tsx',
      'src/shared/a2ui/components/primitives.tsx',
      'src/shared/ui/PaginatedOutput.tsx',
      'src/features/approvals/components/CrewOutputRenderer.tsx',
      'src/features/chat/components/Cards/HelpCard.tsx',
      'src/features/chat/components/Chat/MessageContent.tsx',
      'src/features/chat/components/Preview/StepContent.tsx',
      'src/features/executions/components/ShowResult.tsx',
      'src/features/executions/components/UCMVResultViewer.tsx',
      'src/features/help/documentation/Documentation.tsx',
      'src/features/memory/components/MemoryRecordMarkdown.tsx',
      'src/features/workflow/assistant/components/MessageRenderer.tsx',
      'src/features/workflow/crews/components/DeliverableMarkdown.tsx',
    ],
    rules: {
      'no-restricted-imports': ['error', {
        paths: [{
          name: 'react-markdown',
          message: 'Render markdown through the shared image policy (safeImageComponents / mdComponents / MarkdownImage in shared/a2ui/lib), then add the file to this allowlist.',
        }],
      }],
      'no-restricted-syntax': ['error', { selector: "ImportExpression[source.value='react-markdown']", message: 'Render markdown through the shared image policy; see the react-markdown allowlist in eslint.config.js.' }],
    },
  },
  {
    files: [
      'src/api/**/*.ts',
      'src/types/**/*.ts',
      'src/features/chat/types/**/*.ts',
      'src/features/chat/store/**/*.ts',
      'src/features/chat/utils/preview.ts',
      'src/features/chat/api/**/*.ts',
      'src/features/chat/persistence/**/*.ts',
    ],
    rules: {
      'no-restricted-imports': ['error', {
        patterns: [{
          group: ['**/components/**', '**/PreviewPanel', '**/ChatContainer'],
          message: 'Import contracts from types and parsing from utilities; API, state and contracts must not depend on UI components.',
        }],
      }],
    },
  },
  {
    files: [
      'src/features/executions/trace/lib/**/*.ts',
      'src/features/workflow/canvas/lib/**/*.ts',
      'src/shared/lib/collections.ts',
      'src/shared/api/**/*.ts',
      'src/features/chat/api/**/*.ts',
      'src/features/chat/persistence/**/*.ts',
    ],
    ignores: ['**/*.test.*'],
    rules: {
      'no-restricted-imports': ['error', {
        patterns: [{
          group: ['react', 'react-dom', 'react-dom/*', 'react-hot-toast', '@mui/*', '**/components/**', '**/hooks/**', '**/store/**'],
          message: 'Feature processing and shared helpers must stay independent of UI, React hooks and stores.',
        }],
      }],
    },
  },
  {
    files: ['src/features/chat/utils/skillSelection.ts'],
    rules: {
      'no-restricted-imports': ['error', {
        patterns: [{
          group: ['**/store/**', '**/components/**'],
          message: 'Keep skill lookup and filtering independent of state; coordinate selection in store/skillSelection.',
        }],
      }],
    },
  },
);
