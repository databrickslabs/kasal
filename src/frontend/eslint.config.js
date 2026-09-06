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
    },
  },
  {
    files: [
      'src/api/**/*.ts',
      'src/types/**/*.ts',
      'src/components/ChatMode/types/**/*.ts',
      'src/components/ChatMode/store/**/*.ts',
      'src/components/ChatMode/utils/preview.ts',
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
    ],
    ignores: ['**/*.test.*'],
    rules: {
      'no-restricted-imports': ['error', {
        patterns: [{
          group: ['react', 'react-dom', 'react-dom/*', '@mui/*', '**/components/**', '**/hooks/**', '**/store/**'],
          message: 'Feature processing and shared helpers must stay independent of UI, React hooks and stores.',
        }],
      }],
    },
  },
  {
    files: ['src/components/ChatMode/utils/skillSelection.ts'],
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
