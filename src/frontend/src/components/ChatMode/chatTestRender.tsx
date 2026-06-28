import React from 'react';
import { render, type RenderOptions, type RenderResult } from '@testing-library/react';
import { ThemeProvider } from '@mui/material/styles';
import { createChatTheme } from './chatTheme';

/**
 * Test helper: render a chat-shell component inside the chat MUI ThemeProvider,
 * mirroring how ChatWorkspace wraps the live chat. Chat components read
 * chat-specific tokens via `theme.chat.*`, so they must be rendered within this
 * theme in tests too (the default MUI theme has no `chat` extension).
 *
 * The provider is supplied via RTL's `wrapper` option (not by wrapping `ui`
 * inline) so that `rerender(...)` re-applies it — wrapping inline would drop the
 * provider on rerender and crash any `theme.chat.*` consumer.
 *
 * Not a test file (no `.test.` suffix) — it is imported by tests only.
 */
export function renderWithChatTheme(
  ui: React.ReactElement,
  mode: 'light' | 'dark' = 'light',
  options?: Omit<RenderOptions, 'wrapper'>,
): RenderResult {
  const Wrapper = ({ children }: { children: React.ReactNode }) => (
    <ThemeProvider theme={createChatTheme(mode)}>{children}</ThemeProvider>
  );
  return render(ui, { ...options, wrapper: Wrapper });
}
