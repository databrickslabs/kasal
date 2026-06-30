import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { A2uiSurface } from './A2uiSurface';

// Mock the shared renderer + its theme contexts so this unit test never pulls in
// the heavy ESM render stack (react-markdown / recharts). We only verify the
// A2uiSurface WRAPPER: that it draws the renderer and wires the corner "expand"
// control (the de-MUI'd plain <button> with a lucide Maximize2 icon).
vi.mock('../../../../shared/a2ui', async () => {
  const React = await import('react');
  return {
    A2UIRenderer: ({ payload }: { payload: { surfaceKind?: string } }) => (
      <div data-testid="a2ui-rendered">{payload?.surfaceKind}</div>
    ),
    DeckThemeContext: React.createContext({}),
    SurfaceChromeContext: React.createContext({ downloads: true }),
    getDeckTheme: () => ({ id: 'midnight' }),
    DEFAULT_DECK_THEME_ID: 'midnight',
    themeToDeck: (p: unknown) => p,
    themeToTokens: () => ({}),
  };
});
// Isolate from the workspace-themes fetch (returns no palettes → built-in theme).
vi.mock('../../hooks/useA2uiThemes', () => ({ useA2uiThemes: () => null }));

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const surface: any = {
  surfaceKind: 'document',
  root: 'root',
  components: [{ id: 'root', component: 'Markdown', content: 'hi' }],
  dataModel: {},
};

describe('A2uiSurface', () => {
  it('renders the shared renderer with the surface', () => {
    render(<A2uiSurface surface={surface} />);
    expect(screen.getByTestId('a2ui-rendered')).toHaveTextContent('document');
  });

  it('omits the corner expand control when onExpand is not provided', () => {
    render(<A2uiSurface surface={surface} />);
    expect(screen.queryByLabelText('Open in preview pane')).toBeNull();
  });

  it('renders the expand control and calls onExpand once on click', () => {
    const onExpand = vi.fn();
    render(<A2uiSurface surface={surface} onExpand={onExpand} />);
    fireEvent.click(screen.getByLabelText('Open in preview pane'));
    expect(onExpand).toHaveBeenCalledTimes(1);
  });

  it('stops click propagation so a clickable host row is not also triggered', () => {
    const onExpand = vi.fn();
    const parentClick = vi.fn();
    render(
      <div onClick={parentClick}>
        <A2uiSurface surface={surface} onExpand={onExpand} />
      </div>,
    );
    fireEvent.click(screen.getByLabelText('Open in preview pane'));
    expect(onExpand).toHaveBeenCalledTimes(1);
    expect(parentClick).not.toHaveBeenCalled();
  });
});
