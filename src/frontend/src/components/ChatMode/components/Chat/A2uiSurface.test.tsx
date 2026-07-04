import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { A2uiSurface } from './A2uiSurface';

// Mock the shared renderer + its theme contexts so this unit test never pulls in
// the heavy ESM render stack (react-markdown / recharts). We only verify the
// A2uiSurface WRAPPER: that it draws the renderer and wires the corner "expand"
// control (the de-MUI'd plain <button> with a lucide Maximize2 icon).
vi.mock('../../../../shared/a2ui', async () => {
  const React = await import('react');
  const SurfaceChromeContext = React.createContext({ downloads: true });
  return {
    // Surface the chrome `downloads` flag the wrapper provides, so tests can
    // assert whether the Download control would render for this surface.
    A2UIRenderer: ({ payload }: { payload: { surfaceKind?: string } }) => {
      const chrome = React.useContext(SurfaceChromeContext) as { downloads?: boolean };
      return (
        <div data-testid="a2ui-rendered" data-downloads={String(chrome.downloads)}>
          {payload?.surfaceKind}
        </div>
      );
    },
    DeckThemeContext: React.createContext({}),
    SurfaceChromeContext,
    getDeckTheme: () => ({ id: 'midnight' }),
    DEFAULT_DECK_THEME_ID: 'midnight',
    themeToDeck: (p: unknown) => p,
    // Echo the palette's background as the --a2-background CSS var so a test can
    // assert WHICH palette was resolved for the surface (white default vs dark).
    themeToTokens: (p: { background?: string }) => ({ '--a2-background': p?.background }),
  };
});
// Isolate from the workspace-themes fetch. Default null (no palettes → built-in
// theme); a test can set `h.themes` to exercise per-deliverable palette resolution.
const h = vi.hoisted(() => ({ themes: null as Record<string, unknown> | null }));
vi.mock('../../hooks/useA2uiThemes', () => ({ useA2uiThemes: () => h.themes }));

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

  it('keeps the Download control enabled for a non-quiz surface', () => {
    render(<A2uiSurface surface={surface} />);
    expect(screen.getByTestId('a2ui-rendered')).toHaveAttribute('data-downloads', 'true');
  });

  it('suppresses the Download control for a quiz surface', () => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const quiz: any = { ...surface, surfaceKind: 'quiz' };
    render(<A2uiSurface surface={quiz} />);
    expect(screen.getByTestId('a2ui-rendered')).toHaveAttribute('data-downloads', 'false');
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

  it('defaults a document surface to the white DEFAULT_THEME when no workspace palette', () => {
    // useA2uiThemes is mocked to null (unconfigured/disabled workspace). Without the
    // fallback the surface would inherit the chat root's --a2-* tokens (dark in dark
    // mode); with it a graph surface renders on the white DEFAULT_THEME background.
    const { container } = render(<A2uiSurface surface={surface} />);
    const wrapper = container.querySelector('[data-surface-kind="document"]') as HTMLElement;
    // themeToTokens is mocked to echo the resolved palette's background as --a2-background.
    expect(wrapper.style.getPropertyValue('--a2-background')).toBe('#FFFFFF');
  });

  it('brands a surface from its ROOT component deliverable (Forecast → forecast palette)', () => {
    // A dashboard surface ROOTED on a Forecast must use the workspace's 'forecast'
    // palette (component-based deliverable), not the 'dashboard'/'default' one.
    h.themes = {
      forecast: { background: '#111111' },
      dashboard: { background: '#222222' },
      default: { background: '#333333' },
    };
    try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const s: any = {
        surfaceKind: 'dashboard',
        root: 'fc',
        components: [{ id: 'fc', component: 'Forecast', data: { path: '/d' } }],
        dataModel: {},
      };
      const { container } = render(<A2uiSurface surface={s} />);
      const wrapper = container.querySelector('[data-surface-kind="dashboard"]') as HTMLElement;
      expect(wrapper.style.getPropertyValue('--a2-background')).toBe('#111111');
    } finally {
      h.themes = null;
    }
  });

  it('does NOT force a palette on a deck-themed surface with no workspace palette', () => {
    // Deck kinds keep their existing (deck-theme) behavior — not gated to white.
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const mindmap: any = { ...surface, surfaceKind: 'mindmap' };
    const { container } = render(<A2uiSurface surface={mindmap} />);
    const wrapper = container.querySelector('[data-surface-kind="mindmap"]') as HTMLElement;
    expect(wrapper.style.getPropertyValue('--a2-background')).toBe('');
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
