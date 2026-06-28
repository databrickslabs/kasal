import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react';
import PreviewPanel, { parsePreviewContent, PreviewContent } from './PreviewPanel';
import { UIConfigService } from '../../../../api/UIConfigService';
import { THEME_PRESETS, type Theme } from '../../../Configuration/uiConfigShared';
import { themeToTokens } from '../../../../shared/a2ui';
import type { RunStep } from './RunTimeline';

// The panel fetches the workspace UI-Configurator palettes on mount; stub the
// service so tests control (or disable) the configured themes.
vi.mock('../../../../api/UIConfigService', () => ({
  UIConfigService: { getConfig: vi.fn() },
}));
const getConfigMock = vi.mocked(UIConfigService.getConfig);

// The PDF export itself is exercised in surfacePdf.test.tsx — here we only
// verify the panel wires the themed surface + title into it.
const downloadSurfacePdfMock = vi.fn();
vi.mock('../../utils/surfacePdf', () => ({
  downloadSurfacePdf: (...args: unknown[]) => downloadSurfacePdfMock(...args),
}));

// The PPTX export (pptxgenjs) is exercised in the shared lib's own tests — here we
// only verify the panel's PowerPoint menu option wires the surface + filename in.
// Partial mock so the renderer's other imports (downloadCsv) stay real.
const downloadPptxMock = vi.fn();
vi.mock('../../../../shared/a2ui/lib/download', async (importOriginal) => {
  const orig = await importOriginal<typeof import('../../../../shared/a2ui/lib/download')>();
  return { ...orig, downloadPptx: (...args: unknown[]) => downloadPptxMock(...args) };
});

// Theming flows through the useA2uiThemes hook (workspace palettes). Mock it so
// tests control the resolved palette deterministically and without the hook's
// module-level fetch cache leaking across tests. The hook's own fetch +
// style_json parsing is identical to useWorkspaceThemes (covered by
// useWorkspaceThemes.test.ts). Variable is `mock`-prefixed for vi.mock hoisting.
let mockA2uiThemes: Record<string, Theme> | null = null;
vi.mock('../../hooks/useA2uiThemes', () => ({
  useA2uiThemes: () => mockA2uiThemes,
}));

beforeEach(() => {
  getConfigMock.mockReset();
  getConfigMock.mockResolvedValue({ enabled: false, catalog_type: 'basic' } as never);
  downloadSurfacePdfMock.mockReset();
  downloadSurfacePdfMock.mockResolvedValue(undefined);
  downloadPptxMock.mockReset();
  downloadPptxMock.mockResolvedValue(undefined);
  mockA2uiThemes = null;
});

// A minimal A2UI document — the ONLY previewable content kind.
const uiDoc = JSON.stringify({
  messages: [
    {
      updateComponents: {
        surfaceId: 's1',
        components: [{ id: 'root', component: 'Text', text: 'Hello App' }],
      },
    },
  ],
});
const uiContent: PreviewContent = { type: 'ui', data: uiDoc };

// A deck whose agent-stamped theme is the WRONG (Default/white) palette — the
// regression that motivated re-resolving themes from the workspace config.
const deckDoc = JSON.stringify({
  messages: [
    {
      createSurface: {
        surfaceId: 's1',
        catalogId: 'basic',
        theme: { accent: '#2272B4', background: '#FFFFFF' },
      },
    },
    {
      updateComponents: {
        surfaceId: 's1',
        components: [
          { id: 'root', component: 'Slides', children: ['s1'] },
          { id: 's1', component: 'Slide', title: 'T', children: ['t'] },
          { id: 't', component: 'Text', text: 'Deck text' },
        ],
      },
    },
  ],
});

describe('parsePreviewContent — A2UI only', () => {
  it('returns null for empty or too-short input', () => {
    expect(parsePreviewContent('')).toBeNull();
    expect(parsePreviewContent('short')).toBeNull();
  });

  it('classifies an A2UI document as ui (plain, fenced, and behind a **Title** prefix)', () => {
    expect(parsePreviewContent(uiDoc)?.type).toBe('ui');
    expect(parsePreviewContent('```json\n' + uiDoc + '\n```')?.type).toBe('ui');
    // an inner fence surrounded by prose is unwrapped too
    expect(parsePreviewContent('Here is the app:\n```json\n' + uiDoc + '\n```\nthanks')?.type).toBe('ui');
    const out = parsePreviewContent('**My Task**\n\n' + uiDoc);
    expect(out?.type).toBe('ui');
    // parsePreviewContent now returns the CANONICAL adapted Surface JSON (a flat
    // {surfaceKind, root, components[]}), not the raw legacy {messages} input.
    const surf = JSON.parse(out!.data);
    expect(surf.surfaceKind).toBeDefined();
    expect(Array.isArray(surf.components)).toBe(true);
    expect(out!.data).toContain('Hello App');
  });

  it('does NOT preview raw HTML (A2UI-only by design)', () => {
    // Raw HTML is deliberately not a preview type anymore — crews are steered
    // toward A2UI documents; ad-hoc HTML output gets no preview pane.
    expect(parsePreviewContent('<!DOCTYPE html><html><body>hi</body></html>')).toBeNull();
    expect(parsePreviewContent('<html lang="en"><body>x</body></html>')).toBeNull();
    expect(parsePreviewContent('<div><script>var x=1;</script></div>')).toBeNull();
    // …including full HTML documents embedded in mixed text output.
    const embedded =
      'some text\n<!DOCTYPE html><body>' + 'b'.repeat(160) + '</body></html>\nmore';
    expect(parsePreviewContent(embedded)).toBeNull();
  });

  it('finds an A2UI doc WRAPPED in a result envelope (so it never leaks to chat)', () => {
    // The backend hands the surface inside {result:{…}} / {output:"<json>"} /
    // {data:{result:…}}; a top-level-only parse missed these and dumped raw JSON
    // into the chat. toSurface unwraps the canonical Surface from any of them.
    const surface = {
      surfaceKind: 'document',
      root: 'root',
      components: [{ id: 'root', component: 'Text', text: 'Wrapped Hello' }],
    };
    expect(parsePreviewContent(JSON.stringify({ result: surface }))?.type).toBe('ui');
    expect(parsePreviewContent(JSON.stringify({ output: JSON.stringify(surface) }))?.type).toBe('ui');
    expect(parsePreviewContent(JSON.stringify({ data: { result: surface } }))?.type).toBe('ui');
  });

  it('does NOT preview generic JSON, markdown, or plain text', () => {
    expect(parsePreviewContent('{"a":1,"b":2}')).toBeNull();
    expect(parsePreviewContent('[{"a":1},{"a":2}]')).toBeNull();
    expect(parsePreviewContent('# Title\n\n- a\n- b')).toBeNull();
    expect(parsePreviewContent('# Big\n\n' + 'x'.repeat(600))).toBeNull();
    expect(parsePreviewContent('just a normal sentence with nothing special here at all')).toBeNull();
  });
});

const baseProps = {
  onClose: vi.fn(),
  chatCollapsed: false,
  onToggleChat: vi.fn(),
};

function renderPanel(content: PreviewContent, props: Partial<typeof baseProps> = {}) {
  return render(<PreviewPanel content={content} {...baseProps} {...props} />);
}

describe('PreviewPanel component', () => {
  it('renders an A2UI document via the brand renderer with an "App" label and UI chip', () => {
    renderPanel(uiContent);
    expect(screen.getByText('App')).toBeInTheDocument();
    expect(screen.getByText('UI')).toBeInTheDocument(); // type badge
    expect(screen.getByText('Hello App')).toBeInTheDocument();
  });

  it('uses a provided title over the default', () => {
    renderPanel({ ...uiContent, title: 'Custom Title' });
    expect(screen.getByText('Custom Title')).toBeInTheDocument();
  });

  it('heals a stored **Title** prefix in displayData', () => {
    renderPanel({ type: 'ui', data: '**Task X**\n\n' + uiDoc });
    expect(screen.getByText('Hello App')).toBeInTheDocument();
  });

  it('renders an empty body when the stored data is not a parseable A2UI document', () => {
    renderPanel({ type: 'ui', data: 'not a ui document at all' });
    expect(screen.getByText('App')).toBeInTheDocument(); // header still renders
    expect(screen.getByTestId('preview-body').childElementCount).toBe(0);
  });

  it('close and toggle-chat buttons fire their callbacks', () => {
    const onClose = vi.fn();
    const onToggleChat = vi.fn();
    renderPanel(uiContent, { onClose, onToggleChat });
    fireEvent.click(screen.getByTitle('Close preview'));
    expect(onClose).toHaveBeenCalled();
    fireEvent.click(screen.getByTitle('Hide chat'));
    expect(onToggleChat).toHaveBeenCalled();
  });

  it('shows "Show chat" affordance when chatCollapsed is true', () => {
    renderPanel(uiContent, { chatCollapsed: true });
    expect(screen.getByTitle('Show chat')).toBeInTheDocument();
  });

  it('expand/restore arrows match the preview position (right of chat)', () => {
    // Preview expands LEFTWARD over the chat: "Hide chat" must point left
    // (path M18.75...), restoring the chat must point right (path M11.25...).
    const { unmount } = renderPanel(uiContent, { chatCollapsed: false });
    expect(
      screen.getByTitle('Hide chat').querySelector('path')?.getAttribute('d'),
    ).toMatch(/^M18\.75/);
    unmount();

    renderPanel(uiContent, { chatCollapsed: true });
    expect(
      screen.getByTitle('Show chat').querySelector('path')?.getAttribute('d'),
    ).toMatch(/^M11\.25/);
  });

  it('does not render the Customize button when onRefine is not provided', () => {
    renderPanel(uiContent);
    expect(screen.queryByText('Customize')).not.toBeInTheDocument();
  });

  it('opens the Customize panel and submits a free-text instruction via Send', () => {
    const onRefine = vi.fn();
    render(<PreviewPanel content={uiContent} {...baseProps} onRefine={onRefine} />);
    // toggle open
    fireEvent.click(screen.getByText('Customize'));
    expect(screen.getByTestId('refine-panel')).toBeInTheDocument();
    const input = screen.getByPlaceholderText(/add a chart/i);
    fireEvent.change(input, { target: { value: 'make it blue' } });
    fireEvent.click(screen.getByText('Send'));
    expect(onRefine).toHaveBeenCalledWith('make it blue');
    // panel closes after submit
    expect(screen.queryByTestId('refine-panel')).not.toBeInTheDocument();
  });

  it('submits the free-text instruction on Enter and ignores empty submits', () => {
    const onRefine = vi.fn();
    render(<PreviewPanel content={uiContent} {...baseProps} onRefine={onRefine} />);
    fireEvent.click(screen.getByText('Customize'));
    const input = screen.getByPlaceholderText(/add a chart/i);
    // empty Enter -> no-op
    fireEvent.keyDown(input, { key: 'Enter' });
    expect(onRefine).not.toHaveBeenCalled();
    // typed Enter -> submits
    fireEvent.change(input, { target: { value: 'add a chart' } });
    fireEvent.keyDown(input, { key: 'Enter' });
    expect(onRefine).toHaveBeenCalledWith('add a chart');
  });

  it('applies a deterministic style preset via onStyleChange (no AI)', () => {
    const onRefine = vi.fn();
    const onStyleChange = vi.fn();
    render(<PreviewPanel content={uiContent} {...baseProps} onRefine={onRefine} onStyleChange={onStyleChange} />);
    fireEvent.click(screen.getByText('Customize'));
    // Click a one-click Look preset — restyles instantly, no crew run.
    fireEvent.click(screen.getByTitle('Apply the Dark style'));
    expect(onStyleChange).toHaveBeenCalledTimes(1);
    // The rewritten document carries the picked dark palette as the surface theme.
    const updated = onStyleChange.mock.calls[0][0] as string;
    expect(JSON.parse(updated).theme.accent).toBe('#38BDF8');
    expect(updated).toContain('#38BDF8');
    expect(onRefine).not.toHaveBeenCalled();
  });

  it('restyles a dashboard even when the document has a non-fenced prose preamble (regression)', () => {
    // An agent prefaced the dashboard JSON with a sentence. The renderer tolerates
    // it (so the preview shows fine), but the instant "Look" used to JSON.parse the
    // whole string, throw on the prose, and silently keep the doc unchanged — so
    // restyling appeared broken for that deliverable. It must now restyle the
    // embedded document.
    const dashDoc = JSON.stringify({
      messages: [
        { createSurface: { surfaceId: 's1', catalogId: 'basic' } },
        {
          updateComponents: {
            components: [
              { id: 'root', component: 'Column', children: ['d'] },
              { id: 'd', component: 'Dashboard', children: ['k'] },
              { id: 'k', component: 'Stat', label: 'Revenue', value: '$1M' },
            ],
          },
        },
      ],
    });
    const prosey = 'Here is your dashboard:\n' + dashDoc;
    const onStyleChange = vi.fn();
    render(<PreviewPanel content={{ type: 'ui', data: prosey }} {...baseProps} onRefine={vi.fn()} onStyleChange={onStyleChange} />);
    fireEvent.click(screen.getByText('Customize'));
    expect(screen.getByText('Dashboard')).toBeInTheDocument(); // detected as a dashboard
    fireEvent.click(screen.getByTitle('Apply the Dark style'));
    expect(onStyleChange).toHaveBeenCalledTimes(1);
    const updated = onStyleChange.mock.calls[0][0] as string;
    expect(updated).not.toBe(prosey); // no longer the silent no-op
    expect(JSON.parse(updated).theme.accent).toBe('#38BDF8'); // dark preset accent applied
    expect(updated).toContain('#38BDF8');
  });

  it('a preset click is a safe no-op when onStyleChange is not provided', () => {
    const onRefine = vi.fn();
    render(<PreviewPanel content={uiContent} {...baseProps} onRefine={onRefine} />); // no onStyleChange
    fireEvent.click(screen.getByText('Customize'));
    // Should not throw and must not trigger an AI refine.
    fireEvent.click(screen.getByTitle('Apply the Dark style'));
    expect(onRefine).not.toHaveBeenCalled();
  });

  it('detects the deliverable and shows its content controls (deck → Presentation)', () => {
    render(<PreviewPanel content={{ type: 'ui', data: deckDoc }} {...baseProps} onRefine={vi.fn()} />);
    fireEvent.click(screen.getByText('Customize'));
    // Friendly title noun + the presentation-specific content control.
    expect(screen.getByText('Presentation')).toBeInTheDocument();
    expect(screen.getByLabelText('Target slide count')).toBeInTheDocument();
  });

  it('closes the Customize panel on Escape in the free-text box without submitting', () => {
    const onRefine = vi.fn();
    render(<PreviewPanel content={uiContent} {...baseProps} onRefine={onRefine} />);
    fireEvent.click(screen.getByText('Customize'));
    const input = screen.getByPlaceholderText(/add a chart/i);
    fireEvent.change(input, { target: { value: 'discard me' } });
    fireEvent.keyDown(input, { key: 'Escape' });
    expect(onRefine).not.toHaveBeenCalled();
    expect(screen.queryByTestId('refine-panel')).not.toBeInTheDocument();
  });

  it('does not render the history nav when history has one or zero entries', () => {
    const onNavigate = vi.fn();
    const single = [uiContent];
    renderPanel(uiContent, {});
    expect(screen.queryByLabelText('Previous output')).not.toBeInTheDocument();
    // explicit single-entry history + onNavigate still hides the nav
    render(
      <PreviewPanel
        content={single[0]}
        {...baseProps}
        history={single}
        index={0}
        onNavigate={onNavigate}
      />,
    );
    expect(screen.queryByLabelText('Previous output')).not.toBeInTheDocument();
  });

  it('renders history nav, shows position, and navigates older/newer', () => {
    const onNavigate = vi.fn();
    const history: PreviewContent[] = [uiContent, { type: 'ui', data: deckDoc }, uiContent];
    render(
      <PreviewPanel
        content={history[2]}
        {...baseProps}
        history={history}
        index={2}
        onNavigate={onNavigate}
      />,
    );
    // position label: latest shown first => 3/3
    expect(screen.getByText('3/3')).toBeInTheDocument();
    // next disabled at the latest, prev enabled
    const prev = screen.getByLabelText('Previous output');
    const next = screen.getByLabelText('Next output');
    expect(next).toBeDisabled();
    expect(prev).not.toBeDisabled();
    fireEvent.click(prev);
    expect(onNavigate).toHaveBeenCalledWith(1);
  });

  it('defaults the position to the latest when index is omitted and disables prev at the oldest', () => {
    const onNavigate = vi.fn();
    const history: PreviewContent[] = [uiContent, { type: 'ui', data: deckDoc }];
    // index omitted -> defaults to history.length - 1 (latest)
    render(
      <PreviewPanel
        content={history[1]}
        {...baseProps}
        history={history}
        onNavigate={onNavigate}
      />,
    );
    expect(screen.getByText('2/2')).toBeInTheDocument();
    fireEvent.click(screen.getByLabelText('Next output')); // disabled, no-op visually but click guarded
    // navigate to the oldest then assert prev disabled there
    render(
      <PreviewPanel
        content={history[0]}
        {...baseProps}
        history={history}
        index={0}
        onNavigate={onNavigate}
      />,
    );
    const prevs = screen.getAllByLabelText('Previous output');
    expect(prevs[prevs.length - 1]).toBeDisabled();
    const next = screen.getAllByLabelText('Next output');
    fireEvent.click(next[next.length - 1]);
    expect(onNavigate).toHaveBeenCalledWith(1);
  });
});

describe('PreviewPanel — workspace palettes theme the surface', () => {
  // The configured workspace palette reaches the rendered surface as the shared
  // renderer's `--a2-*` CSS tokens (themeToTokens) on the `.kasal-a2ui` wrapper.
  // Precedence (A2uiSurface): explicit prop → per-surface "Look" override
  // (surface.theme) → workspace palette → built-in defaults. The legacy
  // {messages} deck's embedded theme is dropped by the adapter, so the workspace
  // palette applies. (style_json fetch + parsing live in useA2uiThemes, mocked
  // here; that logic is identical to — and covered by — useWorkspaceThemes.test.)
  const wrapper = (container: HTMLElement) =>
    container.querySelector('.kasal-a2ui') as HTMLElement | null;
  const DARK = THEME_PRESETS.find((p) => p.key === 'dark')!.theme;
  const STUDIO = THEME_PRESETS.find((p) => p.key === 'studio')!.theme;

  it('applies the configured deliverable palette to the surface tokens', () => {
    mockA2uiThemes = { presentation: STUDIO };
    const { container } = renderPanel({ type: 'ui', data: deckDoc });
    expect(wrapper(container)?.style.getPropertyValue('--a2-background')).toBe(
      themeToTokens(STUDIO)['--a2-background'],
    );
  });

  it('lets a per-surface "Look" override win over the workspace palette', () => {
    mockA2uiThemes = { presentation: STUDIO };
    // A new-format surface carrying its own "Look" theme (DARK).
    const overridden = JSON.stringify({
      surfaceKind: 'presentation',
      root: 'root',
      components: [{ id: 'root', component: 'SlideDeck', children: [] }],
      theme: DARK,
    });
    const { container } = renderPanel({ type: 'ui', data: overridden });
    // DARK (the surface's own Look) wins — NOT STUDIO (the workspace palette).
    expect(wrapper(container)?.style.getPropertyValue('--a2-background')).toBe(
      themeToTokens(DARK)['--a2-background'],
    );
    expect(wrapper(container)?.style.getPropertyValue('--a2-background')).not.toBe(
      themeToTokens(STUDIO)['--a2-background'],
    );
  });

  it('falls back to built-in tokens when no palette is configured and the surface has no override', () => {
    mockA2uiThemes = null; // disabled / unavailable / malformed config → null
    const { container } = renderPanel({ type: 'ui', data: deckDoc });
    // No workspace palette and no embedded override → no --a2-* override applied.
    expect(wrapper(container)?.style.getPropertyValue('--a2-background')).toBe('');
  });
});

describe('PreviewPanel — full screen', () => {
  const content: PreviewContent = uiContent;
  let fsEl: Element | null;
  const origReq = Element.prototype.requestFullscreen;
  const origExit = document.exitFullscreen;

  beforeEach(() => {
    fsEl = null;
    Object.defineProperty(document, 'fullscreenElement', { configurable: true, get: () => fsEl });
    Element.prototype.requestFullscreen = vi.fn(function (this: Element) {
      // The fullscreen target IS the `this` element; capture it for the getter.
      // eslint-disable-next-line @typescript-eslint/no-this-alias
      fsEl = this;
      document.dispatchEvent(new Event('fullscreenchange'));
      return Promise.resolve();
    });
    document.exitFullscreen = vi.fn(() => {
      fsEl = null;
      document.dispatchEvent(new Event('fullscreenchange'));
      return Promise.resolve();
    });
    return () => {
      Element.prototype.requestFullscreen = origReq;
      document.exitFullscreen = origExit;
    };
  });

  it('hides the ENTIRE header bar in full screen; the browser (Esc) restores it', () => {
    renderPanel(content, { onRefine: vi.fn() });
    // header visible: title, type chip, Customize, full-screen toggle, close
    expect(screen.getByText('App')).toBeInTheDocument();
    expect(screen.getByText('UI')).toBeInTheDocument();
    expect(screen.getByText('Customize')).toBeInTheDocument();
    expect(screen.getByTitle('Close preview')).toBeInTheDocument();

    fireEvent.click(screen.getByLabelText('Full screen'));
    expect(Element.prototype.requestFullscreen).toHaveBeenCalled();
    // the whole bar is gone — no header controls at all
    expect(screen.queryByText('App')).toBeNull();
    expect(screen.queryByText('UI')).toBeNull();
    expect(screen.queryByText('Customize')).toBeNull();
    expect(screen.queryByTitle('Close preview')).toBeNull();
    expect(screen.queryByLabelText('Full screen')).toBeNull();
    // ...but the content keeps rendering
    expect(screen.getByText('Hello App')).toBeInTheDocument();

    // browser exits full screen (Esc) → the bar returns
    fsEl = null;
    fireEvent(document, new Event('fullscreenchange'));
    expect(screen.getByText('App')).toBeInTheDocument();
    expect(screen.getByLabelText('Full screen')).toBeInTheDocument();
  });

  it('swallows a rejected requestFullscreen (e.g. blocked in an iframe) and keeps the bar', async () => {
    Element.prototype.requestFullscreen = vi.fn(() => Promise.reject(new Error('blocked')));
    renderPanel(content, { onRefine: vi.fn() });
    fireEvent.click(screen.getByLabelText('Full screen'));
    await Promise.resolve();
    // no fullscreenchange fired → bar stays, no unhandled rejection
    expect(screen.getByLabelText('Full screen')).toBeInTheDocument();
    expect(screen.getByText('App')).toBeInTheDocument();
  });
});

describe('PreviewPanel — Download menu (PDF / PowerPoint)', () => {
  // The single top-toolbar control is a MENU: clicking it opens PDF / PowerPoint
  // options (replacing the old icon-only PDF button AND the deck's own PowerPoint
  // button, which is suppressed in the pane).
  const openMenuAndPick = async (label: 'PDF' | 'PowerPoint') => {
    fireEvent.click(screen.getByLabelText('Download'));
    fireEvent.click(await screen.findByText(label));
  };

  it('downloads the THEMED surface as a PDF using the preview title', async () => {
    renderPanel({ ...uiContent, title: 'Oil Report' });

    // The trigger is icon-only — no "PDF"/"Download" text next to the arrow.
    expect(screen.getByLabelText('Download').textContent).toBe('');

    await openMenuAndPick('PDF');
    await waitFor(() => expect(downloadSurfacePdfMock).toHaveBeenCalledTimes(1));
    const [surface, title] = downloadSurfacePdfMock.mock.calls[0];
    expect(title).toBe('Oil Report');
    // The new Surface keeps components as a flat ARRAY (root id + nodes).
    const comps = (surface as { components: unknown[] }).components;
    expect(Array.isArray(comps)).toBe(true);
    expect(comps.length).toBeGreaterThan(0);
  });

  it('downloads as PowerPoint via the menu, themed, using the preview title', async () => {
    renderPanel({ ...uiContent, title: 'Oil Report' });
    await openMenuAndPick('PowerPoint');
    await waitFor(() => expect(downloadPptxMock).toHaveBeenCalledTimes(1));
    const [surface, , filename] = downloadPptxMock.mock.calls[0];
    expect((surface as { components: unknown[] }).components).toBeDefined();
    expect(filename).toBe('Oil Report.pptx');
  });

  it('falls back to a default filename when the preview has no title', async () => {
    renderPanel(uiContent);
    await openMenuAndPick('PDF');
    await waitFor(() => expect(downloadSurfacePdfMock).toHaveBeenCalled());
    expect(downloadSurfacePdfMock.mock.calls[0][1]).toBe('kasal-app');
  });

  it('does nothing when the stored data is not a parseable A2UI document', async () => {
    renderPanel({ type: 'ui', data: 'not a ui document at all' });
    await openMenuAndPick('PDF');
    expect(downloadSurfacePdfMock).not.toHaveBeenCalled();
  });

  it('disables the trigger while the export is in flight and re-enables after', async () => {
    let release: () => void = () => undefined;
    downloadSurfacePdfMock.mockImplementationOnce(
      () => new Promise<void>((resolve) => { release = resolve; }),
    );
    renderPanel(uiContent);
    const btn = screen.getByLabelText('Download');
    await openMenuAndPick('PDF');
    await waitFor(() => expect(btn).toBeDisabled());
    // a second open+download while exporting is ignored (trigger disabled)
    fireEvent.click(btn);
    expect(downloadSurfacePdfMock).toHaveBeenCalledTimes(1);
    act(() => release());
    await waitFor(() => expect(btn).not.toBeDisabled());
  });
});

describe('PreviewPanel run activity', () => {
  const uiContent: PreviewContent = { type: 'ui', data: uiDoc };
  const steps: RunStep[] = [
    { id: '1', label: 'GenieTool', sublabel: 'sales by region', detail: 'Revenue rose 12% in EMEA.', timestamp: 1, durationMs: 2200 },
  ];

  it('lists plain-English steps and opens a step’s full context on its own page, with Back', async () => {
    render(<PreviewPanel content={uiContent} {...baseProps} runSteps={steps} />);
    // The deliverable is visible to start.
    expect(screen.getByText('Hello App')).toBeInTheDocument();
    fireEvent.click(screen.getByText('Activity'));
    // List view: friendly headings + a short cleaned preview; deliverable hidden.
    expect(screen.queryByText('Hello App')).not.toBeInTheDocument();
    expect(screen.getByText('Querying your data')).toBeInTheDocument();
    expect(screen.getByText('Revenue rose 12% in EMEA.')).toBeInTheDocument(); // preview
    // The full-page context isn't open until a step is clicked.
    expect(screen.queryByTestId('run-step-context')).not.toBeInTheDocument();
    // Click the step → its context opens full-page with the readable content.
    fireEvent.click(screen.getByText('Querying your data').closest('button')!);
    await waitFor(() => expect(screen.getByTestId('run-step-context')).toBeInTheDocument());
    expect(screen.getByText('Revenue rose 12% in EMEA.')).toBeInTheDocument();
    // Back returns to the step list.
    fireEvent.click(screen.getByLabelText('Back to the run activity'));
    expect(screen.queryByTestId('run-step-context')).not.toBeInTheDocument();
    expect(screen.getByText('Querying your data')).toBeInTheDocument();
  });

  it('shows no Run activity control when there are no steps', () => {
    render(<PreviewPanel content={uiContent} {...baseProps} runSteps={[]} />);
    expect(screen.queryByText('Activity')).not.toBeInTheDocument();
  });

  it('renders "Show in chat" as an icon button that moves the activity to the chat', () => {
    const onMoveActivityToChat = vi.fn();
    render(
      <PreviewPanel
        content={uiContent}
        {...baseProps}
        runSteps={steps}
        onMoveActivityToChat={onMoveActivityToChat}
      />,
    );
    // Compact icon control (no visible text), found by its accessible label.
    const btn = screen.getByLabelText('Show in chat');
    expect(btn).toBeInTheDocument();
    expect(btn).toHaveTextContent('');
    fireEvent.click(btn);
    expect(onMoveActivityToChat).toHaveBeenCalled();
  });
});
