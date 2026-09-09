import { vi, describe, it, expect, beforeEach } from 'vitest';
import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import { UiSurfaceResult, UiSurfaceView } from './UiSurfaceResult';
import type { Surface } from '../../../../shared/a2ui/index';
import { BuilderPreviewContext } from './BuilderPreviewContext';

// ---------------------------------------------------------------------------
// Mocks — A2uiSurface resolves workspace branding via useA2uiThemes →
// UIConfigService.getConfig; stub it so the card renders with built-in defaults
// (theming itself is covered by the shared a2ui deckThemes tests).
// ---------------------------------------------------------------------------

const mockGetConfig = vi.fn();
vi.mock('../../../../api/config/UIConfigService', () => ({
  UIConfigService: {
    getConfig: (...args: unknown[]) => mockGetConfig(...args),
    // useA2uiThemes seeds synchronously from the session cache and subscribes
    // for Configurator saves — stub both (no cache hit, no-op unsubscribe).
    peek: () => null,
    subscribe: () => () => undefined,
  },
}));

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeSurface(): Surface {
  return {
    surfaceKind: 'document',
    root: 'root',
    components: [
      { id: 'root', component: 'Column', children: ['title', 'body'] },
      { id: 'title', component: 'Heading', text: 'Hello Report', level: 1 },
      { id: 'body', component: 'Text', text: 'All good' },
    ],
    dataModel: {},
  };
}

function makeDeckSurface(): Surface {
  return {
    surfaceKind: 'presentation',
    root: 'deck',
    components: [
      { id: 'deck', component: 'SlideDeck', children: ['s1', 's2'] },
      { id: 's1', component: 'Slide', variant: 'title', title: 'Deck Title' },
      { id: 's2', component: 'Slide', title: 'Second Slide', children: ['s2t'] },
      { id: 's2t', component: 'Text', text: 'Slide body' },
    ],
    dataModel: {},
  };
}

beforeEach(() => {
  vi.clearAllMocks();
  mockGetConfig.mockResolvedValue({ enabled: false });
});

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('UiSurfaceResult (A2UI result card)', () => {
  it('renders the surface content instead of raw JSON', () => {
    render(<UiSurfaceResult surface={makeSurface()} />);
    expect(screen.getByText('Hello Report')).toBeInTheDocument();
    expect(screen.getByText('All good')).toBeInTheDocument();
    expect(screen.queryByText('Generated UI')).toBeNull();
  });

  it('offers Chat’s color palette and passes the chosen surface back to the conversation', () => {
    const onRestyle = vi.fn();
    render(<UiSurfaceResult surface={makeSurface()} onRestyle={onRestyle} />);
    fireEvent.click(screen.getByRole('button', { name: 'Customize colors' }));
    const choice = screen.getByRole('menu').querySelector('button')!;
    fireEvent.click(choice);
    expect(onRestyle).toHaveBeenCalledWith(expect.objectContaining({
      root: 'root', theme: expect.objectContaining({ accent: expect.any(String) }),
    }));
  });

  it('opens a full-size dialog from the expand control', () => {
    render(<UiSurfaceResult surface={makeSurface()} />);
    fireEvent.click(screen.getByLabelText('Open in preview pane'));
    // Surface now renders twice: inline preview + dialog.
    expect(screen.getAllByText('Hello Report')).toHaveLength(2);

    fireEvent.click(screen.getByLabelText('Close full view'));
  });

  it('keeps the inline surface interactive instead of opening a dialog on every click', () => {
    render(<UiSurfaceResult surface={makeSurface()} />);
    fireEvent.click(screen.getByText('Hello Report'));
    expect(screen.getAllByText('Hello Report')).toHaveLength(1);
    expect(screen.queryByRole('dialog')).toBeNull();
  });
  it('opens in the builder preview and avoids rendering a duplicate inline surface', () => {
    const openResult = vi.fn(); const closePreview = vi.fn();
    const value = { openMemory: vi.fn(), openStep: vi.fn(), openResult, closePreview };
    const view = render(<BuilderPreviewContext.Provider value={value}><UiSurfaceResult surface={makeSurface()} messageId="result-one" /></BuilderPreviewContext.Provider>);
    fireEvent.click(screen.getByLabelText('Open in preview pane'));
    expect(openResult).toHaveBeenCalledWith(expect.objectContaining({ type: 'ui', sourceMessageId: 'result-one' }));
    expect(screen.queryByRole('dialog')).toBeNull();
    view.rerender(<BuilderPreviewContext.Provider value={{ ...value, previewMessageId: 'result-one' }}><UiSurfaceResult surface={makeSurface()} messageId="result-one" /></BuilderPreviewContext.Provider>);
    expect(screen.queryByText('Hello Report')).toBeNull();
    fireEvent.click(screen.getByRole('button', { name: 'Show here' }));
    expect(closePreview).toHaveBeenCalledOnce();
  });

  // The shared renderer's Tailwind utilities are compiled under the
  // `.kasal-chat-root` scope (tailwind.config.js `important`); this chat lives
  // outside the chat-mode root, so the card must recreate the scope or every
  // utility no-ops (the "SlideDeck collapses to text height" defect).
  it('renders the surface inside a .kasal-chat-root scope with a data-theme', () => {
    const { container } = render(<UiSurfaceResult surface={makeSurface()} />);
    const scope = container.querySelector('.kasal-chat-root');
    expect(scope).not.toBeNull();
    expect(scope!.getAttribute('data-theme')).toMatch(/^(light|dark)$/);
    // The surface renders INSIDE the scope, not beside it.
    expect(scope!.querySelector('.kasal-a2ui')).not.toBeNull();
  });
});

describe('UiSurfaceResult (retired presentation payload)', () => {
  it('shows the unsupported-component placeholder for legacy SlideDeck payloads', () => {
    render(<UiSurfaceResult surface={makeDeckSurface()} />);
    expect(screen.getByText('Unsupported component: SlideDeck')).toBeInTheDocument();
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
  });

  it('still offers the explicit expand control for decks', () => {
    render(<UiSurfaceResult surface={makeDeckSurface()} />);
    fireEvent.click(screen.getByLabelText('Open in preview pane'));
    expect(screen.getAllByText('Unsupported component: SlideDeck')).toHaveLength(2);
  });
});

describe('UiSurfaceView (full-size themed render)', () => {
  it('renders the surface full size through the shared A2UI renderer', () => {
    render(<UiSurfaceView surface={makeSurface()} />);
    expect(screen.getByText('Hello Report')).toBeInTheDocument();
  });
});
