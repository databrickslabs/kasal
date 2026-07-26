import { vi, describe, it, expect, beforeEach } from 'vitest';
import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import { UiSurfaceResult, UiSurfaceView } from './UiSurfaceResult';
import type { Surface } from '../../../shared/a2ui';

// ---------------------------------------------------------------------------
// Mocks — A2uiSurface resolves workspace branding via useA2uiThemes →
// UIConfigService.getConfig; stub it so the card renders with built-in defaults
// (theming itself is covered by the shared a2ui deckThemes tests).
// ---------------------------------------------------------------------------

const mockGetConfig = vi.fn();
vi.mock('../../../api/config/UIConfigService', () => ({
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
    expect(screen.getByText('Generated UI')).toBeInTheDocument();
  });

  it('opens a full-size dialog from the expand control', () => {
    render(<UiSurfaceResult surface={makeSurface()} />);
    fireEvent.click(screen.getByLabelText('Open full view'));
    // Surface now renders twice: inline preview + dialog.
    expect(screen.getAllByText('Hello Report')).toHaveLength(2);

    fireEvent.click(screen.getByLabelText('Close full view'));
  });

  it('opens the dialog when the preview itself is clicked', () => {
    render(<UiSurfaceResult surface={makeSurface()} />);
    fireEvent.click(screen.getByText('Hello Report'));
    expect(screen.getAllByText('Hello Report')).toHaveLength(2);
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

describe('UiSurfaceResult (presentation deck)', () => {
  it('keeps the deck navigable in place: nav works and does not open the dialog', () => {
    render(<UiSurfaceResult surface={makeDeckSurface()} />);
    expect(screen.getByText('Deck Title')).toBeInTheDocument();
    // Navigating with the deck's own controls advances the slide…
    fireEvent.click(screen.getByText('Next ›'));
    expect(screen.getByText('Second Slide')).toBeInTheDocument();
    // …and the click did NOT bubble into a full-view dialog (still one render).
    expect(screen.getAllByText('Second Slide')).toHaveLength(1);
  });

  it('still offers the explicit expand control for decks', () => {
    render(<UiSurfaceResult surface={makeDeckSurface()} />);
    fireEvent.click(screen.getByLabelText('Open full view'));
    expect(screen.getAllByText('Deck Title').length).toBeGreaterThan(1);
  });
});

describe('UiSurfaceView (full-size themed render)', () => {
  it('renders the surface full size through the shared A2UI renderer', () => {
    render(<UiSurfaceView surface={makeSurface()} />);
    expect(screen.getByText('Hello Report')).toBeInTheDocument();
  });
});
