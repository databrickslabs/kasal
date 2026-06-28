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
vi.mock('../../../api/UIConfigService', () => ({
  UIConfigService: {
    getConfig: (...args: unknown[]) => mockGetConfig(...args),
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
});

describe('UiSurfaceView (full-size themed render)', () => {
  it('renders the surface full size through the shared A2UI renderer', () => {
    render(<UiSurfaceView surface={makeSurface()} />);
    expect(screen.getByText('Hello Report')).toBeInTheDocument();
  });
});
