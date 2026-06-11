import { vi, describe, it, expect, beforeEach } from 'vitest';
import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { UiSurfaceResult, UiSurfaceView } from './UiSurfaceResult';
import { UiSurface } from '../../ChatMode/utils/uiDocument';

// ---------------------------------------------------------------------------
// Mocks
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

function makeSurface(): UiSurface {
  return {
    rootId: 'root',
    components: {
      root: { id: 'root', component: 'Column', children: ['title', 'badge'] },
      title: { id: 'title', component: 'Text', variant: 'h1', text: 'Hello Report' },
      badge: { id: 'badge', component: 'Badge', text: 'All good', tone: 'good' },
    },
    data: {},
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
  it('renders the surface full size', () => {
    render(<UiSurfaceView surface={makeSurface()} />);
    expect(screen.getByText('Hello Report')).toBeInTheDocument();
  });

  it('re-resolves the theme from the workspace UI-Configurator palettes', async () => {
    mockGetConfig.mockResolvedValue({
      enabled: true,
      style_json: JSON.stringify({ themes: { default: { accent: '#123456' } } }),
    });

    const { container } = render(<UiSurfaceView surface={makeSurface()} />);

    await waitFor(() => {
      const stage = container.firstChild as HTMLElement;
      expect(stage.style.getPropertyValue('--ui-accent')).toBe('#123456');
    });
  });

  it('keeps the embedded theme when the config is unavailable', async () => {
    mockGetConfig.mockRejectedValue(new Error('boom'));

    const surface = { ...makeSurface(), theme: { accent: '#ABCDEF' } };
    const { container } = render(<UiSurfaceView surface={surface} />);

    await waitFor(() => {
      const stage = container.firstChild as HTMLElement;
      expect(stage.style.getPropertyValue('--ui-accent')).toBe('#ABCDEF');
    });
  });
});
