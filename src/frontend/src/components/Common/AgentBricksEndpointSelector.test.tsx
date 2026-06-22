import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import { AgentBricksEndpointSelector } from './AgentBricksEndpointSelector';
import type { AgentBricksEndpoint } from '../../api/AgentBricksService';

// ---------------------------------------------------------------------------
// AgentBricksService is the only external dependency. The selector calls
// getEndpoints() to populate the dropdown, isEndpointReady() for the "Ready"
// chip, and getEndpointByName() to hydrate a preselected value.
// ---------------------------------------------------------------------------
const getEndpoints = vi.fn();
const searchEndpoints = vi.fn();
const getEndpointByName = vi.fn();
vi.mock('../../api/AgentBricksService', () => ({
  AgentBricksService: {
    getEndpoints: (...a: unknown[]) => getEndpoints(...a),
    searchEndpoints: (...a: unknown[]) => searchEndpoints(...a),
    getEndpointByName: (...a: unknown[]) => getEndpointByName(...a),
    // Mirrors the real helper: READY state → ready.
    isEndpointReady: (e: AgentBricksEndpoint) => e.state === 'READY',
  },
}));

// jsdom lacks ResizeObserver, which MUI Autocomplete's popper relies on.
beforeEach(() => {
  if (!(window as unknown as { ResizeObserver?: unknown }).ResizeObserver) {
    (window as unknown as { ResizeObserver: unknown }).ResizeObserver = class {
      observe() {}
      unobserve() {}
      disconnect() {}
    };
  }
});

const READY_WITH_DISPLAY: AgentBricksEndpoint = {
  id: 'ep1',
  name: 'agents_sales_bot',
  display_name: 'Sales Bot',
  state: 'READY',
};
const READY_NO_DISPLAY: AgentBricksEndpoint = {
  id: 'ep2',
  name: 'agents_support_bot',
  state: 'READY',
};
const NOT_READY: AgentBricksEndpoint = {
  id: 'ep3',
  name: 'agents_pending_bot',
  display_name: 'Pending Bot',
  state: 'UPDATING',
};

const resolveEndpoints = (endpoints: AgentBricksEndpoint[]) =>
  getEndpoints.mockResolvedValue({ endpoints, total_count: endpoints.length, filtered: false });

beforeEach(() => {
  vi.clearAllMocks();
  resolveEndpoints([READY_WITH_DISPLAY, READY_NO_DISPLAY, NOT_READY]);
  searchEndpoints.mockResolvedValue({ endpoints: [], total_count: 0, filtered: false });
  getEndpointByName.mockResolvedValue(READY_WITH_DISPLAY);
  localStorage.clear();
});

// Open the dropdown so the options (and renderOption) are mounted.
const openDropdown = async () => {
  // The combobox role is the Autocomplete's text input.
  fireEvent.mouseDown(screen.getByRole('combobox'));
  await waitFor(() => expect(getEndpoints).toHaveBeenCalled());
};

describe('AgentBricksEndpointSelector', () => {
  it('renders the display_name as the option label when present', async () => {
    render(<AgentBricksEndpointSelector value={null} onChange={vi.fn()} />);
    await openDropdown();

    // listbox option renders the friendly tile name…
    expect(await screen.findByText('Sales Bot')).toBeInTheDocument();
  });

  it('renders the raw endpoint name as a secondary caption when it differs from display_name', async () => {
    render(<AgentBricksEndpointSelector value={null} onChange={vi.fn()} />);
    await openDropdown();

    // Both the friendly label AND the underlying name appear for the same row.
    expect(await screen.findByText('Sales Bot')).toBeInTheDocument();
    expect(screen.getByText('agents_sales_bot')).toBeInTheDocument();
  });

  it('renders the name as the label when no display_name is set (and no caption)', async () => {
    resolveEndpoints([READY_NO_DISPLAY]);
    render(<AgentBricksEndpointSelector value={null} onChange={vi.fn()} />);
    await openDropdown();

    // The name is used as the label…
    expect(await screen.findByText('agents_support_bot')).toBeInTheDocument();
    // …and there is no separate caption duplicating it (only one occurrence).
    expect(screen.getAllByText('agents_support_bot')).toHaveLength(1);
  });

  it('shows a "Ready" chip only for READY endpoints', async () => {
    render(<AgentBricksEndpointSelector value={null} onChange={vi.fn()} />);
    await openDropdown();

    // Two of the three seeded endpoints are READY.
    expect(await screen.findByText('Sales Bot')).toBeInTheDocument();
    expect(screen.getAllByText('Ready')).toHaveLength(2);
    // The non-ready endpoint's row renders without a chip.
    expect(screen.getByText('Pending Bot')).toBeInTheDocument();
  });

  it('uses display_name||name for the rendered selection label (getOptionLabel)', async () => {
    // A preselected value is hydrated via getEndpointByName and shown in the input.
    render(<AgentBricksEndpointSelector value="agents_sales_bot" onChange={vi.fn()} />);

    const input = await screen.findByRole('combobox');
    await waitFor(() => expect((input as HTMLInputElement).value).toBe('Sales Bot'));
  });
});
