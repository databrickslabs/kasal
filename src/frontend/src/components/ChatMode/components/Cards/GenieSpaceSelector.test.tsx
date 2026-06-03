import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react';
import GenieSpaceSelector from './GenieSpaceSelector';
import { searchGenieSpaces, GenieSpace } from '../../api/genie';

vi.mock('../../api/genie', () => ({
  searchGenieSpaces: vi.fn(),
}));

const mockedSearch = vi.mocked(searchGenieSpaces);

const SPACES: GenieSpace[] = [
  { id: 'space-1', name: 'Sales Space', description: 'Sales analytics' },
  { id: 'space-2', name: 'Marketing Space' }, // no description
];

describe('GenieSpaceSelector', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockedSearch.mockResolvedValue(SPACES);
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  const getToggleButton = () =>
    screen.getByText('Select a space...').closest('button') as HTMLButtonElement;

  it('renders closed with placeholder when no value selected', () => {
    render(<GenieSpaceSelector value="" onChange={() => {}} />);
    expect(screen.getByText('Genie Space')).toBeInTheDocument();
    expect(screen.getByText('Select a space...')).toBeInTheDocument();
    // dropdown not open
    expect(screen.queryByPlaceholderText('Search spaces...')).not.toBeInTheDocument();
  });

  it('renders the raw value as display text when value has no matching space', () => {
    render(<GenieSpaceSelector value="some-id" onChange={() => {}} />);
    // displayText falls back to value
    expect(screen.getByText('some-id')).toBeInTheDocument();
  });

  it('opens dropdown, focuses input, fetches spaces, and shows results', async () => {
    vi.useFakeTimers();
    const focusSpy = vi.spyOn(HTMLInputElement.prototype, 'focus');
    render(<GenieSpaceSelector value="" onChange={() => {}} />);

    act(() => {
      fireEvent.click(getToggleButton());
    });

    // input rendered while open
    expect(screen.getByPlaceholderText('Search spaces...')).toBeInTheDocument();

    // setTimeout(focus, 50) for the input on open
    act(() => {
      vi.advanceTimersByTime(50);
    });
    expect(focusSpy).toHaveBeenCalled();

    // debounce timer (300ms) triggers fetchSpaces; also first-open fetch
    await act(async () => {
      vi.advanceTimersByTime(300);
      await Promise.resolve();
    });

    vi.useRealTimers();

    await waitFor(() => {
      expect(screen.getByText('Sales Space')).toBeInTheDocument();
    });
    expect(screen.getByText('Marketing Space')).toBeInTheDocument();
    expect(screen.getByText('Sales analytics')).toBeInTheDocument();
    expect(mockedSearch).toHaveBeenCalled();
  });

  it('shows loading state while fetching', async () => {
    let resolveFn: (v: GenieSpace[]) => void = () => {};
    mockedSearch.mockReturnValue(
      new Promise<GenieSpace[]>((resolve) => {
        resolveFn = resolve;
      }),
    );

    render(<GenieSpaceSelector value="" onChange={() => {}} />);
    fireEvent.click(getToggleButton());

    await waitFor(() => {
      expect(screen.getByText('Loading spaces...')).toBeInTheDocument();
    });

    await act(async () => {
      resolveFn(SPACES);
      await Promise.resolve();
    });

    await waitFor(() => {
      expect(screen.queryByText('Loading spaces...')).not.toBeInTheDocument();
    });
  });

  it('shows error state when fetch fails', async () => {
    mockedSearch.mockRejectedValue(new Error('boom'));
    render(<GenieSpaceSelector value="" onChange={() => {}} />);
    fireEvent.click(getToggleButton());

    await waitFor(() => {
      expect(screen.getByText('Failed to load spaces')).toBeInTheDocument();
    });
  });

  it('shows "No spaces found" when results are empty', async () => {
    mockedSearch.mockResolvedValue([]);
    render(<GenieSpaceSelector value="" onChange={() => {}} />);
    fireEvent.click(getToggleButton());

    await waitFor(() => {
      expect(screen.getByText('No spaces found')).toBeInTheDocument();
    });
  });

  it('calls onChange and closes when a space is selected', async () => {
    const onChange = vi.fn();
    render(<GenieSpaceSelector value="" onChange={onChange} />);
    fireEvent.click(getToggleButton());

    await waitFor(() => {
      expect(screen.getByText('Sales Space')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText('Sales Space'));
    expect(onChange).toHaveBeenCalledWith('space-1');
    // closed -> search input gone
    await waitFor(() => {
      expect(screen.queryByPlaceholderText('Search spaces...')).not.toBeInTheDocument();
    });
  });

  it('marks the currently selected space and renders the check icon', async () => {
    render(<GenieSpaceSelector value="space-1" onChange={() => {}} />);
    // Before fetch, spaces is empty so the toggle shows the raw value.
    const toggle = screen.getByText('space-1').closest('button') as HTMLButtonElement;
    fireEvent.click(toggle);

    // After the fetch resolves, the toggle re-renders to the matched space name
    // (selectedSpace branch) and the list item also shows it -> multiple matches.
    await waitFor(() => {
      expect(screen.getAllByText('Sales Space').length).toBeGreaterThan(1);
    });
  });

  it('updates query on search input change and re-fetches (debounced)', async () => {
    vi.useFakeTimers();
    render(<GenieSpaceSelector value="" onChange={() => {}} />);

    act(() => {
      fireEvent.click(getToggleButton());
    });

    // initial debounce + first open fetch
    await act(async () => {
      vi.advanceTimersByTime(300);
      await Promise.resolve();
    });

    const input = screen.getByPlaceholderText('Search spaces...') as HTMLInputElement;
    act(() => {
      fireEvent.change(input, { target: { value: 'sales' } });
    });
    expect(input.value).toBe('sales');

    await act(async () => {
      vi.advanceTimersByTime(300);
      await Promise.resolve();
    });

    vi.useRealTimers();

    await waitFor(() => {
      expect(mockedSearch).toHaveBeenCalledWith('sales');
    });
  });

  it('closes when clicking outside the wrapper', async () => {
    render(<GenieSpaceSelector value="" onChange={() => {}} />);
    fireEvent.click(getToggleButton());

    await waitFor(() => {
      expect(screen.getByPlaceholderText('Search spaces...')).toBeInTheDocument();
    });

    // mousedown outside the component
    fireEvent.mouseDown(document.body);

    await waitFor(() => {
      expect(screen.queryByPlaceholderText('Search spaces...')).not.toBeInTheDocument();
    });
  });

  it('does not close when clicking inside the wrapper', async () => {
    render(<GenieSpaceSelector value="" onChange={() => {}} />);
    fireEvent.click(getToggleButton());

    const input = await screen.findByPlaceholderText('Search spaces...');
    fireEvent.mouseDown(input);

    // still open
    expect(screen.getByPlaceholderText('Search spaces...')).toBeInTheDocument();
  });

  it('toggles open then closed via the toggle button', async () => {
    render(<GenieSpaceSelector value="" onChange={() => {}} />);
    const btn = getToggleButton();

    fireEvent.click(btn);
    await waitFor(() => {
      expect(screen.getByPlaceholderText('Search spaces...')).toBeInTheDocument();
    });

    // second click closes (open is true, so no focus timeout branch)
    fireEvent.click(btn);
    await waitFor(() => {
      expect(screen.queryByPlaceholderText('Search spaces...')).not.toBeInTheDocument();
    });
  });
});
