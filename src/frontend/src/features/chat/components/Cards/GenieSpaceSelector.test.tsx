import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react';
import GenieSpaceSelector from './GenieSpaceSelector';
import { searchGenieSpaces, getGenieSpace, GenieSpace } from '../../api/genie';

vi.mock('../../api/genie', () => ({
  searchGenieSpaces: vi.fn(),
  getGenieSpace: vi.fn(async () => null),
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

  it('resolves the selected space deep link from the loaded list and renders open-in-Databricks links', async () => {
    mockedSearch.mockResolvedValue([
      { id: 'space-1', name: 'Sales Space', url: 'https://dbx.example/genie/space-1' },
    ]);
    render(<GenieSpaceSelector value="space-1" onChange={() => {}} />);
    const toggle = screen.getByText('space-1').closest('button') as HTMLButtonElement;
    fireEvent.click(toggle); // open → fetch → spaces (with url) populate the list

    // per-row "open in Databricks" link (space.url present)
    await waitFor(() => {
      expect(screen.getByLabelText('Open Sales Space in Databricks')).toBeInTheDocument();
    });
    // clicking it stops propagation (doesn't select the row) — exercises onClick
    const onChange = vi.fn();
    fireEvent.click(screen.getByLabelText('Open Sales Space in Databricks'));
    expect(onChange).not.toHaveBeenCalled();
    // selected-space deep link resolved from the loaded list (value && selectedUrl)
    await waitFor(() => {
      const link = screen.getByLabelText('Open the selected space in Databricks');
      expect(link).toHaveAttribute('href', 'https://dbx.example/genie/space-1');
    });
  });

  it('fetches the selected space deep link via getGenieSpace when it is not in the list', async () => {
    vi.mocked(getGenieSpace).mockResolvedValueOnce({
      id: 'space-9',
      name: 'Remote',
      url: 'https://dbx.example/genie/space-9',
    });
    render(<GenieSpaceSelector value="space-9" onChange={() => {}} />);
    await waitFor(() => {
      expect(screen.getByLabelText('Open the selected space in Databricks')).toHaveAttribute(
        'href',
        'https://dbx.example/genie/space-9',
      );
    });
  });

  it('ignores a late getGenieSpace result after the effect is cleaned up', async () => {
    let resolveFn: (s: GenieSpace | null) => void = () => {};
    vi.mocked(getGenieSpace).mockReturnValueOnce(
      new Promise<GenieSpace | null>((r) => {
        resolveFn = r;
      }),
    );
    const { rerender } = render(<GenieSpaceSelector value="space-x" onChange={() => {}} />);
    // value change → the space-x effect is cleaned up (cancelled = true)
    rerender(<GenieSpaceSelector value="" onChange={() => {}} />);
    await act(async () => {
      resolveFn({ id: 'space-x', name: 'X', url: 'https://late.example' }); // stale result
      await Promise.resolve();
    });
    // stale result ignored → no selected-space link
    expect(screen.queryByLabelText('Open the selected space in Databricks')).toBeNull();
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
