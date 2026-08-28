import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import CatalogLibrary from './CatalogLibrary';
import { CatalogItem } from '../api/crews';
import { useAppStore } from '../store/appStore';

const crews = (n: number): CatalogItem[] =>
  Array.from({ length: n }, (_, i) => ({ id: `c${i}`, name: `Crew ${i}` }));
const flows = (n: number): CatalogItem[] =>
  Array.from({ length: n }, (_, i) => ({ id: `f${i}`, name: `Flow ${i}` }));

const renderLib = (over?: {
  crews?: CatalogItem[];
  flows?: CatalogItem[];
  onLoadCrew?: (name: string) => void;
  onLoadFlow?: (name: string) => void;
}) => {
  const onLoadCrew = over?.onLoadCrew ?? vi.fn();
  const onLoadFlow = over?.onLoadFlow ?? vi.fn();
  render(
    <CatalogLibrary
      crews={over?.crews ?? []}
      flows={over?.flows ?? []}
      onLoadCrew={onLoadCrew}
      onLoadFlow={onLoadFlow}
    />,
  );
  return { onLoadCrew, onLoadFlow };
};

const header = () => screen.getByText('Catalog');

describe('CatalogLibrary', () => {
  beforeEach(() => {
    // Expansion lives in the app store now (so the collapsed rail can open it);
    // reset it or one test's toggle leaks into the next.
    useAppStore.setState({ catalogOpen: false });
  });

  it('renders nothing when both crews and flows are empty', () => {
    const { container } = render(
      <CatalogLibrary crews={[]} flows={[]} onLoadCrew={vi.fn()} onLoadFlow={vi.fn()} />,
    );
    expect(container.firstChild).toBeNull();
    expect(screen.queryByText('Catalog')).not.toBeInTheDocument();
  });

  it('is collapsed by default — the header shows but item buttons are not visible', () => {
    renderLib({ crews: crews(1), flows: flows(1) });
    expect(header()).toBeInTheDocument();
    expect(header().closest('button')).toHaveAttribute('aria-expanded', 'false');
    expect(screen.queryByText('Crew 0')).not.toBeInTheDocument();
    expect(screen.queryByText('Flow 0')).not.toBeInTheDocument();
  });

  it('shows the total count badge (crews + flows)', () => {
    renderLib({ crews: crews(2), flows: flows(3) });
    expect(screen.getByText('5')).toBeInTheDocument();
  });

  it('expands on header click, showing crews and flows mixed together', () => {
    renderLib({ crews: crews(2), flows: flows(1) });
    fireEvent.click(header());
    expect(header().closest('button')).toHaveAttribute('aria-expanded', 'true');
    expect(screen.getByText('Crew 0')).toBeInTheDocument();
    expect(screen.getByText('Crew 1')).toBeInTheDocument();
    expect(screen.getByText('Flow 0')).toBeInTheDocument();
  });

  it('clicking a crew item calls onLoadCrew with its name', () => {
    const { onLoadCrew, onLoadFlow } = renderLib({ crews: crews(1), flows: flows(1) });
    fireEvent.click(header());
    fireEvent.click(screen.getByText('Crew 0'));
    expect(onLoadCrew).toHaveBeenCalledTimes(1);
    expect(onLoadCrew).toHaveBeenCalledWith('Crew 0');
    expect(onLoadFlow).not.toHaveBeenCalled();
  });

  it('clicking a flow item calls onLoadFlow with its name', () => {
    const { onLoadCrew, onLoadFlow } = renderLib({ crews: crews(1), flows: flows(1) });
    fireEvent.click(header());
    fireEvent.click(screen.getByText('Flow 0'));
    expect(onLoadFlow).toHaveBeenCalledTimes(1);
    expect(onLoadFlow).toHaveBeenCalledWith('Flow 0');
    expect(onLoadCrew).not.toHaveBeenCalled();
  });

  it('does not render the search box when there are 6 or fewer entries', () => {
    renderLib({ crews: crews(4), flows: flows(2) }); // 6 total → no search
    fireEvent.click(header());
    expect(screen.queryByPlaceholderText('Search catalog…')).not.toBeInTheDocument();
  });

  it('renders the search box when there are more than 6 entries and filters case-insensitively', () => {
    renderLib({ crews: crews(5), flows: flows(3) }); // 8 total → search shown
    fireEvent.click(header());
    const search = screen.getByPlaceholderText('Search catalog…');
    expect(search).toBeInTheDocument();

    // Case-insensitive match: "flow 1" should match "Flow 1" only.
    fireEvent.change(search, { target: { value: 'flow 1' } });
    expect(screen.getByText('Flow 1')).toBeInTheDocument();
    expect(screen.queryByText('Crew 0')).not.toBeInTheDocument();
    expect(screen.queryByText('Flow 0')).not.toBeInTheDocument();
  });

  it('shows "No matches" when the query matches nothing', () => {
    renderLib({ crews: crews(5), flows: flows(3) });
    fireEvent.click(header());
    const search = screen.getByPlaceholderText('Search catalog…');
    fireEvent.change(search, { target: { value: 'zzz-nothing' } });
    expect(screen.getByText('No matches')).toBeInTheDocument();
    expect(screen.queryByText('Crew 0')).not.toBeInTheDocument();
  });

  it('caps the unsearched list at 50 rows and says how to see the rest', () => {
    renderLib({ crews: crews(80) });
    fireEvent.click(header());
    expect(screen.getByText('Crew 49')).toBeInTheDocument();
    expect(screen.queryByText('Crew 50')).toBeNull();
    expect(screen.getByText('Showing 50 of 80 — search to narrow')).toBeInTheDocument();
    // A query lifts the cap — filtered results are what the user asked for.
    fireEvent.change(screen.getByPlaceholderText('Search catalog…'), { target: { value: 'Crew 7' } });
    expect(screen.getByText('Crew 79')).toBeInTheDocument();
    expect(screen.queryByText(/search to narrow/)).toBeNull();
  });

  it('toggling open then closed hides the items again', () => {
    renderLib({ crews: crews(1), flows: flows(1) });
    fireEvent.click(header()); // open
    expect(screen.getByText('Crew 0')).toBeInTheDocument();
    fireEvent.click(header()); // close
    expect(screen.queryByText('Crew 0')).not.toBeInTheDocument();
    expect(header().closest('button')).toHaveAttribute('aria-expanded', 'false');
  });
});
