import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { A2UIRenderer } from './A2UIRenderer';
import type { Surface } from './types';

// The Map renderer lazy-loads react-leaflet, which needs a real DOM/canvas. Stub
// it so these tests exercise the GeoMap wrapper (legend, empty-guard) without
// pulling leaflet into jsdom.
vi.mock('./LeafletMap', () => ({ default: () => <div data-testid="leaflet-stub" /> }));

// recharts' ResponsiveContainer uses ResizeObserver, absent in jsdom — polyfill it
// so the Forecast chart (recharts) can mount without throwing.
if (!('ResizeObserver' in globalThis)) {
  (globalThis as unknown as { ResizeObserver: unknown }).ResizeObserver = class {
    observe() {}
    unobserve() {}
    disconnect() {}
  };
}

const flashcards: Surface = {
  surfaceKind: 'flashcards',
  root: 'fc',
  dataModel: {
    cards: [
      { front: 'Capital of France?', back: 'Paris', hint: 'City of light' },
      { front: '2 + 2', back: '4' },
    ],
  },
  components: [{ id: 'fc', component: 'Flashcards', title: 'Study deck', cards: { path: '/cards' } }],
};

describe('Flashcards', () => {
  it('renders the deck with the card counter, front, hint and a flip control', () => {
    render(<A2UIRenderer payload={flashcards} />);
    expect(screen.getByText('Study deck')).toBeInTheDocument();
    expect(screen.getByText('Card 1 of 2')).toBeInTheDocument();
    expect(screen.getByText('Capital of France?')).toBeInTheDocument();
    expect(screen.getByText(/City of light/)).toBeInTheDocument();
    // Back face is in the DOM (CSS 3D flip) and the card is flippable.
    expect(screen.getByText('Paris')).toBeInTheDocument();
    expect(screen.getByLabelText('Flip card')).toBeInTheDocument();
  });

  it('renders nothing when there are no cards', () => {
    const empty: Surface = {
      surfaceKind: 'flashcards',
      root: 'fc',
      dataModel: { cards: [] },
      components: [{ id: 'fc', component: 'Flashcards', cards: { path: '/cards' } }],
    };
    render(<A2UIRenderer payload={empty} />);
    expect(screen.queryByText(/Card 1 of/)).toBeNull();
  });
});

const map: Surface = {
  surfaceKind: 'map',
  root: 'm',
  dataModel: {
    points: [
      { lat: 47.37, lng: 8.54, label: 'Zurich', value: 5 },
      { lat: 46.2, lng: 6.14, label: 'Geneva', value: 3 },
    ],
  },
  components: [{ id: 'm', component: 'Map', title: 'Swiss cities', points: { path: '/points' } }],
};

describe('Map (GeoMap)', () => {
  it('renders a legend with each point label (outside the lazy map)', () => {
    render(<A2UIRenderer payload={map} />);
    expect(screen.getByText('Swiss cities')).toBeInTheDocument();
    expect(screen.getByText('Zurich')).toBeInTheDocument();
    expect(screen.getByText('Geneva')).toBeInTheDocument();
  });

  it('renders nothing when there are no valid coordinates', () => {
    const empty: Surface = {
      surfaceKind: 'map',
      root: 'm',
      dataModel: { points: [{ label: 'no coords' }] },
      components: [{ id: 'm', component: 'Map', title: 'Empty', points: { path: '/points' } }],
    };
    render(<A2UIRenderer payload={empty} />);
    expect(screen.queryByText('Empty')).toBeNull();
  });
});

// Flip interaction shouldn't throw and keeps the card counter stable.
describe('Flashcards — flip interaction', () => {
  it('toggles without crashing', () => {
    render(<A2UIRenderer payload={flashcards} />);
    fireEvent.click(screen.getByLabelText('Flip card'));
    expect(screen.getByText('Card 1 of 2')).toBeInTheDocument();
  });
});

const tableSurface = (rows: unknown[]): Surface => ({
  surfaceKind: 'dashboard',
  root: 'tbl',
  dataModel: { rows },
  components: [
    { id: 'tbl', component: 'Table', columns: ['Product', 'Total DBUs'], rows: { path: '/rows' } },
  ],
});

// Read the first-column (Product) text of each body row, top to bottom.
const productOrder = () =>
  screen.getAllByRole('row').slice(1).map((r) => r.querySelector('td')?.textContent);

describe('Table (sort + filter)', () => {
  it('sorts a figure column numerically, cycling asc → desc → original order', () => {
    render(
      <A2UIRenderer
        payload={tableSurface([
          ['SQL', '~453,163'],
          ['MODEL_SERVING', '~905,930'],
          ['JOBS', '~46,613'],
        ])}
      />,
    );
    expect(productOrder()).toEqual(['SQL', 'MODEL_SERVING', 'JOBS']);
    const header = screen.getByLabelText('Sort by Total DBUs');
    // Ascending: smallest figure first (parses "~46,613" as 46613, not lexically).
    fireEvent.click(header);
    expect(productOrder()).toEqual(['JOBS', 'SQL', 'MODEL_SERVING']);
    // Descending.
    fireEvent.click(header);
    expect(productOrder()).toEqual(['MODEL_SERVING', 'SQL', 'JOBS']);
    // Back to document order.
    fireEvent.click(header);
    expect(productOrder()).toEqual(['SQL', 'MODEL_SERVING', 'JOBS']);
  });

  it('shows a filter box for large tables and narrows the rows by substring', () => {
    const rows = Array.from({ length: 12 }, (_, i) => [`PROD_${i}`, `~${i}`]);
    rows.push(['MODEL_SERVING', '~905,930']);
    render(<A2UIRenderer payload={tableSurface(rows)} />);
    const filter = screen.getByLabelText('Filter table rows');
    fireEvent.change(filter, { target: { value: 'model' } });
    expect(productOrder()).toEqual(['MODEL_SERVING']);
    expect(screen.getByText('1 of 13 rows')).toBeInTheDocument();
  });

  it('hides the filter box for small tables', () => {
    render(<A2UIRenderer payload={tableSurface([['SQL', '~1'], ['JOBS', '~2']])} />);
    expect(screen.queryByLabelText('Filter table rows')).toBeNull();
  });
});

// ---- Table (header resolves through a dataModel binding) ----
describe('Table header binding', () => {
  it('renders the header when columns is a {path} binding, not just a literal array', () => {
    const s: Surface = {
      surfaceKind: 'dashboard',
      root: 't',
      dataModel: {
        cols: ['Month', 'North America', 'EMEA'],
        rows: [['Jul 2023', 504.7, 418], ['Aug 2023', 442.8, 338.3]],
      },
      components: [{ id: 't', component: 'Table', columns: { path: '/cols' }, rows: { path: '/rows' } }],
    };
    render(<A2UIRenderer payload={s} />);
    // Header cells (each a sort button) show the bound column names.
    expect(screen.getByLabelText('Sort by Month')).toBeInTheDocument();
    expect(screen.getByLabelText('Sort by North America')).toBeInTheDocument();
    expect(screen.getByLabelText('Sort by EMEA')).toBeInTheDocument();
  });
});

// ---- Forecast (recharts; assert the wrapper + empty guard, not SVG internals) ----
describe('Forecast', () => {
  it('renders the title for schema-agnostic forecast rows', () => {
    const s: Surface = {
      surfaceKind: 'dashboard',
      root: 'f',
      dataModel: {
        rows: [
          { ds: '2024-06-30', risk_category: 'Credit Risk', default_rate_forecast: 0.021, default_rate_upper: 0.024, default_rate_lower: 0.017 },
          { ds: '2024-07-07', risk_category: 'Credit Risk', default_rate_forecast: 0.022, default_rate_upper: 0.025, default_rate_lower: 0.018 },
        ],
      },
      components: [{ id: 'f', component: 'Forecast', title: 'Default rate forecast', data: { path: '/rows' } }],
    };
    render(<A2UIRenderer payload={s} />);
    expect(screen.getByText('Default rate forecast')).toBeInTheDocument();
  });

  it('renders nothing when there are no rows', () => {
    const s: Surface = {
      surfaceKind: 'dashboard',
      root: 'f',
      dataModel: { rows: [] },
      components: [{ id: 'f', component: 'Forecast', title: 'Empty', data: { path: '/rows' } }],
    };
    render(<A2UIRenderer payload={s} />);
    expect(screen.queryByText('Empty')).toBeNull();
  });
});

// ---- Graph (node-link SVG) ----
describe('Graph', () => {
  const graph: Surface = {
    surfaceKind: 'document',
    root: 'g',
    dataModel: {
      nodes: [{ id: 'a', label: 'Alpha' }, { id: 'b', label: 'Beta' }, { id: 'c', label: 'Gamma' }],
      edges: [{ from: 'a', to: 'b', label: 'calls' }, { from: 'b', to: 'c' }],
    },
    components: [{ id: 'g', component: 'Graph', title: 'Deps', nodes: { path: '/nodes' }, edges: { path: '/edges' } }],
  };

  it('renders a labelled node per node and a line per valid edge', () => {
    const { container } = render(<A2UIRenderer payload={graph} />);
    expect(screen.getByText('Alpha')).toBeInTheDocument();
    expect(screen.getByText('Beta')).toBeInTheDocument();
    expect(screen.getByText('Gamma')).toBeInTheDocument();
    expect(screen.getByText('calls')).toBeInTheDocument();
    expect(container.querySelectorAll('svg line')).toHaveLength(2);
    expect(container.querySelectorAll('svg circle')).toHaveLength(3);
  });

  it('drops edges that reference unknown nodes', () => {
    const bad: Surface = {
      ...graph,
      dataModel: { nodes: [{ id: 'a', label: 'Alpha' }], edges: [{ from: 'a', to: 'ghost' }] },
    };
    const { container } = render(<A2UIRenderer payload={bad} />);
    expect(container.querySelectorAll('svg line')).toHaveLength(0);
  });

  it('renders nothing with no nodes', () => {
    const empty: Surface = { ...graph, dataModel: { nodes: [], edges: [] } };
    render(<A2UIRenderer payload={empty} />);
    expect(screen.queryByText('Deps')).toBeNull();
  });
});

// ---- Sequence diagram (SVG) ----
describe('Sequence', () => {
  it('renders actor labels and message texts, backfilling actors seen only in messages', () => {
    const seq: Surface = {
      surfaceKind: 'document',
      root: 's',
      dataModel: {
        actors: ['User', 'API'],
        messages: [
          { from: 'User', to: 'API', text: 'GET /data' },
          { from: 'API', to: 'DB', text: 'query' },
          { from: 'DB', to: 'API', text: 'rows', dashed: true },
        ],
      },
      components: [{ id: 's', component: 'Sequence', title: 'Flow', actors: { path: '/actors' }, messages: { path: '/messages' } }],
    };
    render(<A2UIRenderer payload={seq} />);
    expect(screen.getByText('User')).toBeInTheDocument();
    expect(screen.getByText('API')).toBeInTheDocument();
    expect(screen.getByText('DB')).toBeInTheDocument(); // backfilled from messages
    expect(screen.getByText('GET /data')).toBeInTheDocument();
    expect(screen.getByText('rows')).toBeInTheDocument();
  });
});

// ---- Diagram (archetype-based business diagrams) ----
describe('Diagram', () => {
  const diagram = (archetype: string, items: unknown[], extra: Record<string, unknown> = {}): Surface => ({
    surfaceKind: 'document',
    root: 'dg',
    dataModel: { items },
    components: [{ id: 'dg', component: 'Diagram', archetype, title: 'The picture', items: { path: '/items' }, ...extra }],
  });

  it('renders a process as numbered steps with labels and details', () => {
    render(
      <A2UIRenderer
        payload={diagram('process', [
          { label: 'Ingest', detail: 'Land the raw data' },
          { label: 'Transform' },
          { label: 'Serve' },
        ])}
      />,
    );
    expect(screen.getByText('The picture')).toBeInTheDocument();
    expect(screen.getByText('Step 1')).toBeInTheDocument();
    expect(screen.getByText('Step 3')).toBeInTheDocument();
    expect(screen.getByText('Ingest')).toBeInTheDocument();
    expect(screen.getByText('Land the raw data')).toBeInTheDocument();
    expect(screen.getByText('Serve')).toBeInTheDocument();
  });

  it('renders a comparison with a VS badge and per-side points', () => {
    render(
      <A2UIRenderer
        payload={diagram('comparison', [
          { label: 'Build', points: ['Full control', 'Slower to ship'] },
          { label: 'Buy', points: ['Fast start'] },
        ])}
      />,
    );
    expect(screen.getByText('Build')).toBeInTheDocument();
    expect(screen.getByText('Buy')).toBeInTheDocument();
    expect(screen.getByText('vs')).toBeInTheDocument();
    expect(screen.getByText('Full control')).toBeInTheDocument();
    expect(screen.getByText('Fast start')).toBeInTheDocument();
  });

  it('renders a hierarchy with root, children and grandchildren', () => {
    render(
      <A2UIRenderer
        payload={diagram('hierarchy', [
          {
            label: 'CEO',
            children: [
              { label: 'Engineering', children: [{ label: 'Platform' }] },
              { label: 'Sales' },
            ],
          },
        ])}
      />,
    );
    expect(screen.getByText('CEO')).toBeInTheDocument();
    expect(screen.getByText('Engineering')).toBeInTheDocument();
    expect(screen.getByText('Platform')).toBeInTheDocument();
  });

  it('maps archetype synonyms instead of rendering Unsupported', () => {
    render(<A2UIRenderer payload={diagram('orgchart', [{ label: 'Root' }])} />);
    expect(screen.getByText('Root')).toBeInTheDocument();
    expect(screen.queryByText(/Unsupported component/)).toBeNull();
  });

  it('renders a matrix2x2 with axis labels and nothing when items are empty', () => {
    const { rerender } = render(
      <A2UIRenderer
        payload={diagram(
          'matrix2x2',
          [{ label: 'Quick win' }, { label: 'Big bet' }, { label: 'Fill-in' }, { label: 'Money pit' }],
          { xLabel: 'Effort', yLabel: 'Impact' },
        )}
      />,
    );
    expect(screen.getByText('Quick win')).toBeInTheDocument();
    expect(screen.getByText(/Effort/)).toBeInTheDocument();
    expect(screen.getByText(/Impact/)).toBeInTheDocument();
    rerender(<A2UIRenderer payload={diagram('matrix2x2', [])} />);
    expect(screen.queryByText('The picture')).toBeNull();
  });
});

// ---- Chart (new chartTypes render without crashing; recharts internals not
// assertable in jsdom — assert the title + no Unsupported) ----
describe('Chart — area/scatter/radar', () => {
  const chart = (chartType: string): Surface => ({
    surfaceKind: 'dashboard',
    root: 'c',
    dataModel: { rows: [{ month: 'Jan', a: 1, b: 2 }, { month: 'Feb', a: 3, b: 4 }] },
    components: [
      { id: 'c', component: 'Chart', chartType, title: `${chartType} chart`, xKey: 'month', yKeys: ['a', 'b'], data: { path: '/rows' } },
    ],
  });

  it.each(['area', 'scatter', 'radar'])('mounts a %s chart', (t) => {
    render(<A2UIRenderer payload={chart(t)} />);
    expect(screen.getByText(`${t} chart`)).toBeInTheDocument();
    expect(screen.queryByText(/Unsupported component/)).toBeNull();
  });
});

// ---- KeyValue icon (curated allowlist) ----
describe('KeyValue icon', () => {
  const kv = (icon?: string): Surface => ({
    surfaceKind: 'dashboard',
    root: 'k',
    dataModel: {},
    components: [{ id: 'k', component: 'KeyValue', label: 'Revenue', value: '$1.2M', ...(icon ? { icon } : {}) }],
  });

  it('renders a lucide icon for an allowlisted name', () => {
    const { container } = render(<A2UIRenderer payload={kv('trending-up')} />);
    expect(container.querySelector('svg.lucide-trending-up')).toBeTruthy();
    expect(screen.getByText('$1.2M')).toBeInTheDocument();
  });

  it('renders no icon for unknown names (never a broken glyph)', () => {
    const { container } = render(<A2UIRenderer payload={kv('made-up-icon')} />);
    expect(container.querySelector('svg')).toBeNull();
    expect(screen.getByText('Revenue')).toBeInTheDocument();
  });
});

// ---- Album (image carousel) ----
describe('Album', () => {
  const album: Surface = {
    surfaceKind: 'document',
    root: 'al',
    dataModel: {
      pics: [
        { src: 'https://example.com/1.jpg', caption: 'First' },
        { src: 'https://example.com/2.jpg', caption: 'Second' },
      ],
    },
    components: [{ id: 'al', component: 'Album', title: 'Gallery', items: { path: '/pics' } }],
  };

  it('shows one image at a time and advances with Next', () => {
    render(<A2UIRenderer payload={album} />);
    expect(screen.getByText('1 / 2')).toBeInTheDocument();
    expect(screen.getByRole('img')).toHaveAttribute('src', 'https://example.com/1.jpg');
    fireEvent.click(screen.getByLabelText('Next image'));
    expect(screen.getByText('2 / 2')).toBeInTheDocument();
    expect(screen.getByRole('img')).toHaveAttribute('src', 'https://example.com/2.jpg');
    // Wraps around.
    fireEvent.click(screen.getByLabelText('Next image'));
    expect(screen.getByText('1 / 2')).toBeInTheDocument();
  });

  it('accepts bare string URLs and renders nothing when empty', () => {
    const bare: Surface = { ...album, dataModel: { pics: ['https://example.com/x.jpg'] } };
    const { rerender } = render(<A2UIRenderer payload={bare} />);
    expect(screen.getByRole('img')).toHaveAttribute('src', 'https://example.com/x.jpg');
    // No nav for a single image.
    expect(screen.queryByLabelText('Next image')).toBeNull();
    const empty: Surface = { ...album, dataModel: { pics: [] } };
    rerender(<A2UIRenderer payload={empty} />);
    expect(screen.queryByText('Gallery')).toBeNull();
  });
});
