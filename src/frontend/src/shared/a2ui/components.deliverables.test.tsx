import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { A2UIRenderer } from './A2UIRenderer';
import type { Surface } from './types';

// The Map renderer lazy-loads react-leaflet, which needs a real DOM/canvas. Stub
// it so these tests exercise the GeoMap wrapper (legend, empty-guard) without
// pulling leaflet into jsdom.
vi.mock('./LeafletMap', () => ({ default: () => <div data-testid="leaflet-stub" /> }));

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
