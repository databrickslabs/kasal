import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import CrewCatalogActions from './CrewCatalogActions';
import type { CrewResponse } from '../../../../../types/workflow/crew';

vi.mock('./PublishButton', () => ({ default: () => <button>Publish</button> }));
vi.mock('../../../export/components/ExportCrewDialog', () => ({
  default: ({ crewId, crewName, onClose }: { crewId: string; crewName: string; onClose: () => void }) =>
    <div role="dialog" aria-label={crewName}><span>{crewId}</span><button onClick={onClose}>Close deployment</button></div>,
}));

const crew = { id: 'catalog-crew', name: 'Saved research crew', nodes: [] } as unknown as CrewResponse;
const props = { crew, canEdit: true, canDelete: true, mlflowEnabled: false, published: false, onPublished: vi.fn(), onOptimize: vi.fn(), onExport: vi.fn(), onDelete: vi.fn() };

describe('crew catalog deployment', () => {
  it('opens deployment for the clicked crew without loading it onto the canvas', () => {
    const load = vi.fn();
    render(<div onClick={load}><CrewCatalogActions {...props} /></div>);
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Deploy to Databricks Apps' }));
    expect(screen.getByRole('dialog', { name: crew.name })).toHaveTextContent('catalog-crew');
    expect(load).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole('button', { name: 'Close deployment' }));
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    expect(load).not.toHaveBeenCalled();
  });

  it('keeps deployment hidden from operators', () => {
    render(<CrewCatalogActions {...props} canEdit={false} canDelete={false} />);
    expect(screen.queryByRole('button', { name: 'Deploy to Databricks Apps' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Publish' })).not.toBeInTheDocument();
  });
});
