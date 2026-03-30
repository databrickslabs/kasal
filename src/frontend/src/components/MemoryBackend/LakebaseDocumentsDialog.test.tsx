import { describe, it, expect, vi, beforeEach, afterEach, Mock } from 'vitest';
import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import { MemoryBackendService } from '../../api/MemoryBackendService';

vi.mock('../../api/MemoryBackendService', () => ({
  MemoryBackendService: {
    getLakebaseTableData: vi.fn(),
  },
}));

import LakebaseDocumentsDialog from './LakebaseDocumentsDialog';

describe('LakebaseDocumentsDialog', () => {
  const defaultProps = {
    open: true,
    onClose: vi.fn(),
    tableName: 'crew_short_term_memory',
    memoryType: 'short_term' as const,
  };

  beforeEach(() => {
    vi.clearAllMocks();
    (MemoryBackendService.getLakebaseTableData as Mock).mockResolvedValue({
      success: true,
      documents: [
        {
          id: 'doc-1',
          crew_id: 'crew-abc',
          group_id: 'grp-1',
          session_id: 'sess-1',
          agent: 'Researcher',
          text: 'First document content',
          metadata: { key: 'value' },
          score: 0.95,
          created_at: '2026-02-28T20:00:00Z',
          updated_at: null,
        },
        {
          id: 'doc-2',
          crew_id: 'crew-abc',
          group_id: 'grp-1',
          session_id: 'sess-1',
          agent: 'Analyst',
          text: 'Second document content',
          metadata: {},
          score: null,
          created_at: null,
          updated_at: null,
        },
      ],
      total: 2,
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('renders dialog with title when open', async () => {
    render(<LakebaseDocumentsDialog {...defaultProps} />);
    expect(screen.getByText('Table Data')).toBeInTheDocument();
  });

  it('does not render when closed', () => {
    render(<LakebaseDocumentsDialog {...defaultProps} open={false} />);
    expect(screen.queryByText('Table Data')).not.toBeInTheDocument();
  });

  it('fetches documents on open', async () => {
    render(<LakebaseDocumentsDialog {...defaultProps} />);
    await waitFor(() => {
      expect(MemoryBackendService.getLakebaseTableData).toHaveBeenCalledWith(
        'crew_short_term_memory',
        50,
        undefined
      );
    });
  });

  it('fetches documents with instance name', async () => {
    render(
      <LakebaseDocumentsDialog
        {...defaultProps}
        instanceName="kasal-lakebase1"
      />
    );
    await waitFor(() => {
      expect(MemoryBackendService.getLakebaseTableData).toHaveBeenCalledWith(
        'crew_short_term_memory',
        50,
        'kasal-lakebase1'
      );
    });
  });

  it('displays documents after loading', async () => {
    render(<LakebaseDocumentsDialog {...defaultProps} />);
    await waitFor(() => {
      expect(screen.getByText('First document content')).toBeInTheDocument();
    });
    expect(screen.getByText('Second document content')).toBeInTheDocument();
  });

  it('shows record numbers', async () => {
    render(<LakebaseDocumentsDialog {...defaultProps} />);
    await waitFor(() => {
      expect(screen.getByText('Record #1')).toBeInTheDocument();
    });
    expect(screen.getByText('Record #2')).toBeInTheDocument();
  });

  it('shows agent chips', async () => {
    render(<LakebaseDocumentsDialog {...defaultProps} />);
    await waitFor(() => {
      expect(screen.getByText('Researcher')).toBeInTheDocument();
    });
    expect(screen.getByText('Analyst')).toBeInTheDocument();
  });

  it('shows score chip when score is present', async () => {
    render(<LakebaseDocumentsDialog {...defaultProps} />);
    await waitFor(() => {
      expect(screen.getByText('Score: 0.95')).toBeInTheDocument();
    });
  });

  it('shows memory type chip', async () => {
    render(<LakebaseDocumentsDialog {...defaultProps} />);
    expect(screen.getByText('Short-term Memory')).toBeInTheDocument();
  });

  it('shows entity memory type chip', () => {
    render(
      <LakebaseDocumentsDialog
        {...defaultProps}
        memoryType="entity"
        tableName="crew_entity_memory"
      />
    );
    expect(screen.getByText('Entity Memory')).toBeInTheDocument();
  });

  it('shows table name and record count', async () => {
    render(<LakebaseDocumentsDialog {...defaultProps} />);
    await waitFor(() => {
      expect(screen.getByText(/crew_short_term_memory/)).toBeInTheDocument();
      expect(screen.getByText(/2 of 2 records/)).toBeInTheDocument();
    });
  });

  it('shows metadata section when present', async () => {
    render(<LakebaseDocumentsDialog {...defaultProps} />);
    await waitFor(() => {
      expect(screen.getByText('Metadata:')).toBeInTheDocument();
    });
  });

  it('shows loading indicator', () => {
    (MemoryBackendService.getLakebaseTableData as Mock).mockImplementation(
      () => new Promise(() => {})
    );
    render(<LakebaseDocumentsDialog {...defaultProps} />);
    expect(screen.getByRole('progressbar')).toBeInTheDocument();
  });

  it('shows error message on failure', async () => {
    (MemoryBackendService.getLakebaseTableData as Mock).mockResolvedValue({
      success: false,
      documents: [],
      message: 'Table not found',
    });
    render(<LakebaseDocumentsDialog {...defaultProps} />);
    await waitFor(() => {
      expect(screen.getByText('Table not found')).toBeInTheDocument();
    });
  });

  it('shows error message on exception', async () => {
    (MemoryBackendService.getLakebaseTableData as Mock).mockRejectedValue(
      new Error('Network error')
    );
    render(<LakebaseDocumentsDialog {...defaultProps} />);
    await waitFor(() => {
      expect(screen.getByText('Failed to fetch documents. Please try again.')).toBeInTheDocument();
    });
  });

  it('shows empty state when no documents', async () => {
    (MemoryBackendService.getLakebaseTableData as Mock).mockResolvedValue({
      success: true,
      documents: [],
      total: 0,
    });
    render(<LakebaseDocumentsDialog {...defaultProps} />);
    await waitFor(() => {
      expect(screen.getByText('No records found in this table.')).toBeInTheDocument();
    });
  });

  it('filters documents with search', async () => {
    render(<LakebaseDocumentsDialog {...defaultProps} />);
    await waitFor(() => {
      expect(screen.getByText('First document content')).toBeInTheDocument();
    });

    const searchInput = screen.getByPlaceholderText('Search records...');
    fireEvent.change(searchInput, { target: { value: 'Second' } });

    await waitFor(() => {
      expect(screen.queryByText('First document content')).not.toBeInTheDocument();
      expect(screen.getByText('Second document content')).toBeInTheDocument();
    });
  });

  it('shows no match message when search returns empty', async () => {
    render(<LakebaseDocumentsDialog {...defaultProps} />);
    await waitFor(() => {
      expect(screen.getByText('First document content')).toBeInTheDocument();
    });

    const searchInput = screen.getByPlaceholderText('Search records...');
    fireEvent.change(searchInput, { target: { value: 'nonexistent' } });

    await waitFor(() => {
      expect(screen.getByText('No records match your search.')).toBeInTheDocument();
    });
  });

  it('calls onClose when close button is clicked', async () => {
    const onClose = vi.fn();
    render(<LakebaseDocumentsDialog {...defaultProps} onClose={onClose} />);
    const closeButtons = screen.getAllByRole('button');
    const closeBtn = closeButtons.find((btn) => btn.querySelector('[data-testid="CloseIcon"]'));
    fireEvent.click(closeBtn!);
    expect(onClose).toHaveBeenCalled();
  });

  it('refreshes data when refresh button is clicked', async () => {
    render(<LakebaseDocumentsDialog {...defaultProps} />);
    await waitFor(() => {
      expect(MemoryBackendService.getLakebaseTableData).toHaveBeenCalledTimes(1);
    });

    const refreshBtn = screen.getAllByRole('button').find(
      (btn) => btn.querySelector('[data-testid="RefreshIcon"]')
    );
    fireEvent.click(refreshBtn!);

    await waitFor(() => {
      expect(MemoryBackendService.getLakebaseTableData).toHaveBeenCalledTimes(2);
    });
  });

  it('shows document ID', async () => {
    render(<LakebaseDocumentsDialog {...defaultProps} />);
    await waitFor(() => {
      expect(screen.getByText(/ID: doc-1/)).toBeInTheDocument();
    });
  });

  it('shows crew_id when present', async () => {
    render(<LakebaseDocumentsDialog {...defaultProps} />);
    await waitFor(() => {
      // crew_id is rendered inside a Typography with "Crew: {crew_id}"
      const elements = document.querySelectorAll('span');
      const crewElement = Array.from(elements).find(
        (el) => el.textContent?.includes('Crew:') && el.textContent?.includes('crew-abc')
      );
      expect(crewElement).toBeTruthy();
    });
  });

  it('shows created_at when present', async () => {
    render(<LakebaseDocumentsDialog {...defaultProps} />);
    await waitFor(() => {
      expect(screen.getByText(/Created:/)).toBeInTheDocument();
    });
  });
});
