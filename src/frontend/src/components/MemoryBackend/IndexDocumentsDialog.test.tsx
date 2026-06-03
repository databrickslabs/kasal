import { describe, it, expect, vi, beforeEach, afterEach, Mock } from 'vitest';
import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import { apiClient } from '../../config/api/ApiConfig';
import { IndexDocumentsDialog } from './IndexDocumentsDialog';

vi.mock('../../config/api/ApiConfig', () => ({
  apiClient: {
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
    delete: vi.fn(),
    patch: vi.fn(),
  },
}));

const theme = createTheme();

const defaultProps = {
  open: true,
  onClose: vi.fn(),
  indexName: 'catalog.schema.memory_index',
  indexType: 'memory' as const,
  workspaceUrl: 'https://example.databricks.com',
  endpointName: 'memory-endpoint',
};

const renderDialog = (props: Partial<React.ComponentProps<typeof IndexDocumentsDialog>> = {}) =>
  render(
    <ThemeProvider theme={theme}>
      <IndexDocumentsDialog {...defaultProps} {...props} />
    </ThemeProvider>,
  );

const sampleDocuments = [
  {
    id: 'doc-1',
    text: 'First memory record content',
    metadata: { scope: '/crew/research', importance: 0.7 },
  },
  {
    id: 'doc-2',
    text: 'Second memory record content',
    metadata: {},
  },
];

describe('IndexDocumentsDialog', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.spyOn(console, 'error').mockImplementation(vi.fn());
    (apiClient.get as Mock).mockResolvedValue({
      data: { success: true, documents: sampleDocuments },
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('does not render content when closed', () => {
    renderDialog({ open: false });
    expect(screen.queryByText('Index Documents')).not.toBeInTheDocument();
  });

  it('renders the dialog title and the memory type chip when open', async () => {
    renderDialog();
    expect(screen.getByText('Index Documents')).toBeInTheDocument();
    expect(screen.getByText('Unified Cognitive Memory')).toBeInTheDocument();
  });

  it('renders the document type chip for document index', async () => {
    renderDialog({ indexType: 'document' });
    expect(screen.getByText('Document Embeddings')).toBeInTheDocument();
  });

  it('fetches documents on open with the expected params (no backend_id)', async () => {
    renderDialog();
    await waitFor(() => {
      expect(apiClient.get).toHaveBeenCalledWith(
        '/memory-backend/databricks/index-documents',
        {
          params: {
            index_name: 'catalog.schema.memory_index',
            workspace_url: 'https://example.databricks.com',
            endpoint_name: 'memory-endpoint',
            index_type: 'memory',
            limit: 30,
          },
        },
      );
    });
  });

  it('includes backend_id in params when provided', async () => {
    renderDialog({ backendId: 'backend-99' });
    await waitFor(() => {
      expect(apiClient.get).toHaveBeenCalledWith(
        '/memory-backend/databricks/index-documents',
        {
          params: expect.objectContaining({ backend_id: 'backend-99' }),
        },
      );
    });
  });

  it('does not fetch when indexName is empty', () => {
    renderDialog({ indexName: '' });
    expect(apiClient.get).not.toHaveBeenCalled();
  });

  it('does not fetch when workspaceUrl is empty', () => {
    renderDialog({ workspaceUrl: '' });
    expect(apiClient.get).not.toHaveBeenCalled();
  });

  it('does not fetch when endpointName is empty', () => {
    renderDialog({ endpointName: '' });
    expect(apiClient.get).not.toHaveBeenCalled();
  });

  it('shows a loading spinner while fetching', () => {
    (apiClient.get as Mock).mockImplementation(() => new Promise(() => {}));
    renderDialog();
    expect(screen.getByRole('progressbar')).toBeInTheDocument();
  });

  it('renders returned documents after loading', async () => {
    renderDialog();
    await waitFor(() => {
      expect(screen.getByText('First memory record content')).toBeInTheDocument();
    });
    expect(screen.getByText('Second memory record content')).toBeInTheDocument();
    expect(screen.getByText('Document #1')).toBeInTheDocument();
    expect(screen.getByText('Document #2')).toBeInTheDocument();
  });

  it('renders document ids and the header count', async () => {
    renderDialog();
    await waitFor(() => {
      expect(screen.getByText('ID: doc-1')).toBeInTheDocument();
    });
    expect(screen.getByText('ID: doc-2')).toBeInTheDocument();
    expect(
      screen.getByText(/catalog\.schema\.memory_index \(2 of 2 documents\)/),
    ).toBeInTheDocument();
  });

  it('renders the metadata block for documents with non-empty metadata', async () => {
    renderDialog();
    await waitFor(() => {
      expect(screen.getByText('Metadata:')).toBeInTheDocument();
    });
    // Pretty-printed JSON includes the scope key
    expect(screen.getByText(/"scope": "\/crew\/research"/)).toBeInTheDocument();
  });

  it('handles documents missing from the response (defaults to empty array)', async () => {
    (apiClient.get as Mock).mockResolvedValue({ data: { success: true } });
    renderDialog();
    await waitFor(() => {
      expect(screen.getByText('No documents found in this index.')).toBeInTheDocument();
    });
  });

  it('shows the empty state when no documents are returned', async () => {
    (apiClient.get as Mock).mockResolvedValue({
      data: { success: true, documents: [] },
    });
    renderDialog();
    await waitFor(() => {
      expect(screen.getByText('No documents found in this index.')).toBeInTheDocument();
    });
  });

  it('shows the server message when success is false', async () => {
    (apiClient.get as Mock).mockResolvedValue({
      data: { success: false, message: 'Index unavailable' },
    });
    renderDialog();
    await waitFor(() => {
      expect(screen.getByText('Index unavailable')).toBeInTheDocument();
    });
  });

  it('falls back to a default message when success is false with no message', async () => {
    (apiClient.get as Mock).mockResolvedValue({ data: { success: false } });
    renderDialog();
    await waitFor(() => {
      expect(screen.getByText('Failed to fetch documents')).toBeInTheDocument();
    });
  });

  it('shows an error message when the request rejects', async () => {
    (apiClient.get as Mock).mockRejectedValue(new Error('Network error'));
    renderDialog();
    await waitFor(() => {
      expect(
        screen.getByText('Failed to fetch documents. Please try again.'),
      ).toBeInTheDocument();
    });
    expect(console.error).toHaveBeenCalled();
  });

  it('filters documents by search query (text match)', async () => {
    renderDialog();
    await waitFor(() => {
      expect(screen.getByText('First memory record content')).toBeInTheDocument();
    });

    const search = screen.getByPlaceholderText('Search documents...');
    fireEvent.change(search, { target: { value: 'Second' } });

    await waitFor(() => {
      expect(screen.queryByText('First memory record content')).not.toBeInTheDocument();
    });
    expect(screen.getByText('Second memory record content')).toBeInTheDocument();
    expect(screen.getByText(/\(1 of 2 documents\)/)).toBeInTheDocument();
  });

  it('filters documents by id match', async () => {
    renderDialog();
    await waitFor(() => {
      expect(screen.getByText('First memory record content')).toBeInTheDocument();
    });
    const search = screen.getByPlaceholderText('Search documents...');
    fireEvent.change(search, { target: { value: 'doc-1' } });
    await waitFor(() => {
      expect(screen.queryByText('Second memory record content')).not.toBeInTheDocument();
    });
    expect(screen.getByText('First memory record content')).toBeInTheDocument();
  });

  it('filters documents by metadata match', async () => {
    renderDialog();
    await waitFor(() => {
      expect(screen.getByText('First memory record content')).toBeInTheDocument();
    });
    const search = screen.getByPlaceholderText('Search documents...');
    fireEvent.change(search, { target: { value: 'research' } });
    await waitFor(() => {
      expect(screen.queryByText('Second memory record content')).not.toBeInTheDocument();
    });
    expect(screen.getByText('First memory record content')).toBeInTheDocument();
  });

  it('shows the no-match message when the search matches nothing', async () => {
    renderDialog();
    await waitFor(() => {
      expect(screen.getByText('First memory record content')).toBeInTheDocument();
    });
    const search = screen.getByPlaceholderText('Search documents...');
    fireEvent.change(search, { target: { value: 'zzz-no-match' } });
    await waitFor(() => {
      expect(screen.getByText('No documents match your search.')).toBeInTheDocument();
    });
  });

  it('refetches when the refresh button is clicked', async () => {
    renderDialog();
    await waitFor(() => {
      expect(apiClient.get).toHaveBeenCalledTimes(1);
    });
    const refreshBtn = screen
      .getAllByRole('button')
      .find((b) => b.querySelector('[data-testid="RefreshIcon"]'));
    fireEvent.click(refreshBtn!);
    await waitFor(() => {
      expect(apiClient.get).toHaveBeenCalledTimes(2);
    });
  });

  it('copies document text to the clipboard when the copy button is clicked', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.assign(navigator, { clipboard: { writeText } });
    renderDialog();
    await waitFor(() => {
      expect(screen.getByText('First memory record content')).toBeInTheDocument();
    });
    const copyBtn = screen
      .getAllByRole('button')
      .find((b) => b.querySelector('[data-testid="ContentCopyIcon"]'));
    fireEvent.click(copyBtn!);
    expect(writeText).toHaveBeenCalledWith('First memory record content');
  });

  it('calls onClose when the close icon button is clicked', async () => {
    const onClose = vi.fn();
    renderDialog({ onClose });
    const closeBtn = screen
      .getAllByRole('button')
      .find((b) => b.querySelector('[data-testid="CloseIcon"]'));
    fireEvent.click(closeBtn!);
    expect(onClose).toHaveBeenCalled();
  });

  it('calls onClose when the footer Close button is clicked', async () => {
    const onClose = vi.fn();
    renderDialog({ onClose });
    fireEvent.click(screen.getByRole('button', { name: 'Close' }));
    expect(onClose).toHaveBeenCalled();
  });

  it('does not render a metadata block for documents with empty metadata', async () => {
    (apiClient.get as Mock).mockResolvedValue({
      data: {
        success: true,
        documents: [{ id: 'only', text: 'lonely doc', metadata: {} }],
      },
    });
    renderDialog();
    await waitFor(() => {
      expect(screen.getByText('lonely doc')).toBeInTheDocument();
    });
    expect(screen.queryByText('Metadata:')).not.toBeInTheDocument();
  });

  it('renders a document without an id (uses index key) and no ID caption', async () => {
    (apiClient.get as Mock).mockResolvedValue({
      data: {
        success: true,
        documents: [{ id: '', text: 'no-id doc' }],
      },
    });
    renderDialog();
    await waitFor(() => {
      expect(screen.getByText('no-id doc')).toBeInTheDocument();
    });
    expect(screen.queryByText(/^ID:/)).not.toBeInTheDocument();
  });

  it('formats string metadata by parsing and re-stringifying', async () => {
    (apiClient.get as Mock).mockResolvedValue({
      data: {
        success: true,
        documents: [
          {
            id: 's1',
            text: 'stringy metadata doc',
            metadata: { raw: '{"a":1}' },
          },
        ],
      },
    });
    renderDialog();
    await waitFor(() => {
      expect(screen.getByText('stringy metadata doc')).toBeInTheDocument();
    });
    expect(screen.getByText('Metadata:')).toBeInTheDocument();
  });
});
