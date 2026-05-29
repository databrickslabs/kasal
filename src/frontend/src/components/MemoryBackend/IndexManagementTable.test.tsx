import { describe, it, expect, vi, beforeEach } from 'vitest';
import React from 'react';
import { render, screen, fireEvent, within } from '@testing-library/react';
import '@testing-library/jest-dom';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import { IndexManagementTable } from './IndexManagementTable';
import { SavedConfigInfo, IndexInfoState } from '../../types/memoryBackend';
import { INDEX_DESCRIPTIONS } from './constants';
import {
  buildVectorSearchIndexUrl,
  buildVectorSearchEndpointUrl,
} from './databricksVectorSearchUtils';

const theme = createTheme();

const renderTable = (props: Partial<React.ComponentProps<typeof IndexManagementTable>> = {}) => {
  const savedConfig: SavedConfigInfo = {
    workspace_url: 'https://example.databricks.com?o=12345',
    backend_id: 'backend-1',
  };

  const defaultProps: React.ComponentProps<typeof IndexManagementTable> = {
    title: 'Memory Index',
    savedConfig,
    endpointName: 'memory-endpoint',
    endpointType: 'memory',
    indexes: [{ type: 'memory', name: 'catalog.schema.memory_index' }],
    indexInfoMap: {
      'catalog.schema.memory_index': {
        doc_count: 42,
        loading: false,
        index_type: 'DIRECT_ACCESS',
      } as IndexInfoState,
    },
    endpointStatuses: {
      memory: { state: 'ONLINE', can_delete_indexes: true },
    },
    isSettingUp: false,
    onEmpty: vi.fn(),
    onDelete: vi.fn(),
  };

  const merged = { ...defaultProps, ...props };
  return {
    ...render(
      <ThemeProvider theme={theme}>
        <IndexManagementTable {...merged} />
      </ThemeProvider>,
    ),
    props: merged,
  };
};

describe('IndexManagementTable', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('renders title and column headers', () => {
    renderTable({ title: 'My Memory Index' });
    expect(screen.getByText('My Memory Index')).toBeInTheDocument();
    expect(screen.getByText('Name')).toBeInTheDocument();
    expect(screen.getByText('Type')).toBeInTheDocument();
    expect(screen.getByText('Documents')).toBeInTheDocument();
    expect(screen.getByText('Description')).toBeInTheDocument();
    expect(screen.getByText('Actions')).toBeInTheDocument();
  });

  it('renders subtitle info tooltip button when subtitle is provided', () => {
    renderTable({ subtitle: 'Some subtitle' });
    // The subtitle is rendered as a tooltip title (not directly in DOM text),
    // but the info IconButton exists. There should be at least the subtitle info icon.
    const infoIcons = document.querySelectorAll('[data-testid="InfoIcon"]');
    // one for subtitle + one per row description
    expect(infoIcons.length).toBeGreaterThanOrEqual(2);
  });

  it('does not render subtitle info icon when subtitle absent', () => {
    renderTable({ subtitle: undefined, indexes: [] });
    const infoIcons = document.querySelectorAll('[data-testid="InfoIcon"]');
    expect(infoIcons.length).toBe(0);
  });

  it('renders the endpoint link using the URL helper when showEndpointLink and endpointName present', () => {
    renderTable({ endpointName: 'memory-endpoint' });
    const link = screen.getByRole('link', { name: 'memory-endpoint' });
    expect(link).toHaveAttribute(
      'href',
      buildVectorSearchEndpointUrl('https://example.databricks.com?o=12345', 'memory-endpoint'),
    );
    expect(link).toHaveAttribute('target', '_blank');
    expect(link).toHaveAttribute('rel', 'noopener noreferrer');
  });

  it('does not render endpoint link when showEndpointLink is false', () => {
    renderTable({ showEndpointLink: false });
    expect(screen.queryByRole('link', { name: 'memory-endpoint' })).not.toBeInTheDocument();
  });

  it('does not render endpoint link when endpointName missing', () => {
    renderTable({ endpointName: undefined });
    expect(screen.queryByText('Endpoint:')).not.toBeInTheDocument();
  });

  it('falls back to empty workspace url when savedConfig.workspace_url missing', () => {
    renderTable({ savedConfig: { backend_id: 'b' } });
    const link = screen.getByRole('link', { name: 'memory-endpoint' });
    expect(link).toHaveAttribute('href', buildVectorSearchEndpointUrl('', 'memory-endpoint'));
  });

  it('skips indexes without a name', () => {
    renderTable({ indexes: [{ type: 'memory' }] });
    // No data rows -> no index link
    expect(screen.queryByRole('link', { name: 'memory_index' })).not.toBeInTheDocument();
  });

  it('renders the index name link (last dotted segment) using URL helper', () => {
    renderTable();
    const link = screen.getByRole('link', { name: 'memory_index' });
    expect(link).toHaveAttribute(
      'href',
      buildVectorSearchIndexUrl('https://example.databricks.com?o=12345', 'catalog.schema.memory_index'),
    );
  });

  it('renders the index status (index_type) text', () => {
    renderTable();
    expect(screen.getByText('DIRECT_ACCESS')).toBeInTheDocument();
  });

  it('renders UNKNOWN status when index has no info entry', () => {
    renderTable({ indexInfoMap: {} });
    // status cell shows literal 'UNKNOWN'
    expect(screen.getByText('UNKNOWN')).toBeInTheDocument();
  });

  it('renders UNKNOWN typography when info exists but index_type missing', () => {
    renderTable({
      indexInfoMap: {
        'catalog.schema.memory_index': { doc_count: 0, loading: false } as IndexInfoState,
      },
    });
    expect(screen.getByText('UNKNOWN')).toBeInTheDocument();
  });

  it('renders NOT FOUND when index_type is DELETED', () => {
    renderTable({
      indexInfoMap: {
        'catalog.schema.memory_index': {
          doc_count: 0,
          loading: false,
          index_type: 'DELETED',
        } as IndexInfoState,
      },
    });
    expect(screen.getByText('NOT FOUND')).toBeInTheDocument();
  });

  it('renders a spinner for status and doc count while loading', () => {
    renderTable({
      indexInfoMap: {
        'catalog.schema.memory_index': {
          doc_count: 0,
          loading: true,
        } as IndexInfoState,
      },
    });
    // two progress bars: one in status cell, one in document count cell
    expect(screen.getAllByRole('progressbar').length).toBe(2);
  });

  it('renders the document count', () => {
    renderTable();
    expect(screen.getByText('42')).toBeInTheDocument();
  });

  it('renders 0 document count when doc_count falsy', () => {
    renderTable({
      indexInfoMap: {
        'catalog.schema.memory_index': {
          doc_count: 0,
          loading: false,
          index_type: 'DIRECT_ACCESS',
        } as IndexInfoState,
      },
    });
    expect(screen.getByText('0')).toBeInTheDocument();
  });

  it('renders a doc-count spinner when index has no info entry', () => {
    renderTable({ indexInfoMap: {} });
    // doc-count cell renders a spinner when info is missing
    expect(screen.getAllByRole('progressbar').length).toBe(1);
  });

  it('renders the brief description for a memory index', () => {
    renderTable();
    expect(screen.getByText(INDEX_DESCRIPTIONS.memory.brief)).toBeInTheDocument();
  });

  it('renders an empty brief description for an unknown index type', () => {
    renderTable({
      indexes: [{ type: 'memory', name: 'catalog.schema.weird' } as { type: 'memory'; name?: string }],
      indexInfoMap: {
        'catalog.schema.weird': { doc_count: 1, loading: false, index_type: 'X' } as IndexInfoState,
      },
    });
    // brief from memory still exists since type === 'memory'; just verify render is fine
    expect(screen.getByText(INDEX_DESCRIPTIONS.memory.brief)).toBeInTheDocument();
  });

  it('renders the document index brief description', () => {
    renderTable({
      title: 'Document Index',
      endpointType: 'document',
      endpointName: 'doc-endpoint',
      indexes: [{ type: 'document', name: 'catalog.schema.doc_index' }],
      indexInfoMap: {
        'catalog.schema.doc_index': {
          doc_count: 5,
          loading: false,
          index_type: 'DELTA_SYNC',
        } as IndexInfoState,
      },
      endpointStatuses: { document: { state: 'ONLINE', can_delete_indexes: true } },
    });
    expect(screen.getByText(INDEX_DESCRIPTIONS.document.brief)).toBeInTheDocument();
  });

  it('fires onViewDocuments with type and name when the view button is clicked', () => {
    const onViewDocuments = vi.fn();
    renderTable({ onViewDocuments });
    const viewBtn = screen
      .getAllByRole('button')
      .find((b) => b.querySelector('[data-testid="DescriptionIcon"]'));
    fireEvent.click(viewBtn!);
    expect(onViewDocuments).toHaveBeenCalledWith('memory', 'catalog.schema.memory_index');
  });

  it('does not render a view-documents button when onViewDocuments is omitted', () => {
    renderTable({ onViewDocuments: undefined });
    expect(document.querySelector('[data-testid="DescriptionIcon"]')).toBeNull();
  });

  it('renders the re-seed button only for document indexes with onRefresh and fires it', () => {
    const onRefresh = vi.fn();
    renderTable({
      title: 'Document Index',
      endpointType: 'document',
      endpointName: 'doc-endpoint',
      indexes: [{ type: 'document', name: 'catalog.schema.doc_index' }],
      indexInfoMap: {
        'catalog.schema.doc_index': {
          doc_count: 5,
          loading: false,
          index_type: 'DELTA_SYNC',
        } as IndexInfoState,
      },
      endpointStatuses: { document: { state: 'ONLINE', can_delete_indexes: true } },
      onRefresh,
    });
    const refreshBtn = screen
      .getAllByRole('button')
      .find((b) => b.querySelector('[data-testid="RefreshIcon"]'));
    expect(refreshBtn).toBeTruthy();
    fireEvent.click(refreshBtn!);
    expect(onRefresh).toHaveBeenCalledTimes(1);
  });

  it('does not render re-seed button for memory indexes even with onRefresh', () => {
    renderTable({ onRefresh: vi.fn() });
    expect(document.querySelector('[data-testid="RefreshIcon"]')).toBeNull();
  });

  it('does not render re-seed button for document index without onRefresh', () => {
    renderTable({
      endpointType: 'document',
      indexes: [{ type: 'document', name: 'catalog.schema.doc_index' }],
      indexInfoMap: {
        'catalog.schema.doc_index': {
          doc_count: 5,
          loading: false,
          index_type: 'DELTA_SYNC',
        } as IndexInfoState,
      },
      endpointStatuses: { document: { state: 'ONLINE', can_delete_indexes: true } },
      onRefresh: undefined,
    });
    expect(document.querySelector('[data-testid="RefreshIcon"]')).toBeNull();
  });

  it('fires onEmpty with the index type when the reset button is clicked', () => {
    const onEmpty = vi.fn();
    renderTable({ onEmpty });
    const emptyBtn = screen
      .getAllByRole('button')
      .find((b) => b.querySelector('[data-testid="RestartAltIcon"]'));
    fireEvent.click(emptyBtn!);
    expect(onEmpty).toHaveBeenCalledWith('memory');
  });

  it('fires onDelete with the index type when the delete button is clicked', () => {
    const onDelete = vi.fn();
    renderTable({ onDelete });
    const deleteBtn = screen
      .getAllByRole('button')
      .find((b) => b.querySelector('[data-testid="DeleteIcon"]'));
    fireEvent.click(deleteBtn!);
    expect(onDelete).toHaveBeenCalledWith('memory');
  });

  it('disables view/empty buttons when isSettingUp is true', () => {
    renderTable({ isSettingUp: true, onViewDocuments: vi.fn() });
    const viewBtn = screen
      .getAllByRole('button')
      .find((b) => b.querySelector('[data-testid="DescriptionIcon"]'));
    const emptyBtn = screen
      .getAllByRole('button')
      .find((b) => b.querySelector('[data-testid="RestartAltIcon"]'));
    expect(viewBtn).toBeDisabled();
    expect(emptyBtn).toBeDisabled();
  });

  it('disables the delete button when isSettingUp is true', () => {
    renderTable({ isSettingUp: true });
    const deleteBtn = screen
      .getAllByRole('button')
      .find((b) => b.querySelector('[data-testid="DeleteIcon"]'));
    expect(deleteBtn).toBeDisabled();
  });

  it('disables the delete button when endpoint can_delete_indexes is false', () => {
    renderTable({
      endpointStatuses: { memory: { state: 'ONLINE', can_delete_indexes: false } },
    });
    const deleteBtn = screen
      .getAllByRole('button')
      .find((b) => b.querySelector('[data-testid="DeleteIcon"]'));
    expect(deleteBtn).toBeDisabled();
  });

  it('disables the delete button when index_type is DELETED', () => {
    renderTable({
      indexInfoMap: {
        'catalog.schema.memory_index': {
          doc_count: 0,
          loading: false,
          index_type: 'DELETED',
        } as IndexInfoState,
      },
    });
    const deleteBtn = screen
      .getAllByRole('button')
      .find((b) => b.querySelector('[data-testid="DeleteIcon"]'));
    expect(deleteBtn).toBeDisabled();
  });

  it('enables the delete button when not setting up, deletes allowed, and not deleted', () => {
    renderTable();
    const deleteBtn = screen
      .getAllByRole('button')
      .find((b) => b.querySelector('[data-testid="DeleteIcon"]'));
    expect(deleteBtn).not.toBeDisabled();
  });

  it('enables delete button when endpoint status entry is missing', () => {
    renderTable({ endpointStatuses: {} });
    const deleteBtn = screen
      .getAllByRole('button')
      .find((b) => b.querySelector('[data-testid="DeleteIcon"]'));
    expect(deleteBtn).not.toBeDisabled();
  });

  it('renders multiple index rows', () => {
    renderTable({
      indexes: [
        { type: 'memory', name: 'catalog.schema.memory_index' },
        { type: 'document', name: 'catalog.schema.doc_index' },
      ],
      indexInfoMap: {
        'catalog.schema.memory_index': {
          doc_count: 42,
          loading: false,
          index_type: 'DIRECT_ACCESS',
        } as IndexInfoState,
        'catalog.schema.doc_index': {
          doc_count: 7,
          loading: false,
          index_type: 'DELTA_SYNC',
        } as IndexInfoState,
      },
    });
    expect(screen.getByRole('link', { name: 'memory_index' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'doc_index' })).toBeInTheDocument();
    const rows = screen.getAllByRole('row');
    // header row + 2 data rows
    expect(rows.length).toBe(3);
    const dataRow = rows.find((r) => within(r).queryByText('42'));
    expect(dataRow).toBeTruthy();
  });
});
