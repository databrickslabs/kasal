import { vi, beforeEach, describe, it, expect } from 'vitest';
import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import { EditConfigurationForm } from './EditConfigurationForm';
import { SavedConfigInfo } from '../../types/memoryBackend';

const theme = createTheme();

function renderForm(props: {
  editedConfig: SavedConfigInfo | null;
  onEditChange?: (field: string, value: string | undefined) => void;
}) {
  const onEditChange = props.onEditChange ?? vi.fn();
  const result = render(
    <ThemeProvider theme={theme}>
      <EditConfigurationForm editedConfig={props.editedConfig} onEditChange={onEditChange} />
    </ThemeProvider>,
  );
  return { ...result, onEditChange };
}

// MUI TextField renders the editable element as an <input>; query by its label.
const input = (label: RegExp | string) => screen.getByLabelText(label) as HTMLInputElement;

describe('EditConfigurationForm', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('static rendering', () => {
    it('renders the Endpoints and Indexes section labels', () => {
      renderForm({ editedConfig: null });
      expect(screen.getByText('Endpoints:')).toBeInTheDocument();
      expect(screen.getByText('Indexes:')).toBeInTheDocument();
    });

    it('renders all four text fields', () => {
      renderForm({ editedConfig: null });
      expect(screen.getByLabelText(/Memory Endpoint/i)).toBeInTheDocument();
      expect(screen.getByLabelText(/Document Endpoint/i)).toBeInTheDocument();
      expect(screen.getByLabelText(/Unified Memory Index/i)).toBeInTheDocument();
      expect(screen.getByLabelText(/Document Index/i)).toBeInTheDocument();
    });

    it('renders placeholders for each field', () => {
      renderForm({ editedConfig: null });
      expect(screen.getByPlaceholderText('kasal_memory_endpoint')).toBeInTheDocument();
      expect(screen.getByPlaceholderText('kasal_docs_endpoint')).toBeInTheDocument();
      expect(screen.getByPlaceholderText('catalog.schema.crew_memory')).toBeInTheDocument();
      expect(
        screen.getByPlaceholderText('catalog.schema.document_embeddings'),
      ).toBeInTheDocument();
    });
  });

  describe('rendering with a null config (empty values)', () => {
    it('shows all fields empty', () => {
      renderForm({ editedConfig: null });
      expect(input(/Memory Endpoint/i).value).toBe('');
      expect(input(/Document Endpoint/i).value).toBe('');
      expect(input(/Unified Memory Index/i).value).toBe('');
      expect(input(/Document Index/i).value).toBe('');
    });
  });

  describe('rendering with a partially-populated config', () => {
    it('falls back to empty strings for absent optional nested fields', () => {
      // Only the memory endpoint is set; everything else absent.
      const config: SavedConfigInfo = {
        endpoints: { memory: { name: 'my-memory-ep' } },
      };
      renderForm({ editedConfig: config });

      expect(input(/Memory Endpoint/i).value).toBe('my-memory-ep');
      expect(input(/Document Endpoint/i).value).toBe('');
      expect(input(/Unified Memory Index/i).value).toBe('');
      expect(input(/Document Index/i).value).toBe('');
    });

    it('handles endpoints present but document absent, and indexes present but unified absent', () => {
      const config: SavedConfigInfo = {
        endpoints: { memory: { name: 'mem-ep' } },
        indexes: { document: { name: 'doc-index' } },
      };
      renderForm({ editedConfig: config });

      expect(input(/Memory Endpoint/i).value).toBe('mem-ep');
      expect(input(/Document Endpoint/i).value).toBe('');
      expect(input(/Unified Memory Index/i).value).toBe('');
      expect(input(/Document Index/i).value).toBe('doc-index');
    });
  });

  describe('rendering with a fully-populated config', () => {
    const fullConfig: SavedConfigInfo = {
      backend_id: 'abc',
      workspace_url: 'https://example.com',
      catalog: 'cat',
      schema: 'sch',
      endpoints: {
        memory: { name: 'mem-endpoint' },
        document: { name: 'doc-endpoint' },
      },
      indexes: {
        unified: { name: 'cat.sch.crew_memory' },
        document: { name: 'cat.sch.document_embeddings' },
      },
    };

    it('populates every field from the config', () => {
      renderForm({ editedConfig: fullConfig });
      expect(input(/Memory Endpoint/i).value).toBe('mem-endpoint');
      expect(input(/Document Endpoint/i).value).toBe('doc-endpoint');
      expect(input(/Unified Memory Index/i).value).toBe('cat.sch.crew_memory');
      expect(input(/Document Index/i).value).toBe('cat.sch.document_embeddings');
    });
  });

  describe('onEditChange callbacks', () => {
    it('fires with the memory endpoint path and the typed value', () => {
      const { onEditChange } = renderForm({ editedConfig: null });
      fireEvent.change(input(/Memory Endpoint/i), { target: { value: 'new-mem' } });
      expect(onEditChange).toHaveBeenCalledWith('endpoints.memory.name', 'new-mem');
    });

    it('fires with the document endpoint path and the typed value', () => {
      const { onEditChange } = renderForm({ editedConfig: null });
      fireEvent.change(input(/Document Endpoint/i), { target: { value: 'new-doc' } });
      expect(onEditChange).toHaveBeenCalledWith('endpoints.document.name', 'new-doc');
    });

    it('fires with the unified index path and the typed value', () => {
      const { onEditChange } = renderForm({ editedConfig: null });
      fireEvent.change(input(/Unified Memory Index/i), { target: { value: 'cat.s.mem' } });
      expect(onEditChange).toHaveBeenCalledWith('indexes.unified.name', 'cat.s.mem');
    });

    it('fires with the document index path and the typed value', () => {
      const { onEditChange } = renderForm({ editedConfig: null });
      fireEvent.change(input(/Document Index/i), { target: { value: 'cat.s.docs' } });
      expect(onEditChange).toHaveBeenCalledWith('indexes.document.name', 'cat.s.docs');
    });

    it('passes undefined when a field is cleared to empty (|| undefined branch)', () => {
      const config: SavedConfigInfo = {
        endpoints: { memory: { name: 'something' } },
      };
      const { onEditChange } = renderForm({ editedConfig: config });
      fireEvent.change(input(/Memory Endpoint/i), { target: { value: '' } });
      expect(onEditChange).toHaveBeenCalledWith('endpoints.memory.name', undefined);
    });

    it('passes undefined for each of the four fields when cleared', () => {
      const config: SavedConfigInfo = {
        endpoints: { memory: { name: 'a' }, document: { name: 'b' } },
        indexes: { unified: { name: 'c' }, document: { name: 'd' } },
      };
      const { onEditChange } = renderForm({ editedConfig: config });

      fireEvent.change(input(/Memory Endpoint/i), { target: { value: '' } });
      fireEvent.change(input(/Document Endpoint/i), { target: { value: '' } });
      fireEvent.change(input(/Unified Memory Index/i), { target: { value: '' } });
      fireEvent.change(input(/Document Index/i), { target: { value: '' } });

      expect(onEditChange).toHaveBeenNthCalledWith(1, 'endpoints.memory.name', undefined);
      expect(onEditChange).toHaveBeenNthCalledWith(2, 'endpoints.document.name', undefined);
      expect(onEditChange).toHaveBeenNthCalledWith(3, 'indexes.unified.name', undefined);
      expect(onEditChange).toHaveBeenNthCalledWith(4, 'indexes.document.name', undefined);
    });
  });
});
