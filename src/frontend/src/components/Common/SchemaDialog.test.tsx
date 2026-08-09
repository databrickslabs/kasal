import { vi, beforeEach, describe, it, expect } from 'vitest';
import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import SchemaDialog from './SchemaDialog';
import { SchemaService } from '../../api/workflow/SchemaService';

const mockCreateSchema = vi.fn();

vi.mock('../../api/workflow/SchemaService', () => ({
  SchemaService: { getInstance: () => ({ createSchema: mockCreateSchema }) },
}));

const setup = (overrides: Partial<React.ComponentProps<typeof SchemaDialog>> = {}) => {
  const onClose = vi.fn();
  const onCreated = vi.fn();
  render(<SchemaDialog open onClose={onClose} onSaved={onCreated} {...overrides} />);
  return { onClose, onCreated };
};

const typeInto = (el: Element, value: string) => fireEvent.change(el, { target: { value } });

beforeEach(() => {
  vi.clearAllMocks();
});

describe('SchemaDialog', () => {
  it('renders nothing visible when closed', () => {
    render(<SchemaDialog open={false} onClose={vi.fn()} onSaved={vi.fn()} />);
    expect(screen.queryByText('New Output Schema')).not.toBeInTheDocument();
  });

  it('renders the form when open', () => {
    setup();
    expect(screen.getByText('New Output Schema')).toBeInTheDocument();
    expect(screen.getByLabelText('Schema name')).toBeInTheDocument();
    expect(screen.getByPlaceholderText('field name')).toBeInTheDocument();
  });

  it('errors when name is empty', () => {
    setup();
    fireEvent.click(screen.getByText('Create & use'));
    expect(screen.getByText('Schema name is required.')).toBeInTheDocument();
    expect(mockCreateSchema).not.toHaveBeenCalled();
  });

  it('errors when no valid fields are provided', () => {
    setup();
    typeInto(screen.getByLabelText('Schema name'), 'ResearchResult');
    fireEvent.click(screen.getByText('Create & use'));
    expect(screen.getByText('Add at least one field.')).toBeInTheDocument();
    expect(mockCreateSchema).not.toHaveBeenCalled();
  });

  it('adds and removes fields', () => {
    setup();
    fireEvent.click(screen.getByText('Add field'));
    expect(screen.getAllByPlaceholderText('field name')).toHaveLength(2);

    const removeBtns = screen.getAllByTestId('DeleteIcon').map((i) => i.closest('button'));
    fireEvent.click(removeBtns[0]!);
    expect(screen.getAllByPlaceholderText('field name')).toHaveLength(1);
  });

  it('updates only the targeted field when several exist', () => {
    setup();
    fireEvent.click(screen.getByText('Add field'));
    const inputs = screen.getAllByPlaceholderText('field name');
    typeInto(inputs[0], 'first');
    const after = screen.getAllByPlaceholderText('field name') as HTMLInputElement[];
    expect(after[0].value).toBe('first');
    expect(after[1].value).toBe(''); // the other field is left untouched (map else-branch)
  });

  it('changes a field type via the select', () => {
    setup();
    fireEvent.mouseDown(screen.getByRole('combobox'));
    fireEvent.click(screen.getByRole('option', { name: 'Number' }));
    expect(screen.getByRole('combobox')).toHaveTextContent('Number');
  });

  it('expands a list field so one item can be described', () => {
    setup();
    fireEvent.mouseDown(screen.getByRole('combobox'));
    fireEvent.click(screen.getByRole('option', { name: 'List of items' }));

    expect(screen.getByText('Each item has:')).toBeInTheDocument();
    expect(screen.getByText('Add field to item')).toBeInTheDocument();
  });

  it('writes a list of items the router can address', async () => {
    mockCreateSchema.mockResolvedValue({ id: '9', name: 'News' });
    const { onCreated } = setup();

    typeInto(screen.getByLabelText('Schema name'), 'News');
    const names = () => screen.getAllByPlaceholderText('field name') as HTMLInputElement[];
    typeInto(names()[0], 'articles');
    fireEvent.mouseDown(screen.getAllByRole('combobox')[0]);
    fireEvent.click(screen.getByRole('option', { name: 'List of items' }));
    typeInto(names()[1], 'category');

    fireEvent.click(screen.getByText('Create & use'));
    await waitFor(() => expect(onCreated).toHaveBeenCalled());

    expect(onCreated.mock.calls[0][0].schema_definition).toEqual({
      type: 'object',
      properties: {
        articles: {
          type: 'array',
          items: {
            type: 'object',
            properties: { category: { type: 'string' } },
            required: ['category'],
          },
        },
      },
      required: ['articles'],
    });
  });

  it('creates a schema and calls onCreated with a normalized definition', async () => {
    mockCreateSchema.mockResolvedValue({ id: '1', name: 'ResearchResult' });
    const { onCreated } = setup();

    typeInto(screen.getByLabelText('Schema name'), 'ResearchResult');
    typeInto(screen.getByLabelText('Description (optional)'), 'desc');
    typeInto(screen.getByPlaceholderText('field name'), 'title');
    fireEvent.click(screen.getByText('Create & use'));

    await waitFor(() => expect(onCreated).toHaveBeenCalled());
    const arg = onCreated.mock.calls[0][0];
    expect(arg.schema_definition.properties).toHaveProperty('title');
    expect(arg.schema_definition.required).toEqual(['title']);

    const payload = mockCreateSchema.mock.calls[0][0];
    expect(payload.schema_type).toBe('data_model');
    expect(payload.description).toBe('desc');
  });

  it('uses a default description when none is given', async () => {
    mockCreateSchema.mockResolvedValue({ id: '2', name: 'Foo' });
    setup();
    typeInto(screen.getByLabelText('Schema name'), 'Foo');
    typeInto(screen.getByPlaceholderText('field name'), 'x');
    fireEvent.click(screen.getByText('Create & use'));
    await waitFor(() => expect(mockCreateSchema).toHaveBeenCalled());
    expect(mockCreateSchema.mock.calls[0][0].description).toBe('Output schema for Foo');
  });

  it('shows an error when creation returns null', async () => {
    mockCreateSchema.mockResolvedValue(null);
    const { onCreated } = setup();
    typeInto(screen.getByLabelText('Schema name'), 'Foo');
    typeInto(screen.getByPlaceholderText('field name'), 'x');
    fireEvent.click(screen.getByText('Create & use'));
    await waitFor(() => expect(screen.getByText('Failed to create schema.')).toBeInTheDocument());
    expect(onCreated).not.toHaveBeenCalled();
  });

  it('surfaces a generic error message on throw', async () => {
    mockCreateSchema.mockRejectedValue(new Error('boom'));
    setup();
    typeInto(screen.getByLabelText('Schema name'), 'Foo');
    typeInto(screen.getByPlaceholderText('field name'), 'x');
    fireEvent.click(screen.getByText('Create & use'));
    await waitFor(() => expect(screen.getByText('boom')).toBeInTheDocument());
  });

  it('maps a 409/exists error to a friendly duplicate message', async () => {
    mockCreateSchema.mockRejectedValue(new Error('Request failed with status code 409'));
    setup();
    typeInto(screen.getByLabelText('Schema name'), 'Dup');
    typeInto(screen.getByPlaceholderText('field name'), 'x');
    fireEvent.click(screen.getByText('Create & use'));
    await waitFor(() =>
      expect(screen.getByText('A schema named "Dup" already exists.')).toBeInTheDocument(),
    );
  });

  it('falls back to a default message when a non-Error is thrown', async () => {
    mockCreateSchema.mockRejectedValue('weird');
    setup();
    typeInto(screen.getByLabelText('Schema name'), 'Foo');
    typeInto(screen.getByPlaceholderText('field name'), 'x');
    fireEvent.click(screen.getByText('Create & use'));
    await waitFor(() => expect(screen.getByText('Failed to create schema.')).toBeInTheDocument());
  });

  it('cancel resets and calls onClose', () => {
    const { onClose } = setup();
    typeInto(screen.getByLabelText('Schema name'), 'temp');
    fireEvent.click(screen.getByText('Cancel'));
    expect(onClose).toHaveBeenCalled();
  });
});

describe('SchemaDialog in edit mode', () => {
  const existing = {
    name: 'Classification',
    description: 'Output schema for Classification',
    schema_type: 'data_model',
    schema_definition: {
      type: 'object',
      properties: {
        classification: {
          type: 'array',
          items: {
            type: 'object',
            properties: { category: { type: 'string' } },
            required: ['category'],
          },
        },
      },
      required: ['classification'],
    },
  } as never;

  it('titles itself after the schema and loads its fields', () => {
    render(<SchemaDialog open onClose={vi.fn()} schema={existing} onSaved={vi.fn()} />);

    expect(screen.getByText('Edit: Classification')).toBeInTheDocument();
    expect(screen.getByText('Each item has:')).toBeInTheDocument();
    const names = screen.getAllByPlaceholderText('field name') as HTMLInputElement[];
    expect(names.map((i) => i.value)).toEqual(['classification', 'category']);
  });

  it('will not let the name change — the API identifies a schema by it', () => {
    render(<SchemaDialog open onClose={vi.fn()} schema={existing} onSaved={vi.fn()} />);

    expect(screen.getByLabelText('Schema name')).toBeDisabled();
  });

  it('updates rather than creates, preserving the nesting', async () => {
    const mockUpdate = vi.fn().mockResolvedValue({ ...existing });
    (SchemaService.getInstance as unknown as ReturnType<typeof vi.fn>) = vi.fn(() => ({
      createSchema: mockCreateSchema,
      updateSchema: mockUpdate,
    })) as never;
    const onSaved = vi.fn();

    render(<SchemaDialog open onClose={vi.fn()} schema={existing} onSaved={onSaved} />);
    fireEvent.click(screen.getByText('Save'));

    await waitFor(() => expect(onSaved).toHaveBeenCalled());
    expect(mockCreateSchema).not.toHaveBeenCalled();
    expect(mockUpdate.mock.calls[0][0]).toBe('Classification');
    expect(mockUpdate.mock.calls[0][1].schema_definition).toEqual(
      existing.schema_definition
    );
  });
});
