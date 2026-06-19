/**
 * Unit tests for TaskForm's LLM-guardrail section.
 *
 * Focused on the recent guardrail changes:
 *  1. "Suggest criteria from task" button — when the guardrail is enabled and
 *     clicked, it calls GenerateService.suggestGuardrail(description,
 *     expected_output, <guardrail llm_model || chat-input selectedModel>) and
 *     fills the Validation Criteria field with the returned text. It is
 *     disabled when both description and expected_output are empty, and shows
 *     "Suggesting…" while loading.
 *  2. The "Validation LLM Model" dropdown defaults to a
 *     "Use the model selected for the run (default)" empty option.
 *  3. The "Max retries on validation failure" number field is bound to
 *     config.max_retries.
 *
 * TaskForm imports a large surface of services and heavy child components; the
 * mocks below stub everything it touches on mount so a real render succeeds and
 * the guardrail interactions can be exercised in isolation.
 */
import React from 'react';
import { render, screen, fireEvent, waitFor, within } from '@testing-library/react';
import { vi, describe, it, expect, beforeEach } from 'vitest';

// jsdom has no ResizeObserver; TaskForm's useStableResize hook constructs one
// on mount. Provide a no-op polyfill so the component can render.
class ResizeObserverStub {
  observe() {}
  unobserve() {}
  disconnect() {}
}
(globalThis as unknown as { ResizeObserver: typeof ResizeObserverStub }).ResizeObserver =
  ResizeObserverStub;

// ---- Mocks ----

// The chat-input model the guardrail falls back to when no explicit guardrail
// model is picked. TaskForm reads it via useCrewExecutionStore((s) => s.selectedModel).
const CHAT_INPUT_MODEL = 'databricks-gpt-5-3-codex';

vi.mock('../../store/crewExecution', () => ({
  useCrewExecutionStore: vi.fn((selector: (s: { selectedModel: string }) => unknown) =>
    selector({ selectedModel: CHAT_INPUT_MODEL }),
  ),
}));

// The service under test for the Suggest button — a controllable vi.fn().
const suggestGuardrailMock = vi.fn();
vi.mock('../../api/GenerateService', () => ({
  GenerateService: {
    suggestGuardrail: (...args: unknown[]) => suggestGuardrailMock(...args),
  },
}));

// Services TaskForm calls on mount — stubbed so render doesn't error.
vi.mock('../../api/TaskService', () => ({
  TaskService: {
    listTasks: vi.fn().mockResolvedValue([]),
    createTask: vi.fn(),
    updateTask: vi.fn(),
  },
}));

vi.mock('../../api/DatabricksService', () => ({
  DatabricksService: {
    getInstance: () => ({
      getDatabricksEnvironment: vi.fn().mockResolvedValue({}),
      getDatabricksConfig: vi.fn().mockResolvedValue(null),
    }),
  },
}));

// ModelService.getModels() drives the "Validation LLM Model" dropdown options.
const getModelsMock = vi.fn().mockResolvedValue({
  'databricks-llama': { name: 'databricks-llama' },
  'gpt-4o': { name: 'gpt-4o' },
});
vi.mock('../../api/ModelService', () => ({
  ModelService: {
    getInstance: () => ({
      getModels: getModelsMock,
    }),
  },
}));

// Heavy child components that always render in the form — stub to lightweight
// placeholders so they don't pull in their own service/store dependencies.
vi.mock('../Common/MCPServerSelector', () => ({
  MCPServerSelector: () => <div data-testid="mcp-server-selector" />,
}));

vi.mock('./TaskAdvancedConfig', () => ({
  TaskAdvancedConfig: () => <div data-testid="task-advanced-config" />,
}));

vi.mock('../BestPractices/TaskBestPractices', () => ({
  default: () => <div data-testid="task-best-practices" />,
}));

import TaskForm from './TaskForm';
import type { Task } from '../../api/TaskService';

// ---- Helpers ----

const baseTask: Task = {
  id: 'task-1',
  name: 'Summarize report',
  description: 'Summarize the quarterly report into key points',
  expected_output: 'A concise bullet list of the key findings',
  tools: [],
  agent_id: null,
  async_execution: false,
  context: [],
};

const renderForm = (taskOverrides: Partial<Task> = {}) =>
  render(
    <TaskForm
      initialData={{ ...baseTask, ...taskOverrides }}
      tools={[]}
      onCancel={vi.fn()}
      onTaskSaved={vi.fn()}
    />,
  );

// Enable the LLM guardrail toggle so the guardrail section renders.
const enableGuardrail = () => {
  const toggle = screen.getByRole('checkbox', { name: /Enable LLM Guardrail/i });
  fireEvent.click(toggle);
};

beforeEach(() => {
  vi.clearAllMocks();
  getModelsMock.mockResolvedValue({
    'databricks-llama': { name: 'databricks-llama' },
    'gpt-4o': { name: 'gpt-4o' },
  });
});

// ---- Tests ----

describe('TaskForm LLM guardrail section', () => {
  it('shows the guardrail section only after the toggle is enabled', () => {
    renderForm();

    // Section is hidden until enabled.
    expect(screen.queryByText('Suggest criteria from task')).toBeNull();

    enableGuardrail();

    expect(screen.getByText('Suggest criteria from task')).toBeInTheDocument();
    expect(screen.getByLabelText(/Validation Criteria/i)).toBeInTheDocument();
  });
});

describe('TaskForm Suggest criteria from task', () => {
  it('calls suggestGuardrail with the task description, expected output and chat-input model, then fills Validation Criteria', async () => {
    suggestGuardrailMock.mockResolvedValue('Ensure the summary covers all key findings accurately.');

    renderForm();
    enableGuardrail();

    fireEvent.click(screen.getByRole('button', { name: 'Suggest criteria from task' }));

    await waitFor(() => {
      expect(suggestGuardrailMock).toHaveBeenCalledWith(
        baseTask.description,
        baseTask.expected_output,
        CHAT_INPUT_MODEL,
      );
    });

    const criteria = screen.getByLabelText(/Validation Criteria/i) as HTMLTextAreaElement;
    await waitFor(() => {
      expect(criteria.value).toBe('Ensure the summary covers all key findings accurately.');
    });
  });

  it('shows "Suggesting…" while the suggestion is in flight', async () => {
    let resolveSuggestion: (value: string) => void = () => {};
    suggestGuardrailMock.mockReturnValue(
      new Promise<string>((resolve) => {
        resolveSuggestion = resolve;
      }),
    );

    renderForm();
    enableGuardrail();

    fireEvent.click(screen.getByRole('button', { name: 'Suggest criteria from task' }));

    // While the promise is pending the button reads "Suggesting…".
    expect(await screen.findByRole('button', { name: 'Suggesting…' })).toBeInTheDocument();

    resolveSuggestion('Validate completeness.');

    // Once resolved it returns to the normal label.
    expect(await screen.findByRole('button', { name: 'Suggest criteria from task' })).toBeInTheDocument();
  });

  it('disables the Suggest button when both description and expected output are empty', () => {
    renderForm({ description: '', expected_output: '' });
    enableGuardrail();

    expect(screen.getByRole('button', { name: 'Suggest criteria from task' })).toBeDisabled();
  });

  it('enables the Suggest button when only the description is present', () => {
    renderForm({ description: 'Do the thing', expected_output: '' });
    enableGuardrail();

    expect(screen.getByRole('button', { name: 'Suggest criteria from task' })).toBeEnabled();
  });
});

describe('TaskForm Validation LLM Model dropdown', () => {
  it('defaults to the "Use the model selected for the run (default)" option', async () => {
    renderForm();
    enableGuardrail();

    // This MUI Select has no aria-labelledby, so locate it via its label's
    // FormControl rather than by accessible name. Opening it reveals the
    // default (empty value) option — the run-model placeholder text rather
    // than a hardcoded model name — plus the loaded models.
    // The outlined Select renders its label text twice (the <label> and the
    // fieldset legend); pick the actual <label> element.
    const modelLabel = screen
      .getAllByText('Validation LLM Model')
      .find((el) => el.tagName === 'LABEL') as HTMLElement;
    const modelFormControl = modelLabel.closest('.MuiFormControl-root') as HTMLElement;
    const combo = within(modelFormControl).getByRole('combobox');

    // The Select is disabled while models load; wait for that to finish before
    // opening it (a disabled Select won't open).
    await waitFor(() => {
      expect(combo).not.toHaveAttribute('aria-disabled', 'true');
    });
    fireEvent.mouseDown(combo);

    const listbox = await screen.findByRole('listbox');
    expect(
      within(listbox).getByText('Use the model selected for the run (default)'),
    ).toBeInTheDocument();
    // The empty default is the first option in the listbox.
    const options = within(listbox).getAllByRole('option');
    expect(options[0]).toHaveAttribute('data-value', '');
    await waitFor(() => {
      expect(within(listbox).getByText('databricks-llama')).toBeInTheDocument();
    });
  });

  it('uses the explicitly-picked guardrail model for the suggestion when one is selected', async () => {
    suggestGuardrailMock.mockResolvedValue('criteria');

    // Seed an explicit guardrail model so the Suggest call should use it (not
    // the chat-input fallback).
    renderForm({
      config: {
        max_retries: 3,
        llm_guardrail: { description: 'existing', llm_model: 'gpt-4o' },
      } as Task['config'],
    });

    // Guardrail is already enabled because config.llm_guardrail is present.
    fireEvent.click(screen.getByRole('button', { name: 'Suggest criteria from task' }));

    await waitFor(() => {
      expect(suggestGuardrailMock).toHaveBeenCalledWith(
        baseTask.description,
        baseTask.expected_output,
        'gpt-4o',
      );
    });
  });
});

describe('TaskForm Max retries on validation failure', () => {
  it('renders bound to config.max_retries and is editable', () => {
    renderForm({
      config: {
        max_retries: 5,
        llm_guardrail: { description: 'validate' },
      } as Task['config'],
    });

    const retries = screen.getByLabelText(/Max retries on validation failure/i) as HTMLInputElement;
    // Reflects the initial config.max_retries.
    expect(retries.value).toBe('5');

    // Editable and clamped to the 0-10 range.
    fireEvent.change(retries, { target: { value: '7' } });
    expect(retries.value).toBe('7');
  });

  it('defaults the retries field to 3 when config has no explicit value', () => {
    renderForm();
    enableGuardrail();

    const retries = screen.getByLabelText(/Max retries on validation failure/i) as HTMLInputElement;
    expect(retries.value).toBe('3');
  });
});
