import { vi, beforeEach, describe, it, expect } from 'vitest';
import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { KnowledgeFileUpload } from './KnowledgeFileUpload';
import { Agent } from '../../types/workflow/agent';

// Mock the knowledge config store - must use vi.hoisted for variables used in vi.mock
const mockKnowledgeConfigStore = vi.hoisted(() => ({
  isMemoryBackendConfigured: true,
  isKnowledgeSourceEnabled: true,
  isLoading: false,
  refreshConfiguration: vi.fn(),
  checkConfiguration: vi.fn(),
}));

vi.mock('../../store/knowledgeConfigStore', () => ({
  useKnowledgeConfigStore: () => mockKnowledgeConfigStore,
}));

// Mock the agent store
vi.mock('../../store/agent', () => ({
  useAgentStore: () => ({
    updateAgent: vi.fn(),
  }),
}));

// Mock the API client
vi.mock('../../config/api/ApiConfig', () => ({
  apiClient: {
    post: vi.fn(),
    get: vi.fn(),
  },
}));

// Mock AgentService
vi.mock('../../api/workflow/AgentService', () => ({
  AgentService: {
    updateAgentFull: vi.fn(),
    getAgent: vi.fn(),
    listAgents: vi.fn().mockResolvedValue([]),
  },
}));

// Mock DatabricksService
vi.mock('../../api/databricks/DatabricksService', () => ({
  DatabricksService: {
    getInstance: vi.fn(() => ({
      getDatabricksConfig: vi.fn().mockResolvedValue({
        knowledge_volume_enabled: true,
        knowledge_volume_path: '/Volumes/test/knowledge',
        workspace_url: 'https://example.com',
      }),
    })),
  },
}));

describe('KnowledgeFileUpload', () => {
  const mockOnFilesUploaded = vi.fn();
  const mockOnAgentsUpdated = vi.fn();

  const mockAgents: Agent[] = [
    {
      id: 'agent-1',
      name: 'Test Agent 1',
      role: 'Test Role 1',
      goal: 'Test Goal 1',
      backstory: 'Test Backstory 1',
      tools: [],
      knowledge_sources: [],
    },
    {
      id: 'agent-2',
      name: 'Test Agent 2',
      role: 'Test Role 2',
      goal: 'Test Goal 2',
      backstory: 'Test Backstory 2',
      tools: [],
      knowledge_sources: [],
    },
  ];

  const defaultProps = {
    executionId: 'test-execution-123',
    groupId: 'test-group-456',
    onFilesUploaded: mockOnFilesUploaded,
    onAgentsUpdated: mockOnAgentsUpdated,
    availableAgents: mockAgents,
    disabled: false,
    compact: false,
    hasAgents: true,
    hasTasks: true,
  };

  beforeEach(() => {
    vi.clearAllMocks();
    // Reset knowledge config to enabled state
    mockKnowledgeConfigStore.isMemoryBackendConfigured = true;
    mockKnowledgeConfigStore.isKnowledgeSourceEnabled = true;
  });

  it('renders the upload button when knowledge is configured', async () => {
    render(<KnowledgeFileUpload {...defaultProps} />);

    await waitFor(() => {
      // The button should have the AttachFile icon
      expect(screen.getByTestId('AttachFileIcon')).toBeInTheDocument();
    });
  });

  it('opens the file browser directly, with no dialog in between', () => {
    // The interaction Chat mode has. The dialog it replaced asked for an agent
    // selection that was auto-filled anyway, then showed a "Choose Files"
    // button that opened this same picker.
    const { container } = render(<KnowledgeFileUpload {...defaultProps} />);

    const input = container.querySelector('input[type="file"]') as HTMLInputElement;
    expect(input).toBeTruthy();
    const clicked = vi.spyOn(input, 'click');

    fireEvent.click(screen.getByRole('button'));

    expect(clicked).toHaveBeenCalled();
    expect(screen.queryByText(/Upload Knowledge Files/i)).not.toBeInTheDocument();
  });

  it('accepts multiple files at once', () => {
    const { container } = render(<KnowledgeFileUpload {...defaultProps} />);
    const input = container.querySelector('input[type="file"]') as HTMLInputElement;

    expect(input.multiple).toBe(true);
    expect(input.accept).toContain('.pdf');
    // Excel workbooks are accepted (parsed to text server-side).
    expect(input.accept).toContain('.xlsx');
    expect(input.accept).toContain('.xls');
  });

  it('disables upload when disabled prop is true', () => {
    render(<KnowledgeFileUpload {...defaultProps} disabled={true} />);

    const uploadButton = screen.getByRole('button');
    expect(uploadButton).toBeDisabled();
  });

  it('renders in compact mode', () => {
    render(<KnowledgeFileUpload {...defaultProps} compact={true} />);

    const uploadButton = screen.getByRole('button');
    // In compact mode, button should be an icon button
    expect(uploadButton).toHaveClass('MuiIconButton-root');
  });

  it('still uploads with no agents on the canvas', () => {
    // The canvas gates the button itself via `disabled` when there is nothing
    // to wire the file into. The component does not second-guess that with a
    // dialog explaining why it refused.
    const { container } = render(
      <KnowledgeFileUpload {...defaultProps} availableAgents={[]} />,
    );

    expect(screen.getByRole('button')).not.toBeDisabled();
    expect(container.querySelector('input[type="file"]')).toBeTruthy();
  });

  it('stays enabled with no Databricks knowledge configuration', async () => {
    // Uploading no longer needs a Lakebase memory backend or an enabled
    // knowledge volume. The backend stages the file, embeds it into
    // `knowledge_embeddings` (SQLite locally, Lakebase pgvector when deployed)
    // and deletes the temp file — no Volume involved — and the search reads
    // that same table. Chat mode calls the same endpoint with no gate at all;
    // this used to be the only place the paperclip went grey.
    mockKnowledgeConfigStore.isMemoryBackendConfigured = false;
    mockKnowledgeConfigStore.isKnowledgeSourceEnabled = false;

    render(<KnowledgeFileUpload {...defaultProps} />);

    expect(screen.getByRole('button')).not.toBeDisabled();
  });

  it('still honours the caller-supplied disabled flag', async () => {
    // The canvas passes `disabled` when there is no agent or task to wire the
    // uploaded file into. That precondition is about the CANVAS having
    // somewhere to attach the file, not about Databricks configuration, so it
    // survives.
    mockKnowledgeConfigStore.isMemoryBackendConfigured = false;

    render(<KnowledgeFileUpload {...defaultProps} disabled />);

    expect(screen.getByRole('button')).toBeDisabled();
  });
});

describe('KnowledgeFileUpload — attachments survive a refresh', () => {
  const KEY = 'kasal-canvas-attachments-test-execution-123';
  const props = {
    executionId: 'test-execution-123',
    groupId: 'test-group-456',
    hasAgents: true,
    hasTasks: true,
  };

  beforeEach(() => {
    localStorage.clear();
  });

  it('restores files attached in this session', () => {
    // Losing them on refresh was misleading in a way an empty state is not:
    // the file was still uploaded, embedded and searchable by the crew — only
    // the UI had forgotten, so the only recourse was to upload it again and
    // create a duplicate.
    localStorage.setItem(
      KEY,
      JSON.stringify([
        {
          id: 'a',
          filename: 'paper.pdf',
          path: 'uploads/g/s/paper.pdf',
          size: 10,
          status: 'success',
        },
      ]),
    );

    render(<KnowledgeFileUpload {...props} />);

    expect(screen.getByText('paper.pdf')).toBeInTheDocument();
  });

  it('offers a detach control per attached file', () => {
    localStorage.setItem(
      KEY,
      JSON.stringify([
        {
          id: 'a',
          filename: 'paper.pdf',
          path: 'uploads/g/s/paper.pdf',
          size: 10,
          status: 'success',
        },
      ]),
    );

    const { container } = render(<KnowledgeFileUpload {...props} />);

    // MUI renders the Chip's onDelete as a cancel icon.
    expect(container.querySelector('.MuiChip-deleteIcon')).toBeTruthy();
  });

  it('does not restore a failed upload', () => {
    localStorage.setItem(
      KEY,
      JSON.stringify([
        { id: 'b', filename: 'broken.pdf', path: '', size: 1, status: 'error' },
      ]),
    );

    render(<KnowledgeFileUpload {...props} />);

    // Hydration shows what was stored, but the persist effect drops non-success
    // entries, so a failed upload never survives a second refresh.
    expect(localStorage.getItem(KEY)).toBeNull();
  });

  it('keeps sessions separate', () => {
    localStorage.setItem(
      'kasal-canvas-attachments-other-session',
      JSON.stringify([
        { id: 'c', filename: 'elsewhere.pdf', path: 'p', size: 1, status: 'success' },
      ]),
    );

    render(<KnowledgeFileUpload {...props} />);

    expect(screen.queryByText('elsewhere.pdf')).not.toBeInTheDocument();
  });
});
