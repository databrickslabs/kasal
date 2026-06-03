import { describe, it, expect, vi, beforeEach } from 'vitest';
import React from 'react';
import { render, screen, fireEvent, within } from '@testing-library/react';
import '@testing-library/jest-dom';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import { SetupResultDialog } from './SetupResultDialog';
import { SetupResult } from '../../types/memoryBackend';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const renderDialog = (props: Partial<React.ComponentProps<typeof SetupResultDialog>> = {}) => {
  const onClose = props.onClose ?? vi.fn();
  const result = render(
    <ThemeProvider theme={createTheme()}>
      <SetupResultDialog
        open={props.open ?? true}
        onClose={onClose}
        setupResult={props.setupResult ?? null}
        workspaceUrl={props.workspaceUrl}
        savedConfigWorkspaceUrl={props.savedConfigWorkspaceUrl}
      />
    </ThemeProvider>,
  );
  return { ...result, onClose };
};

const WORKSPACE = 'https://workspace.databricks.com';

const successResult = (overrides: Partial<SetupResult> = {}): SetupResult => ({
  success: true,
  message: 'Everything set up successfully',
  endpoints: {
    memory: { name: 'kasal_memory_endpoint', status: 'created' },
    document: { name: 'kasal_docs_endpoint', status: 'already_exists' },
  },
  indexes: {
    unified: { name: 'ml.agents.crew_memory', status: 'created' },
    document: { name: 'ml.agents.document_embeddings', status: 'created' },
  },
  ...overrides,
});

describe('SetupResultDialog', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('does not render dialog content when closed', () => {
    renderDialog({ open: false, setupResult: successResult() });
    expect(screen.queryByText('Setup Complete!')).not.toBeInTheDocument();
    expect(screen.queryByText('Setup Failed')).not.toBeInTheDocument();
  });

  it('renders the success title when result is successful', () => {
    renderDialog({ open: true, setupResult: successResult(), workspaceUrl: WORKSPACE });
    expect(screen.getByText('Setup Complete!')).toBeInTheDocument();
  });

  it('renders the failure title when result is unsuccessful', () => {
    renderDialog({
      open: true,
      setupResult: { success: false, message: 'It failed' },
    });
    expect(screen.getByText('Setup Failed')).toBeInTheDocument();
  });

  it('renders the failure title when setupResult is null but dialog is open', () => {
    // setupResult?.success is undefined -> falsy -> "Setup Failed".
    renderDialog({ open: true, setupResult: null });
    expect(screen.getByText('Setup Failed')).toBeInTheDocument();
    // No content box should render.
    expect(screen.queryByText('Endpoints Created:')).not.toBeInTheDocument();
    expect(screen.queryByText('Indexes Created:')).not.toBeInTheDocument();
  });

  it('renders the success severity message', () => {
    renderDialog({ open: true, setupResult: successResult({ message: 'All good' }), workspaceUrl: WORKSPACE });
    expect(screen.getByText('All good')).toBeInTheDocument();
  });

  it('renders the error severity message when unsuccessful', () => {
    renderDialog({
      open: true,
      setupResult: { success: false, message: 'Setup error message' },
    });
    expect(screen.getByText('Setup error message')).toBeInTheDocument();
  });

  it('does not render the warning, info, endpoints or indexes sections when absent', () => {
    renderDialog({
      open: true,
      setupResult: { success: true, message: 'Minimal success' },
      workspaceUrl: WORKSPACE,
    });
    expect(screen.queryByText('Endpoints Created:')).not.toBeInTheDocument();
    expect(screen.queryByText('Indexes Created:')).not.toBeInTheDocument();
  });

  it('renders the warning alert when warning is present', () => {
    renderDialog({
      open: true,
      setupResult: successResult({ warning: 'A warning occurred' }),
      workspaceUrl: WORKSPACE,
    });
    expect(screen.getByText('A warning occurred')).toBeInTheDocument();
  });

  it('renders the info alert when info is present', () => {
    renderDialog({
      open: true,
      setupResult: successResult({ info: 'Some info text' }),
      workspaceUrl: WORKSPACE,
    });
    expect(screen.getByText('Some info text')).toBeInTheDocument();
  });

  it('renders memory and document endpoints with correct links and labels', () => {
    renderDialog({ open: true, setupResult: successResult(), workspaceUrl: WORKSPACE });
    expect(screen.getByText('Endpoints Created:')).toBeInTheDocument();
    expect(screen.getByText('Memory Endpoint (Direct Access)')).toBeInTheDocument();
    expect(screen.getByText('Document Endpoint (Direct Access)')).toBeInTheDocument();

    const memoryLink = screen.getByRole('link', { name: 'kasal_memory_endpoint' });
    expect(memoryLink).toHaveAttribute(
      'href',
      `${WORKSPACE}/compute/vector-search/kasal_memory_endpoint`,
    );
    expect(memoryLink).toHaveAttribute('target', '_blank');
    expect(memoryLink).toHaveAttribute('rel', 'noopener noreferrer');

    const docLink = screen.getByRole('link', { name: 'kasal_docs_endpoint' });
    expect(docLink).toHaveAttribute(
      'href',
      `${WORKSPACE}/compute/vector-search/kasal_docs_endpoint`,
    );
  });

  it('renders unified and document indexes with correct explore-data links', () => {
    renderDialog({ open: true, setupResult: successResult(), workspaceUrl: WORKSPACE });
    expect(screen.getByText('Indexes Created:')).toBeInTheDocument();
    expect(screen.getByText('Unified Cognitive Memory Index')).toBeInTheDocument();
    expect(screen.getByText('Document Embeddings Index')).toBeInTheDocument();

    const unifiedLink = screen.getByRole('link', { name: 'ml.agents.crew_memory' });
    expect(unifiedLink).toHaveAttribute(
      'href',
      `${WORKSPACE}/explore/data/ml/agents/crew_memory`,
    );

    const docIndexLink = screen.getByRole('link', { name: 'ml.agents.document_embeddings' });
    expect(docIndexLink).toHaveAttribute(
      'href',
      `${WORKSPACE}/explore/data/ml/agents/document_embeddings`,
    );
  });

  it('uses savedConfigWorkspaceUrl when workspaceUrl is not provided', () => {
    renderDialog({
      open: true,
      setupResult: successResult(),
      savedConfigWorkspaceUrl: WORKSPACE,
    });
    expect(screen.getByRole('link', { name: 'kasal_memory_endpoint' })).toHaveAttribute(
      'href',
      `${WORKSPACE}/compute/vector-search/kasal_memory_endpoint`,
    );
  });

  it('prefers workspaceUrl over savedConfigWorkspaceUrl', () => {
    renderDialog({
      open: true,
      setupResult: successResult(),
      workspaceUrl: WORKSPACE,
      savedConfigWorkspaceUrl: 'https://other.databricks.com',
    });
    expect(screen.getByRole('link', { name: 'kasal_memory_endpoint' })).toHaveAttribute(
      'href',
      `${WORKSPACE}/compute/vector-search/kasal_memory_endpoint`,
    );
  });

  it('builds an empty-base URL when neither workspace url is provided', () => {
    renderDialog({ open: true, setupResult: successResult() });
    expect(screen.getByRole('link', { name: 'kasal_memory_endpoint' })).toHaveAttribute(
      'href',
      '/compute/vector-search/kasal_memory_endpoint',
    );
  });

  it('appends the workspace id query param when present in the workspace url', () => {
    renderDialog({
      open: true,
      setupResult: successResult(),
      workspaceUrl: `${WORKSPACE}?o=123456`,
    });
    expect(screen.getByRole('link', { name: 'kasal_memory_endpoint' })).toHaveAttribute(
      'href',
      `${WORKSPACE}/compute/vector-search/kasal_memory_endpoint?o=123456`,
    );
    expect(screen.getByRole('link', { name: 'ml.agents.crew_memory' })).toHaveAttribute(
      'href',
      `${WORKSPACE}/explore/data/ml/agents/crew_memory?o=123456`,
    );
  });

  it('renders only the memory endpoint when document endpoint is absent', () => {
    renderDialog({
      open: true,
      setupResult: successResult({
        endpoints: { memory: { name: 'mem_only', status: 'created' } },
        indexes: undefined,
      }),
      workspaceUrl: WORKSPACE,
    });
    expect(screen.getByText('Memory Endpoint (Direct Access)')).toBeInTheDocument();
    expect(screen.queryByText('Document Endpoint (Direct Access)')).not.toBeInTheDocument();
    expect(screen.queryByText('Indexes Created:')).not.toBeInTheDocument();
  });

  it('renders only the document endpoint when memory endpoint is absent', () => {
    renderDialog({
      open: true,
      setupResult: successResult({
        endpoints: { document: { name: 'doc_only', status: 'created' } },
        indexes: undefined,
      }),
      workspaceUrl: WORKSPACE,
    });
    expect(screen.queryByText('Memory Endpoint (Direct Access)')).not.toBeInTheDocument();
    expect(screen.getByText('Document Endpoint (Direct Access)')).toBeInTheDocument();
  });

  it('renders only the unified index when document index is absent', () => {
    renderDialog({
      open: true,
      setupResult: successResult({
        endpoints: undefined,
        indexes: { unified: { name: 'ml.a.b', status: 'created' } },
      }),
      workspaceUrl: WORKSPACE,
    });
    expect(screen.getByText('Unified Cognitive Memory Index')).toBeInTheDocument();
    expect(screen.queryByText('Document Embeddings Index')).not.toBeInTheDocument();
    expect(screen.queryByText('Endpoints Created:')).not.toBeInTheDocument();
  });

  it('renders only the document index when unified index is absent', () => {
    renderDialog({
      open: true,
      setupResult: successResult({
        endpoints: undefined,
        indexes: { document: { name: 'ml.a.docs', status: 'created' } },
      }),
      workspaceUrl: WORKSPACE,
    });
    expect(screen.queryByText('Unified Cognitive Memory Index')).not.toBeInTheDocument();
    expect(screen.getByText('Document Embeddings Index')).toBeInTheDocument();
  });

  it('shows a success status icon for a created status', () => {
    renderDialog({
      open: true,
      setupResult: successResult({
        endpoints: { memory: { name: 'mem', status: 'created' } },
        indexes: undefined,
      }),
      workspaceUrl: WORKSPACE,
    });
    expect(document.querySelector('[data-testid="CheckCircleIcon"]')).toBeInTheDocument();
    expect(document.querySelector('[data-testid="ErrorIcon"]')).not.toBeInTheDocument();
  });

  it('shows a success status icon for an already_exists status', () => {
    renderDialog({
      open: true,
      setupResult: successResult({
        endpoints: { memory: { name: 'mem', status: 'already_exists' } },
        indexes: undefined,
      }),
      workspaceUrl: WORKSPACE,
    });
    expect(document.querySelector('[data-testid="CheckCircleIcon"]')).toBeInTheDocument();
    expect(document.querySelector('[data-testid="ErrorIcon"]')).not.toBeInTheDocument();
  });

  it('shows an error status icon for a failed/unknown status', () => {
    renderDialog({
      open: true,
      setupResult: successResult({
        endpoints: { memory: { name: 'mem', status: 'failed' } },
        indexes: undefined,
      }),
      workspaceUrl: WORKSPACE,
    });
    expect(document.querySelector('[data-testid="ErrorIcon"]')).toBeInTheDocument();
    expect(document.querySelector('[data-testid="CheckCircleIcon"]')).not.toBeInTheDocument();
  });

  it('shows an error status icon when status is undefined', () => {
    renderDialog({
      open: true,
      setupResult: successResult({
        endpoints: { memory: { name: 'mem' } },
        indexes: undefined,
      }),
      workspaceUrl: WORKSPACE,
    });
    expect(document.querySelector('[data-testid="ErrorIcon"]')).toBeInTheDocument();
  });

  it('calls onClose when the Close button is clicked', () => {
    const { onClose } = renderDialog({ open: true, setupResult: successResult(), workspaceUrl: WORKSPACE });
    fireEvent.click(screen.getByRole('button', { name: 'Close' }));
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it('renders a full failure result with endpoints showing error icons', () => {
    renderDialog({
      open: true,
      setupResult: {
        success: false,
        message: 'Setup failed partway',
        warning: 'partial warning',
        endpoints: {
          memory: { name: 'failed_endpoint', status: 'failed' },
        },
      },
      workspaceUrl: WORKSPACE,
    });
    expect(screen.getByText('Setup Failed')).toBeInTheDocument();
    expect(screen.getByText('Setup failed partway')).toBeInTheDocument();
    expect(screen.getByText('partial warning')).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'failed_endpoint' })).toHaveAttribute(
      'href',
      `${WORKSPACE}/compute/vector-search/failed_endpoint`,
    );
    expect(document.querySelector('[data-testid="ErrorIcon"]')).toBeInTheDocument();
  });
});
