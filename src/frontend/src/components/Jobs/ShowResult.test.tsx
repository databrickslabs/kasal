/**
 * Unit tests for ShowResult component.
 *
 * Covers the A2UI result handling: download buttons are gone, and an A2UI
 * document result can be toggled between the rendered surface and the raw
 * JSON structure.
 */
import { render, screen, fireEvent } from '@testing-library/react';
import { vi, describe, it, expect, beforeEach } from 'vitest';
import ShowResult from './ShowResult';

// ShowResult uses react-router's useNavigate; these tests don't exercise
// navigation, so stub the hook to avoid needing a Router wrapper.
vi.mock('react-router-dom', async (importOriginal) => ({
  ...(await importOriginal<typeof import('react-router-dom')>()),
  useNavigate: () => vi.fn(),
}));

// ---- Mocks ----

vi.mock('react-i18next', () => ({
  useTranslation: () => ({
    t: (key: string) => key,
  }),
}));

vi.mock('react-markdown', () => ({
  default: ({ children }: { children: string }) => <pre data-testid="md">{children}</pre>,
}));

vi.mock('remark-gfm', () => ({ default: () => {} }));

vi.mock('../../api/DatabricksService', () => ({
  DatabricksService: { getConfig: vi.fn().mockResolvedValue(null) },
}));

// The full A2UI preview pane is exercised in PreviewPanel.test.tsx — here we only
// verify that ShowResult routes an A2UI result into the SAME chat preview pane.
vi.mock('../ChatMode/components/Preview/PreviewPanel', () => ({
  __esModule: true,
  default: () => <div data-testid="preview-panel">rendered surface</div>,
}));

beforeEach(() => {
  vi.clearAllMocks();
});

const baseRun = {
  id: '1',
  job_id: '1a9ec0c6-test',
  status: 'COMPLETED',
  created_at: '2026-03-16T00:00:00Z',
  run_name: 'test_run',
  agents_yaml: '',
  tasks_yaml: '',
};

// A minimal valid A2UI document, as the backend stores it (a JSON string
// under the result's single value key).
const a2uiDoc = JSON.stringify({
  messages: [
    { createSurface: { surfaceId: 's1', catalogId: 'basic' } },
    {
      updateComponents: {
        surfaceId: 's1',
        components: [
          { id: 'root', component: 'Column', children: ['title'] },
          { id: 'title', component: 'Text', variant: 'h1', text: 'Crude Oil Snapshot' },
        ],
      },
    },
  ],
});

// ---- Tests ----

describe('ShowResult download buttons removal', () => {
  it('does not render Download PDF for plain results', () => {
    render(
      <ShowResult open={true} onClose={vi.fn()} result={{ status: 'ok', count: 42 } as never} run={baseRun} />
    );
    expect(screen.queryByText('Download PDF')).toBeNull();
    expect(screen.queryByText('Download HTML')).toBeNull();
  });

  it('does not render Download HTML for HTML results', () => {
    const result = { Value: '<!DOCTYPE html><html><body>Hello</body></html>' };
    render(<ShowResult open={true} onClose={vi.fn()} result={result} run={baseRun} />);
    expect(screen.queryByText('Download PDF')).toBeNull();
    expect(screen.queryByText('Download HTML')).toBeNull();
  });
});

describe('ShowResult A2UI rendering toggle', () => {
  it('defaults to the rendered A2UI surface for an A2UI document result', () => {
    render(<ShowResult open={true} onClose={vi.fn()} result={{ value: a2uiDoc }} run={baseRun} />);

    expect(screen.getByTestId('preview-panel')).toBeInTheDocument();
    // The toggle offers both views.
    expect(screen.getByLabelText('rendered view')).toBeInTheDocument();
    expect(screen.getByLabelText('raw json view')).toBeInTheDocument();
  });

  it('switches to the raw JSON structure and back', () => {
    render(<ShowResult open={true} onClose={vi.fn()} result={{ value: a2uiDoc }} run={baseRun} />);

    fireEvent.click(screen.getByLabelText('raw json view'));
    expect(screen.queryByTestId('preview-panel')).toBeNull();
    // The raw structure (unwrapped document) is shown instead.
    expect(screen.getByText('messages')).toBeInTheDocument();

    fireEvent.click(screen.getByLabelText('rendered view'));
    expect(screen.getByTestId('preview-panel')).toBeInTheDocument();
  });

  it('handles an A2UI document passed as a plain object result', () => {
    render(
      <ShowResult open={true} onClose={vi.fn()} result={JSON.parse(a2uiDoc)} run={baseRun} />
    );
    expect(screen.getByTestId('preview-panel')).toBeInTheDocument();
  });

  it('shows no A2UI toggle for non-A2UI results', () => {
    render(
      <ShowResult open={true} onClose={vi.fn()} result={{ status: 'ok' } as never} run={baseRun} />
    );
    expect(screen.queryByLabelText('rendered view')).toBeNull();
    expect(screen.queryByTestId('preview-panel')).toBeNull();
  });

  it('does not false-positive on a nested non-A2UI object', () => {
    render(
      <ShowResult open={true} onClose={vi.fn()} result={{ wrapper: { status: 'ok' } } as never} run={baseRun} />
    );
    expect(screen.queryByTestId('preview-panel')).toBeNull();
  });

  // The run result wraps the A2UI document in varying shapes; ALL of these must
  // render the surface (previously only a top-level doc did, so it rendered only
  // "sometimes"). The doc lives nested below the top level in each case.
  it('renders an A2UI doc nested under a single key as a parsed object', () => {
    render(<ShowResult open={true} onClose={vi.fn()} result={{ result: JSON.parse(a2uiDoc) }} run={baseRun} />);
    expect(screen.getByTestId('preview-panel')).toBeInTheDocument();
    expect(screen.getByLabelText('rendered view')).toBeInTheDocument();
  });

  it('renders an A2UI doc in a multi-key envelope with a JSON-string value', () => {
    render(<ShowResult open={true} onClose={vi.fn()} result={{ output: 'ok', meta: a2uiDoc } as never} run={baseRun} />);
    expect(screen.getByTestId('preview-panel')).toBeInTheDocument();
  });

  it('renders an A2UI doc inside a prose-prefixed string value', () => {
    render(<ShowResult open={true} onClose={vi.fn()} result={{ task: `Here is your dashboard: ${a2uiDoc}` } as never} run={baseRun} />);
    expect(screen.getByTestId('preview-panel')).toBeInTheDocument();
  });

  it('renders a deeply nested A2UI doc', () => {
    render(<ShowResult open={true} onClose={vi.fn()} result={{ a: { b: JSON.parse(a2uiDoc) } } as never} run={baseRun} />);
    expect(screen.getByTestId('preview-panel')).toBeInTheDocument();
  });

  it('keeps the Code/HTML toggle for HTML results', () => {
    const result = { Value: '<!DOCTYPE html><html><body>Hello</body></html>' };
    render(<ShowResult open={true} onClose={vi.fn()} result={result} run={baseRun} />);

    expect(screen.getByLabelText('code view')).toBeInTheDocument();
    expect(screen.getByLabelText('html view')).toBeInTheDocument();
    expect(screen.queryByLabelText('rendered view')).toBeNull();
  });
});

describe('ShowResult fullscreen toggle', () => {
  it('opens full screen by default and can shrink back to a dialog', () => {
    const { baseElement } = render(
      <ShowResult open={true} onClose={vi.fn()} result={{ value: a2uiDoc }} run={baseRun} />
    );

    // Full screen is the default.
    expect(baseElement.querySelector('.MuiDialog-paperFullScreen')).not.toBeNull();

    fireEvent.click(screen.getByLabelText('Exit fullscreen'));
    expect(baseElement.querySelector('.MuiDialog-paperFullScreen')).toBeNull();

    fireEvent.click(screen.getByLabelText('Fullscreen'));
    expect(baseElement.querySelector('.MuiDialog-paperFullScreen')).not.toBeNull();
  });

  it('reopens full screen even after the user exited fullscreen last time', () => {
    const { baseElement, rerender } = render(
      <ShowResult open={true} onClose={vi.fn()} result={{ value: a2uiDoc }} run={baseRun} />
    );

    fireEvent.click(screen.getByLabelText('Exit fullscreen'));
    expect(baseElement.querySelector('.MuiDialog-paperFullScreen')).toBeNull();

    // Close and reopen — the keepMounted dialog must reset to full screen.
    rerender(<ShowResult open={false} onClose={vi.fn()} result={{ value: a2uiDoc }} run={baseRun} />);
    rerender(<ShowResult open={true} onClose={vi.fn()} result={{ value: a2uiDoc }} run={baseRun} />);

    expect(baseElement.querySelector('.MuiDialog-paperFullScreen')).not.toBeNull();
  });

  it('does not depend on the browser Fullscreen API', () => {
    // requestFullscreen is unavailable in the Databricks Apps iframe (and in
    // jsdom) — the toggle must work without ever calling it.
    const result = { value: a2uiDoc };
    render(<ShowResult open={true} onClose={vi.fn()} result={result} run={baseRun} />);

    expect(() => fireEvent.click(screen.getByLabelText('Exit fullscreen'))).not.toThrow();
    expect(screen.getByLabelText('Fullscreen')).toBeInTheDocument();
  });
});
