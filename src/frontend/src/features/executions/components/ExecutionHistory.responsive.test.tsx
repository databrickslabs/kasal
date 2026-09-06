import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { ThemeProvider, createTheme } from '@mui/material';

// --- Mocks that must be defined before importing the component ---

// ExecutionHistory renders ShowResult, which now uses react-router's useNavigate;
// stub it so these layout tests don't need a Router wrapper.
vi.mock('react-router-dom', async (importOriginal) => ({
  ...(await importOriginal<typeof import('react-router-dom')>()),
  useNavigate: () => vi.fn(),
}));

let mockIsMobile = false;
let mockRuns: Run[] = [];
const mockSearch = vi.fn();
const mockSort = vi.fn();
const mockClose = vi.fn();

vi.mock('../../../hooks/workflow/useResponsiveLayout', () => ({
  useResponsiveLayout: () => ({ isMobile: mockIsMobile, isCompact: mockIsMobile }),
}));

vi.mock('../../../hooks/global/useExecutionResult', () => ({
  useRunResult: () => ({
    showRunResult: vi.fn(),
    selectedRun: null,
    isOpen: false,
    closeRunResult: vi.fn(),
  }),
}));

vi.mock('../../../hooks/global/useExecutionHistory', () => ({
  useRunHistory: () => ({
    runs: mockRuns,
    searchQuery: '',
    loading: false,
    showSkeleton: false,
    error: null,
    page: 1,
    totalPages: 1,
    totalRuns: 0,
    jobsPerPage: 10,
    sortField: 'created_at',
    sortOrder: 'desc',
    fetchRuns: vi.fn(),
    handlePageChange: vi.fn(),
    handleSearchChange: mockSearch,
    handleDeleteAllRuns: vi.fn(),
    handleDeleteRun: vi.fn(),
    handleSort: mockSort,
    setJobsPerPage: vi.fn(),
  }),
}));

vi.mock('../../../store/runStatus', () => ({
  useRunStatusStore: () => ({
    isRunning: false,
    currentRunId: null,
    runStatuses: {},
  }),
}));

vi.mock('../../../store/taskExecutionStore', () => ({
  useTaskExecutionStore: () => ({
    taskStatuses: {},
  }),
}));

vi.mock('../../../hooks/usePermissions', () => ({
  usePermissions: () => ({ userRole: 'admin' }),
}));

vi.mock('react-i18next', () => ({
  useTranslation: () => ({
    t: (key: string) => key,
  }),
}));

vi.mock('react-hot-toast', () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

vi.mock('../../../api/execution/ExecutionLogs', () => ({
  executionLogService: {
    connectToJobLogs: vi.fn(),
    disconnectFromJob: vi.fn(),
  },
}));

vi.mock('../../../api/execution/ScheduleService', () => ({
  ScheduleService: {
    getInstance: () => ({
      createScheduleFromRun: vi.fn(),
    }),
  },
}));

vi.mock('./recipeIndexCache', () => ({ refreshRecipeIndexIfStale: vi.fn() }));
vi.mock('./ExecutionMemoryButton', () => ({ default: () => <button>Memory</button> }));
vi.mock('./RecipeCurationButton', () => ({ default: () => <button>Reusable</button> }));
vi.mock('../../../api/execution/ExecutionHistoryService', async importOriginal => ({
  ...(await importOriginal<typeof import('../../../api/execution/ExecutionHistoryService')>()),
  calculateDurationFromTraces: vi.fn().mockResolvedValue('15s'),
}));
import type { Run } from '../../../api/execution/ExecutionHistoryService';
// Import component after mocks are set up
import ExecutionHistory from './ExecutionHistory';

const theme = createTheme();

const renderHistory = () =>
  render(
    <ThemeProvider theme={theme}>
      <ExecutionHistory onClose={mockClose} />
    </ThemeProvider>
  );

describe('Job history sidebar', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockRuns = [];
    mockIsMobile = false;
  });
  it('shows a useful empty state and closes from its header', () => {
    renderHistory();
    expect(screen.getByText('Your work, all in one place')).toBeVisible();
    fireEvent.click(screen.getByRole('button', { name: 'Close job history' }));
    expect(mockClose).toHaveBeenCalledOnce();
  });
  it('makes search and sorting available without a table header', () => {
    renderHistory();
    fireEvent.change(screen.getByRole('textbox', { name: 'Search runs' }), { target: { value: 'research' } });
    expect(mockSearch).toHaveBeenCalledOnce();
    fireEvent.click(screen.getByRole('button', { name: 'History options' }));
    fireEvent.click(screen.getByRole('menuitem', { name: /Sort by status/ }));
    expect(mockSort).toHaveBeenCalledWith('status');
  });
  it.each([false, true])('keeps run actions and secondary details reachable on compact=%s', async compact => {
    mockIsMobile = compact;
    mockRuns = [{ id: 'run-1', job_id: 'job-1', run_name: 'Research report', status: 'COMPLETED', created_at: '2026-09-06T10:00:00Z', group_email: 'person@example.com' } as Run];
    renderHistory();
    expect(screen.getByRole('list', { name: 'Job runs' })).toBeVisible();
    expect(screen.getByRole('button', { name: 'Result' })).toBeVisible();
    expect(await screen.findByText('15s')).toBeVisible();
    fireEvent.click(screen.getByRole('button', { name: 'Details for Research report' }));
    expect(screen.getByRole('region', { name: 'Details for Research report' })).toHaveTextContent('person@example.com');
    expect(screen.getByRole('button', { name: 'Memory' })).toBeVisible();
    expect(screen.getByRole('button', { name: 'Reusable' })).toBeVisible();
    fireEvent.click(screen.getByRole('button', { name: 'Actions for Research report' }));
    expect(screen.getByRole('menuitem', { name: 'Schedule execution' })).toBeVisible();
    expect(screen.getByRole('menuitem', { name: 'Checkpoint and resume' })).toBeVisible();
  });
});
