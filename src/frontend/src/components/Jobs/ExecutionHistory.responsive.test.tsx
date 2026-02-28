/**
 * Responsive behaviour tests for ExecutionHistory component.
 *
 * Verifies that table columns are hidden/shown based on viewport breakpoints
 * via the useResponsiveLayout hook.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { ThemeProvider, createTheme } from '@mui/material';

// --- Mocks that must be defined before importing the component ---

let mockIsMobile = false;

vi.mock('../../hooks/workflow/useResponsiveLayout', () => ({
  useResponsiveLayout: () => ({ isMobile: mockIsMobile, isCompact: mockIsMobile }),
}));

vi.mock('../../hooks/global/useExecutionResult', () => ({
  useRunResult: () => ({
    showRunResult: vi.fn(),
    selectedRun: null,
    isOpen: false,
    closeRunResult: vi.fn(),
  }),
}));

vi.mock('../../hooks/global/useExecutionHistory', () => ({
  useRunHistory: () => ({
    runs: [],
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
    handleSearchChange: vi.fn(),
    handleDeleteAllRuns: vi.fn(),
    handleDeleteRun: vi.fn(),
    handleSort: vi.fn(),
    setJobsPerPage: vi.fn(),
  }),
}));

vi.mock('../../store/runStatus', () => ({
  useRunStatusStore: () => ({
    isRunning: false,
    currentRunId: null,
    runStatuses: {},
  }),
}));

vi.mock('../../store/taskExecutionStore', () => ({
  useTaskExecutionStore: () => ({
    taskStatuses: {},
  }),
}));

vi.mock('../../hooks/usePermissions', () => ({
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

vi.mock('../../api/ExecutionLogs', () => ({
  executionLogService: {
    connectToJobLogs: vi.fn(),
    disconnectFromJob: vi.fn(),
  },
}));

vi.mock('../../api/ScheduleService', () => ({
  ScheduleService: {
    getInstance: () => ({
      createScheduleFromRun: vi.fn(),
    }),
  },
}));

// Import component after mocks are set up
import ExecutionHistory from './ExecutionHistory';

const theme = createTheme();

const renderHistory = () =>
  render(
    <ThemeProvider theme={theme}>
      <ExecutionHistory executionHistoryHeight={200} />
    </ThemeProvider>
  );

describe('ExecutionHistory responsive columns', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('desktop (isMobile=false)', () => {
    beforeEach(() => {
      mockIsMobile = false;
    });

    it('shows all table header columns', () => {
      renderHistory();

      // These columns should be visible on desktop
      expect(screen.getByText('Agents/Tasks')).toBeVisible();
      expect(screen.getByText('Submitter')).toBeVisible();
      expect(screen.getByText('Duration')).toBeVisible();
      expect(screen.getByText('Trace')).toBeVisible();
      expect(screen.getByText('Schedule Execution')).toBeVisible();
    });
  });

  describe('mobile (isMobile=true)', () => {
    beforeEach(() => {
      mockIsMobile = true;
    });

    it('hides secondary columns on mobile', () => {
      renderHistory();

      // These columns should be hidden via display: none
      const agentsTasks = screen.getByText('Agents/Tasks');
      expect(agentsTasks).toHaveStyle({ display: 'none' });

      const submitter = screen.getByText('Submitter');
      expect(submitter).toHaveStyle({ display: 'none' });

      const duration = screen.getByText('Duration');
      expect(duration).toHaveStyle({ display: 'none' });

      const trace = screen.getByText('Trace');
      expect(trace).toHaveStyle({ display: 'none' });

      const schedule = screen.getByText('Schedule Execution');
      expect(schedule).toHaveStyle({ display: 'none' });
    });

    it('keeps primary columns visible on mobile', () => {
      renderHistory();

      // Status column header (via translation key)
      expect(screen.getByText('runHistory.columns.status')).toBeVisible();
      // Result column
      expect(screen.getByText('Result')).toBeVisible();
    });
  });
});
