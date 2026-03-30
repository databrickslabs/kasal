/**
 * Unit tests for DatabaseManagement component.
 *
 * Tests the database information display, alert severity logic,
 * and connection error handling for various database backends
 * (sqlite, lakebase, lakebase with connection errors).
 */
import React from 'react';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import { vi, describe, it, expect, beforeEach } from 'vitest';
import '@testing-library/jest-dom';

// ---------------------------------------------------------------------------
// Hoisted mock references (available to vi.mock factories)
// ---------------------------------------------------------------------------
const {
  mockApiClient,
  mockGetAPIKeys,
  mockDatabaseStoreState,
} = vi.hoisted(() => {
  const state = {
    databaseInfo: null as Record<string, unknown> | null,
    lakebaseConfig: {
      enabled: false,
      instance_name: 'kasal-lakebase',
      capacity: 'CU_1',
      retention_days: 14,
      node_count: 1,
      instance_status: 'NOT_CREATED',
    },
    schemaExists: false,
    showMigrationDialog: false,
    migrationOption: 'recreate' as const,
    loading: false,
    checkingInstance: false,
    expandedSections: { lakebaseConfig: false },
    error: null as string | null,
    success: null as string | null,
    setDatabaseInfo: vi.fn(),
    setLakebaseConfig: vi.fn(),
    setSchemaExists: vi.fn(),
    setShowMigrationDialog: vi.fn(),
    setMigrationOption: vi.fn(),
    setLoading: vi.fn(),
    setCheckingInstance: vi.fn(),
    setError: vi.fn(),
    setSuccess: vi.fn(),
    setCurrentBackend: vi.fn(),
    setExpandedSection: vi.fn(),
    reset: vi.fn(),
  };

  return {
    mockApiClient: {
      get: vi.fn().mockResolvedValue({ data: {} }),
      post: vi.fn().mockResolvedValue({ data: {} }),
      put: vi.fn().mockResolvedValue({ data: {} }),
      delete: vi.fn().mockResolvedValue({ data: {} }),
    },
    mockGetAPIKeys: vi.fn().mockResolvedValue([]),
    mockDatabaseStoreState: state,
  };
});

// ---------------------------------------------------------------------------
// Module mocks
// ---------------------------------------------------------------------------

vi.mock('../../../config/api/ApiConfig', () => ({
  apiClient: mockApiClient,
  config: {
    backendUrl: 'http://localhost:8000',
  },
}));

vi.mock('../../../store/databaseStore', () => ({
  useDatabaseStore: () => mockDatabaseStoreState,
}));

vi.mock('../../../api/APIKeysService', () => ({
  APIKeysService: {
    getInstance: vi.fn(() => ({
      getAPIKeys: mockGetAPIKeys,
    })),
  },
}));

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------
const theme = createTheme();

const renderWithProviders = (component: React.ReactElement) => {
  return render(
    <ThemeProvider theme={theme}>
      {component}
    </ThemeProvider>
  );
};

// Helper to set databaseInfo on the mock store
const setMockDatabaseInfo = (info: Record<string, unknown> | null) => {
  mockDatabaseStoreState.databaseInfo = info;
};

const resetMockStoreState = () => {
  mockDatabaseStoreState.databaseInfo = null;
  mockDatabaseStoreState.loading = false;
  mockDatabaseStoreState.error = null;
  mockDatabaseStoreState.success = null;
  mockDatabaseStoreState.lakebaseConfig = {
    enabled: false,
    instance_name: 'kasal-lakebase',
    capacity: 'CU_1',
    retention_days: 14,
    node_count: 1,
    instance_status: 'NOT_CREATED',
  };
};

// Dynamically import DatabaseManagement after mocks are set up
const importComponent = async () => {
  const mod = await import('../DatabaseManagement');
  return mod.default;
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
describe('DatabaseManagement', () => {
  let DatabaseManagement: React.FC;
  let componentLoaded = false;

  beforeEach(async () => {
    vi.clearAllMocks();
    resetMockStoreState();

    // Default API responses
    mockApiClient.get.mockImplementation((url: string) => {
      if (url.includes('/database-management/info')) {
        return Promise.resolve({ data: { success: true } });
      }
      if (url.includes('/database-management/lakebase/config')) {
        return Promise.resolve({ data: { enabled: false } });
      }
      return Promise.resolve({ data: {} });
    });

    mockGetAPIKeys.mockResolvedValue([]);

    // Only import the component once to avoid repeated heavy imports
    if (!componentLoaded) {
      DatabaseManagement = await importComponent();
      componentLoaded = true;
    }
  }, 30_000);

  it('renders the component with Database Management title', async () => {
    renderWithProviders(<DatabaseManagement />);

    await waitFor(() => {
      expect(screen.getByText('Database Management')).toBeInTheDocument();
    });
  });

  it('renders tab labels', async () => {
    renderWithProviders(<DatabaseManagement />);

    await waitFor(() => {
      expect(screen.getByText('General')).toBeInTheDocument();
      expect(screen.getByText('Databricks Import/Export')).toBeInTheDocument();
      expect(screen.getByText('Lakebase')).toBeInTheDocument();
    });
  });

  describe('Database Information Display', () => {
    it('renders sqlite database info with info alert', async () => {
      setMockDatabaseInfo({
        success: true,
        database_type: 'sqlite',
        database_path: '/data/kasal.db',
        size_mb: 5.25,
        total_tables: 10,
        tables: { agents: 5, tasks: 12 },
        created_at: '2025-01-01T00:00:00Z',
        modified_at: '2025-06-15T12:00:00Z',
      });

      renderWithProviders(<DatabaseManagement />);

      await waitFor(() => {
        expect(screen.getByText(/Current Database Backend: SQLITE/)).toBeInTheDocument();
      });

      // SQLite should render an 'info' severity alert
      const alert = screen.getByRole('alert');
      expect(alert).toHaveClass('MuiAlert-standardInfo');

      // Should show type
      expect(screen.getByText('sqlite')).toBeInTheDocument();

      // Should show path
      expect(screen.getByText('/data/kasal.db')).toBeInTheDocument();

      // Should show table chips
      expect(screen.getByText('agents (5 rows)')).toBeInTheDocument();
      expect(screen.getByText('tasks (12 rows)')).toBeInTheDocument();
    });

    it('renders lakebase database info with success alert when no connection error', async () => {
      setMockDatabaseInfo({
        success: true,
        database_type: 'lakebase',
        lakebase_enabled: true,
        lakebase_instance: 'my-lakebase-instance',
        lakebase_endpoint: 'lakebase-endpoint.example.com',
        total_tables: 8,
        tables: { agents: 3, tasks: 7 },
      });

      renderWithProviders(<DatabaseManagement />);

      await waitFor(() => {
        expect(screen.getByText(/Current Database Backend: LAKEBASE/)).toBeInTheDocument();
      });

      // Lakebase without connection_error should show 'success' severity
      const alert = screen.getByRole('alert');
      expect(alert).toHaveClass('MuiAlert-standardSuccess');

      // Should show instance name
      expect(screen.getByText(/Connected to Lakebase instance: my-lakebase-instance/)).toBeInTheDocument();

      // Should show endpoint
      expect(screen.getByText('lakebase-endpoint.example.com')).toBeInTheDocument();
    });

    it('renders lakebase database info with warning alert when connection_error is set', async () => {
      setMockDatabaseInfo({
        success: true,
        database_type: 'lakebase',
        lakebase_enabled: true,
        lakebase_instance: 'my-lakebase-instance',
        lakebase_endpoint: 'lakebase-endpoint.example.com',
        connection_error: 'Connection timed out after 30 seconds',
        total_tables: 0,
        tables: {},
      });

      renderWithProviders(<DatabaseManagement />);

      await waitFor(() => {
        expect(screen.getByText(/Current Database Backend: LAKEBASE/)).toBeInTheDocument();
      });

      // Lakebase with connection_error should show 'warning' severity
      const alert = screen.getByRole('alert');
      expect(alert).toHaveClass('MuiAlert-standardWarning');
    });

    it('displays the connection error message text', async () => {
      const errorMessage = 'Unable to connect: authentication failed for lakebase instance';

      setMockDatabaseInfo({
        success: true,
        database_type: 'lakebase',
        lakebase_enabled: true,
        lakebase_instance: 'my-lakebase-instance',
        connection_error: errorMessage,
      });

      renderWithProviders(<DatabaseManagement />);

      await waitFor(() => {
        expect(screen.getByText(errorMessage)).toBeInTheDocument();
      });
    });

    it('does not display connection error text when connection_error is absent', async () => {
      setMockDatabaseInfo({
        success: true,
        database_type: 'lakebase',
        lakebase_enabled: true,
        lakebase_instance: 'my-lakebase-instance',
      });

      renderWithProviders(<DatabaseManagement />);

      await waitFor(() => {
        expect(screen.getByText(/Current Database Backend: LAKEBASE/)).toBeInTheDocument();
      });

      // There should be no connection error text in the document
      expect(screen.queryByText(/Unable to connect/)).not.toBeInTheDocument();
      expect(screen.queryByText(/Connection timed out/)).not.toBeInTheDocument();
    });

    it('does not show lakebase instance info for sqlite databases', async () => {
      setMockDatabaseInfo({
        success: true,
        database_type: 'sqlite',
        database_path: '/data/kasal.db',
        size_mb: 2.0,
      });

      renderWithProviders(<DatabaseManagement />);

      await waitFor(() => {
        expect(screen.getByText(/Current Database Backend: SQLITE/)).toBeInTheDocument();
      });

      expect(screen.queryByText(/Connected to Lakebase instance/)).not.toBeInTheDocument();
    });

    it('does not render database info card when databaseInfo is null', async () => {
      setMockDatabaseInfo(null);

      renderWithProviders(<DatabaseManagement />);

      await waitFor(() => {
        expect(screen.getByText('Database Management')).toBeInTheDocument();
      });

      expect(screen.queryByText('Database Information')).not.toBeInTheDocument();
    });

    it('renders lakebase endpoint when database type is lakebase', async () => {
      setMockDatabaseInfo({
        success: true,
        database_type: 'lakebase',
        lakebase_endpoint: 'my-endpoint.lakehouse.example.com',
        lakebase_instance: 'test-instance',
      });

      renderWithProviders(<DatabaseManagement />);

      await waitFor(() => {
        expect(screen.getByText('my-endpoint.lakehouse.example.com')).toBeInTheDocument();
      });

      // Should show Endpoint label
      expect(screen.getByText('Endpoint')).toBeInTheDocument();
    });

    it('does not render path and size fields for lakebase databases', async () => {
      setMockDatabaseInfo({
        success: true,
        database_type: 'lakebase',
        lakebase_instance: 'test-instance',
        lakebase_endpoint: 'endpoint.example.com',
      });

      renderWithProviders(<DatabaseManagement />);

      await waitFor(() => {
        expect(screen.getByText(/Current Database Backend: LAKEBASE/)).toBeInTheDocument();
      });

      // Path and Size labels should not appear for lakebase type
      expect(screen.queryByText('Path')).not.toBeInTheDocument();
      expect(screen.queryByText('Size')).not.toBeInTheDocument();
    });

    it('renders table chips when tables data is present', async () => {
      setMockDatabaseInfo({
        success: true,
        database_type: 'sqlite',
        database_path: '/data/kasal.db',
        size_mb: 1.5,
        total_tables: 3,
        tables: {
          users: 100,
          models: 25,
          workflows: 50,
        },
      });

      renderWithProviders(<DatabaseManagement />);

      await waitFor(() => {
        expect(screen.getByText('users (100 rows)')).toBeInTheDocument();
        expect(screen.getByText('models (25 rows)')).toBeInTheDocument();
        expect(screen.getByText('workflows (50 rows)')).toBeInTheDocument();
      });

      // Table count header
      expect(screen.getByText('Tables (3)')).toBeInTheDocument();
    });
  });

  describe('Alert severity logic', () => {
    it('uses info severity for non-lakebase database types', async () => {
      setMockDatabaseInfo({
        success: true,
        database_type: 'sqlite',
      });

      renderWithProviders(<DatabaseManagement />);

      await waitFor(() => {
        const alert = screen.getByRole('alert');
        expect(alert).toHaveClass('MuiAlert-standardInfo');
      });
    });

    it('uses info severity for postgresql database type', async () => {
      setMockDatabaseInfo({
        success: true,
        database_type: 'postgresql',
        database_path: 'postgresql://localhost/kasal',
        size_mb: 50,
      });

      renderWithProviders(<DatabaseManagement />);

      await waitFor(() => {
        const alert = screen.getByRole('alert');
        expect(alert).toHaveClass('MuiAlert-standardInfo');
      });
    });

    it('uses success severity for lakebase without connection error', async () => {
      setMockDatabaseInfo({
        success: true,
        database_type: 'lakebase',
        lakebase_instance: 'test-instance',
      });

      renderWithProviders(<DatabaseManagement />);

      await waitFor(() => {
        const alert = screen.getByRole('alert');
        expect(alert).toHaveClass('MuiAlert-standardSuccess');
      });
    });

    it('uses warning severity for lakebase with connection error', async () => {
      setMockDatabaseInfo({
        success: true,
        database_type: 'lakebase',
        lakebase_instance: 'test-instance',
        connection_error: 'DNS resolution failed',
      });

      renderWithProviders(<DatabaseManagement />);

      await waitFor(() => {
        const alert = screen.getByRole('alert');
        expect(alert).toHaveClass('MuiAlert-standardWarning');
      });
    });
  });

  describe('Lakebase tab - connect form visibility', () => {
    const navigateToLakebaseTab = async () => {
      await waitFor(() => {
        expect(screen.getByText('Lakebase')).toBeInTheDocument();
      });
      fireEvent.click(screen.getByText('Lakebase'));
    };

    it('shows connect form when lakebase is selected but not connected', async () => {
      renderWithProviders(<DatabaseManagement />);
      await navigateToLakebaseTab();

      // Select "Databricks Lakebase" radio
      fireEvent.click(screen.getByText('Databricks Lakebase'));

      await waitFor(() => {
        expect(screen.getByText('Connect to Existing Lakebase Instance')).toBeInTheDocument();
      });
    });

    it('shows prerequisites alert when not connected', async () => {
      renderWithProviders(<DatabaseManagement />);
      await navigateToLakebaseTab();

      fireEvent.click(screen.getByText('Databricks Lakebase'));

      await waitFor(() => {
        expect(screen.getByText('Databricks App Setup')).toBeInTheDocument();
      });
    });

    it('hides connect form when instance is enabled and READY', async () => {
      mockDatabaseStoreState.lakebaseConfig = {
        enabled: true,
        instance_name: 'kasal-lakebase1',
        capacity: 'CU_1',
        retention_days: 14,
        node_count: 1,
        instance_status: 'READY',
        endpoint: 'instance-123.database.cloud.databricks.com',
      };

      renderWithProviders(<DatabaseManagement />);
      await navigateToLakebaseTab();

      fireEvent.click(screen.getByText('Databricks Lakebase'));

      await waitFor(() => {
        expect(screen.queryByText('Connect to Existing Lakebase Instance')).not.toBeInTheDocument();
      });
    });

    it('hides prerequisites alert when instance is enabled and READY', async () => {
      mockDatabaseStoreState.lakebaseConfig = {
        enabled: true,
        instance_name: 'kasal-lakebase1',
        capacity: 'CU_1',
        retention_days: 14,
        node_count: 1,
        instance_status: 'READY',
        endpoint: 'instance-123.database.cloud.databricks.com',
      };

      renderWithProviders(<DatabaseManagement />);
      await navigateToLakebaseTab();

      fireEvent.click(screen.getByText('Databricks Lakebase'));

      await waitFor(() => {
        expect(screen.queryByText('Databricks App Setup')).not.toBeInTheDocument();
      });
    });

    it('shows connect form when instance is READY but not enabled (re-enabling flow)', async () => {
      mockDatabaseStoreState.lakebaseConfig = {
        enabled: false,
        instance_name: 'kasal-lakebase1',
        capacity: 'CU_1',
        retention_days: 14,
        node_count: 1,
        instance_status: 'READY',
        endpoint: 'instance-123.database.cloud.databricks.com',
      };

      renderWithProviders(<DatabaseManagement />);
      await navigateToLakebaseTab();

      fireEvent.click(screen.getByText('Databricks Lakebase'));

      await waitFor(() => {
        expect(screen.getByText('Connect to Existing Lakebase Instance')).toBeInTheDocument();
      });
    });

    it('shows status section with action buttons when enabled and READY', async () => {
      mockDatabaseStoreState.lakebaseConfig = {
        enabled: true,
        instance_name: 'kasal-lakebase1',
        capacity: 'CU_1',
        retention_days: 14,
        node_count: 1,
        instance_status: 'READY',
        endpoint: 'instance-123.database.cloud.databricks.com',
      };

      renderWithProviders(<DatabaseManagement />);
      await navigateToLakebaseTab();

      fireEvent.click(screen.getByText('Databricks Lakebase'));

      await waitFor(() => {
        expect(screen.getByText('Current Status')).toBeInTheDocument();
        expect(screen.getByText('View in Databricks')).toBeInTheDocument();
        expect(screen.getByText('Refresh Status')).toBeInTheDocument();
      });
    });

    it('does not render Create New Instance option', async () => {
      renderWithProviders(<DatabaseManagement />);
      await navigateToLakebaseTab();

      fireEvent.click(screen.getByText('Databricks Lakebase'));

      await waitFor(() => {
        expect(screen.getByText('Connect to Existing Lakebase Instance')).toBeInTheDocument();
      });

      expect(screen.queryByText('Create New Instance')).not.toBeInTheDocument();
      expect(screen.queryByText('How would you like to set up Lakebase?')).not.toBeInTheDocument();
    });
  });

  describe('Connection error display', () => {
    it('renders connection error with specific message text', async () => {
      const errorMsg = 'Lakebase instance is not responding: timeout after 60s';

      setMockDatabaseInfo({
        success: true,
        database_type: 'lakebase',
        lakebase_instance: 'prod-instance',
        connection_error: errorMsg,
      });

      renderWithProviders(<DatabaseManagement />);

      await waitFor(() => {
        const errorElement = screen.getByText(errorMsg);
        expect(errorElement).toBeInTheDocument();
      });
    });

    it('does not render connection error element when field is undefined', async () => {
      setMockDatabaseInfo({
        success: true,
        database_type: 'lakebase',
        lakebase_instance: 'prod-instance',
        // no connection_error field
      });

      renderWithProviders(<DatabaseManagement />);

      await waitFor(() => {
        expect(screen.getByText(/Current Database Backend: LAKEBASE/)).toBeInTheDocument();
      });

      // Verify no warning-colored text is present in the alert
      const alert = screen.getByRole('alert');
      expect(alert).toHaveClass('MuiAlert-standardSuccess');
    });

    it('renders connection error alongside instance name', async () => {
      setMockDatabaseInfo({
        success: true,
        database_type: 'lakebase',
        lakebase_instance: 'my-failing-instance',
        connection_error: 'Connection refused',
      });

      renderWithProviders(<DatabaseManagement />);

      await waitFor(() => {
        // Both instance name and error should be visible
        expect(screen.getByText(/Connected to Lakebase instance: my-failing-instance/)).toBeInTheDocument();
        expect(screen.getByText('Connection refused')).toBeInTheDocument();
      });
    });
  });
});
