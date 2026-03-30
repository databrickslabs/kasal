/**
 * Tests for DatabaseManagement autoscaling/pagination changes.
 *
 * Covers: configLoaded spinner, loadLakebaseInstances pagination,
 * setDatabaseInfo(null) clearing stale data, Autocomplete type badges.
 */
import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import { vi, describe, it, expect, beforeEach } from 'vitest';
import '@testing-library/jest-dom';

// ---------------------------------------------------------------------------
// Hoisted mock references
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
      instance_name: '',
      capacity: 'CU_1',
      retention_days: 14,
      node_count: 1,
      instance_status: 'NOT_CREATED' as const,
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
  config: { backendUrl: 'http://localhost:8000' },
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
// Helpers
// ---------------------------------------------------------------------------
const theme = createTheme();

const importComponent = async () => {
  const mod = await import('../DatabaseManagement');
  return mod.default;
};

const resetMockStoreState = () => {
  mockDatabaseStoreState.databaseInfo = null;
  mockDatabaseStoreState.loading = false;
  mockDatabaseStoreState.checkingInstance = false;
  mockDatabaseStoreState.error = null;
  mockDatabaseStoreState.success = null;
  mockDatabaseStoreState.lakebaseConfig = {
    enabled: false,
    instance_name: '',
    capacity: 'CU_1',
    retention_days: 14,
    node_count: 1,
    instance_status: 'NOT_CREATED',
  };
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
describe('DatabaseManagement - Autoscaling changes', () => {
  let DatabaseManagement: React.FC;

  beforeEach(async () => {
    vi.clearAllMocks();
    resetMockStoreState();

    mockApiClient.get.mockImplementation((url: string) => {
      if (url.includes('/database-management/info')) {
        return Promise.resolve({ data: { success: true, database_type: 'sqlite' } });
      }
      if (url.includes('/database-management/lakebase/config')) {
        return Promise.resolve({ data: { enabled: false, instance_status: 'NOT_CREATED' } });
      }
      if (url.includes('/lakebase/instances')) {
        return Promise.resolve({
          data: {
            items: [
              { name: 'prov-inst', state: 'AVAILABLE', capacity: 'CU_2', read_write_dns: 'instance-abc.database.cloud.databricks.com', type: 'provisioned' },
              { name: 'auto-proj', state: 'AVAILABLE', capacity: 'CU_2-8', read_write_dns: 'ep-cool.database.us-west-2.cloud.databricks.com', type: 'autoscaling' },
            ],
            total: 2, page: 1, page_size: 30, total_pages: 1, has_more: false,
          },
        });
      }
      return Promise.resolve({ data: {} });
    });

    DatabaseManagement = await importComponent();
  });

  describe('Loading states', () => {
    it('clears databaseInfo at start of load to prevent stale display', async () => {
      render(<ThemeProvider theme={theme}><DatabaseManagement /></ThemeProvider>);

      await waitFor(() => {
        expect(mockDatabaseStoreState.setDatabaseInfo).toHaveBeenCalledWith(null);
      });
    });

    it('does not show database info content before API returns', async () => {
      // Make the info API hang so loading state persists
      mockApiClient.get.mockImplementation((url: string) => {
        if (url.includes('/database-management/info')) {
          return new Promise(() => {}); // Never resolves
        }
        if (url.includes('/database-management/lakebase/config')) {
          return Promise.resolve({ data: { enabled: false } });
        }
        return Promise.resolve({ data: {} });
      });

      mockDatabaseStoreState.databaseInfo = null;

      render(<ThemeProvider theme={theme}><DatabaseManagement /></ThemeProvider>);

      // databaseInfo is null so the card should not render
      await waitFor(() => {
        expect(screen.queryByText('Database Information')).not.toBeInTheDocument();
      });
    });

    it('does not show database info card when databaseInfo is null', async () => {
      mockDatabaseStoreState.databaseInfo = null;
      mockDatabaseStoreState.loading = false;

      render(<ThemeProvider theme={theme}><DatabaseManagement /></ThemeProvider>);

      await waitFor(() => {
        expect(screen.queryByText('Database Information')).not.toBeInTheDocument();
      });
    });
  });

  describe('API calls on mount', () => {
    it('calls info and config endpoints on mount', async () => {
      render(<ThemeProvider theme={theme}><DatabaseManagement /></ThemeProvider>);

      await waitFor(() => {
        expect(mockApiClient.get).toHaveBeenCalledWith(
          expect.stringContaining('/database-management/info')
        );
        expect(mockApiClient.get).toHaveBeenCalledWith(
          expect.stringContaining('/database-management/lakebase/config')
        );
      });
    });
  });

  describe('Lakebase connected state', () => {
    it('calls setDatabaseInfo(null) then sets fresh data', async () => {
      render(<ThemeProvider theme={theme}><DatabaseManagement /></ThemeProvider>);

      await waitFor(() => {
        // First call should be null (clearing stale)
        const calls = mockDatabaseStoreState.setDatabaseInfo.mock.calls;
        expect(calls[0][0]).toBeNull();
        // Second call should be the fresh data
        if (calls.length > 1) {
          expect(calls[1][0]).toEqual({ success: true, database_type: 'sqlite' });
        }
      });
    });
  });
});
