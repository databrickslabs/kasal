import { create } from 'zustand';

interface LakebaseConfig {
  enabled: boolean;
  instance_name: string;
  capacity: string;
  retention_days: number;
  node_count: number;
  instance_status?: 'NOT_CREATED' | 'CREATING' | 'READY' | 'STOPPED' | 'ERROR' | 'NOT_FOUND';
  endpoint?: string;
  created_at?: string;
  migration_status?: 'pending' | 'in_progress' | 'completed' | 'failed';
  migration_completed?: boolean;
  migration_result?: {
    total_tables: number;
    total_rows: number;
    migrated_tables?: Array<{ table: string; rows: number }>;
  };
  migration_error?: string;
}

interface DatabaseInfo {
  success: boolean;
  database_path?: string;
  database_type?: string;
  size_mb?: number;
  created_at?: string;
  modified_at?: string;
  tables?: Record<string, number>;
  total_tables?: number;
  error?: string;
  lakebase_instance?: string;
  lakebase_endpoint?: string;
}

interface DatabaseStore {
  // Database info
  databaseInfo: DatabaseInfo | null;
  setDatabaseInfo: (info: DatabaseInfo) => void;

  // Current database backend
  currentBackend: 'sqlite' | 'postgresql' | 'lakebase' | null;
  setCurrentBackend: (backend: 'sqlite' | 'postgresql' | 'lakebase' | null) => void;

  // Lakebase configuration
  lakebaseConfig: LakebaseConfig;
  setLakebaseConfig: (config: Partial<LakebaseConfig>) => void;

  // Lakebase mode
  lakebaseMode: 'create' | 'connect';
  setLakebaseMode: (mode: 'create' | 'connect') => void;

  // Schema state
  schemaExists: boolean;
  setSchemaExists: (exists: boolean) => void;

  // Migration dialog
  showMigrationDialog: boolean;
  setShowMigrationDialog: (show: boolean) => void;

  // Migration option
  migrationOption: 'recreate' | 'use' | 'schema_only';
  setMigrationOption: (option: 'recreate' | 'use' | 'schema_only') => void;

  // Loading states
  loading: boolean;
  setLoading: (loading: boolean) => void;

  checkingInstance: boolean;
  setCheckingInstance: (checking: boolean) => void;

  creatingInstance: boolean;
  setCreatingInstance: (creating: boolean) => void;

  // UI states
  expandedSections: {
    lakebaseConfig: boolean;
  };
  setExpandedSection: (section: 'lakebaseConfig', expanded: boolean) => void;

  // Messages
  error: string | null;
  setError: (error: string | null) => void;

  success: string | null;
  setSuccess: (success: string | null) => void;

  // Reset
  reset: () => void;
}

const defaultLakebaseConfig: LakebaseConfig = {
  enabled: false,
  instance_name: 'kasal-lakebase',
  capacity: 'CU_1',
  retention_days: 14,
  node_count: 1,
  instance_status: 'NOT_CREATED',
};

export const useDatabaseStore = create<DatabaseStore>((set) => ({
  // Initial state
  databaseInfo: null,
  currentBackend: null,
  lakebaseConfig: defaultLakebaseConfig,
  lakebaseMode: 'connect',
  schemaExists: false,
  showMigrationDialog: false,
  migrationOption: 'use',
  loading: false,
  checkingInstance: false,
  creatingInstance: false,
  expandedSections: {
    lakebaseConfig: false,
  },
  error: null,
  success: null,

  // Actions
  setDatabaseInfo: (info) => set({ databaseInfo: info }),

  setCurrentBackend: (backend) => set({ currentBackend: backend }),

  setLakebaseConfig: (config) =>
    set((state) => ({
      lakebaseConfig: { ...state.lakebaseConfig, ...config },
    })),

  setLakebaseMode: (mode) => set({ lakebaseMode: mode }),

  setSchemaExists: (exists) => set({ schemaExists: exists }),

  setShowMigrationDialog: (show) => set({ showMigrationDialog: show }),

  setMigrationOption: (option) => set({ migrationOption: option }),

  setLoading: (loading) => set({ loading }),

  setCheckingInstance: (checking) => set({ checkingInstance: checking }),

  setCreatingInstance: (creating) => set({ creatingInstance: creating }),

  setExpandedSection: (section, expanded) =>
    set((state) => ({
      expandedSections: {
        ...state.expandedSections,
        [section]: expanded,
      },
    })),

  setError: (error) => set({ error }),

  setSuccess: (success) => set({ success }),

  reset: () =>
    set({
      databaseInfo: null,
      currentBackend: null,
      lakebaseConfig: defaultLakebaseConfig,
      lakebaseMode: 'connect',
      schemaExists: false,
      showMigrationDialog: false,
      migrationOption: 'use',
      loading: false,
      checkingInstance: false,
      creatingInstance: false,
      expandedSections: {
        lakebaseConfig: false,
      },
      error: null,
      success: null,
    }),
}));
