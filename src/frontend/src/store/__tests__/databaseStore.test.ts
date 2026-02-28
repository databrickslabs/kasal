import { describe, it, expect, beforeEach } from 'vitest';
import { useDatabaseStore } from '../databaseStore';

describe('databaseStore', () => {
  beforeEach(() => {
    useDatabaseStore.getState().reset();
  });

  it('has correct initial state', () => {
    const state = useDatabaseStore.getState();
    expect(state.databaseInfo).toBeNull();
    expect(state.currentBackend).toBeNull();
    expect(state.lakebaseConfig).toEqual({
      enabled: false,
      instance_name: 'kasal-lakebase',
      capacity: 'CU_1',
      retention_days: 14,
      node_count: 1,
      instance_status: 'NOT_CREATED',
    });
    expect(state.schemaExists).toBe(false);
    expect(state.showMigrationDialog).toBe(false);
    expect(state.migrationOption).toBe('recreate');
    expect(state.loading).toBe(false);
    expect(state.checkingInstance).toBe(false);
    expect(state.expandedSections).toEqual({ lakebaseConfig: false });
    expect(state.error).toBeNull();
    expect(state.success).toBeNull();
  });

  it('setDatabaseInfo updates databaseInfo', () => {
    const info = { success: true, database_type: 'sqlite' };
    useDatabaseStore.getState().setDatabaseInfo(info);
    expect(useDatabaseStore.getState().databaseInfo).toEqual(info);
  });

  it('setCurrentBackend updates currentBackend', () => {
    useDatabaseStore.getState().setCurrentBackend('lakebase');
    expect(useDatabaseStore.getState().currentBackend).toBe('lakebase');
  });

  it('setLakebaseConfig merges partial config', () => {
    useDatabaseStore.getState().setLakebaseConfig({ enabled: true, instance_status: 'READY' });
    const config = useDatabaseStore.getState().lakebaseConfig;
    expect(config.enabled).toBe(true);
    expect(config.instance_status).toBe('READY');
    // Other fields preserved
    expect(config.instance_name).toBe('kasal-lakebase');
    expect(config.capacity).toBe('CU_1');
  });

  it('setSchemaExists updates schemaExists', () => {
    useDatabaseStore.getState().setSchemaExists(true);
    expect(useDatabaseStore.getState().schemaExists).toBe(true);
  });

  it('setShowMigrationDialog updates showMigrationDialog', () => {
    useDatabaseStore.getState().setShowMigrationDialog(true);
    expect(useDatabaseStore.getState().showMigrationDialog).toBe(true);
  });

  it('setMigrationOption updates migrationOption', () => {
    useDatabaseStore.getState().setMigrationOption('schema_only');
    expect(useDatabaseStore.getState().migrationOption).toBe('schema_only');
  });

  it('setLoading updates loading', () => {
    useDatabaseStore.getState().setLoading(true);
    expect(useDatabaseStore.getState().loading).toBe(true);
  });

  it('setCheckingInstance updates checkingInstance', () => {
    useDatabaseStore.getState().setCheckingInstance(true);
    expect(useDatabaseStore.getState().checkingInstance).toBe(true);
  });

  it('setExpandedSection updates a specific section', () => {
    useDatabaseStore.getState().setExpandedSection('lakebaseConfig', true);
    expect(useDatabaseStore.getState().expandedSections.lakebaseConfig).toBe(true);
  });

  it('setError updates error', () => {
    useDatabaseStore.getState().setError('Something went wrong');
    expect(useDatabaseStore.getState().error).toBe('Something went wrong');
  });

  it('setSuccess updates success', () => {
    useDatabaseStore.getState().setSuccess('All good');
    expect(useDatabaseStore.getState().success).toBe('All good');
  });

  it('reset restores all state to defaults', () => {
    // Mutate everything
    useDatabaseStore.getState().setDatabaseInfo({ success: true });
    useDatabaseStore.getState().setCurrentBackend('lakebase');
    useDatabaseStore.getState().setLakebaseConfig({ enabled: true, instance_status: 'READY' });
    useDatabaseStore.getState().setSchemaExists(true);
    useDatabaseStore.getState().setShowMigrationDialog(true);
    useDatabaseStore.getState().setMigrationOption('use');
    useDatabaseStore.getState().setLoading(true);
    useDatabaseStore.getState().setCheckingInstance(true);
    useDatabaseStore.getState().setExpandedSection('lakebaseConfig', true);
    useDatabaseStore.getState().setError('err');
    useDatabaseStore.getState().setSuccess('ok');

    // Reset
    useDatabaseStore.getState().reset();

    const state = useDatabaseStore.getState();
    expect(state.databaseInfo).toBeNull();
    expect(state.currentBackend).toBeNull();
    expect(state.lakebaseConfig.enabled).toBe(false);
    expect(state.lakebaseConfig.instance_status).toBe('NOT_CREATED');
    expect(state.schemaExists).toBe(false);
    expect(state.showMigrationDialog).toBe(false);
    expect(state.migrationOption).toBe('recreate');
    expect(state.loading).toBe(false);
    expect(state.checkingInstance).toBe(false);
    expect(state.expandedSections.lakebaseConfig).toBe(false);
    expect(state.error).toBeNull();
    expect(state.success).toBeNull();
  });
});
