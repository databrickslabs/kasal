import { describe, it, expect, beforeEach } from 'vitest';
import { useDatabaseStore } from '../databaseStore';

describe('databaseStore - nullable setDatabaseInfo', () => {
  beforeEach(() => {
    useDatabaseStore.getState().reset();
  });

  it('setDatabaseInfo accepts null to clear stale data', () => {
    // Set some data first
    useDatabaseStore.getState().setDatabaseInfo({ success: true, database_type: 'lakebase' });
    expect(useDatabaseStore.getState().databaseInfo).not.toBeNull();

    // Clear it
    useDatabaseStore.getState().setDatabaseInfo(null);
    expect(useDatabaseStore.getState().databaseInfo).toBeNull();
  });

  it('setDatabaseInfo accepts DatabaseInfo object after null', () => {
    useDatabaseStore.getState().setDatabaseInfo(null);
    expect(useDatabaseStore.getState().databaseInfo).toBeNull();

    useDatabaseStore.getState().setDatabaseInfo({ success: true, database_type: 'sqlite' });
    expect(useDatabaseStore.getState().databaseInfo).toEqual({ success: true, database_type: 'sqlite' });
  });

  it('reset sets databaseInfo back to null', () => {
    useDatabaseStore.getState().setDatabaseInfo({ success: true });
    useDatabaseStore.getState().reset();
    expect(useDatabaseStore.getState().databaseInfo).toBeNull();
  });
});
