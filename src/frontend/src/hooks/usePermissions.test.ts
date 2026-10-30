import { describe, expect, it, vi } from 'vitest';
import { renderHook } from '@testing-library/react';

const loadPermissions = vi.fn();

vi.mock('../store/permissions', () => ({
  usePermissionStore: (select: (state: unknown) => unknown) =>
    select({ loadPermissions, userRole: null, isLoading: false }),
}));

import { usePermissionLoader } from './usePermissions';

describe('usePermissionLoader', () => {
  it('keeps this tab\'s permissions when another tab switches teamspace', () => {
    renderHook(() => usePermissionLoader());
    loadPermissions.mockClear();

    window.dispatchEvent(new StorageEvent('storage', { key: 'selectedGroupId', oldValue: 'team_a', newValue: 'team_b' }));

    expect(loadPermissions).not.toHaveBeenCalled();
  });
});
