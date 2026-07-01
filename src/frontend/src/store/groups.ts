import { create } from 'zustand';
import { devtools } from 'zustand/middleware';
import { GroupService, GroupWithRole } from '../api/GroupService';
import { useUserStore } from './user';

interface GroupState {
  groups: GroupWithRole[];
  isLoading: boolean;
  currentGroupId: string | null;
  
  // Actions
  fetchMyGroups: () => Promise<void>;
  refresh: () => Promise<void>;
  setCurrentGroup: (groupId: string) => void;
  getCurrentGroup: () => GroupWithRole | null;
}

export const useGroupStore = create<GroupState>()(
  devtools((set, get) => ({
    groups: [],
    isLoading: false,
    currentGroupId: localStorage.getItem('selectedGroupId'),

    getCurrentGroup: () => {
      const { groups, currentGroupId } = get();
      if (!currentGroupId) return null;
      return groups.find(g => g.id === currentGroupId) || null;
    },

    fetchMyGroups: async () => {
      const currentUser = useUserStore.getState().currentUser;
      if (!currentUser?.email) return;

      set({ isLoading: true });
      try {
        const groupService = GroupService.getInstance();
        let userGroups: GroupWithRole[] = [];

        try {
          userGroups = await groupService.getMyGroups();
        } catch (error) {
          console.warn('Could not fetch user groups, using empty list:', error);
          userGroups = [];
        }

        // Create personal workspace
        const currentUserEmail = currentUser.email;
        const emailDomain = currentUserEmail.split('@')[1] || '';
        const emailUser = currentUserEmail.split('@')[0] || '';
        // Keep dots in domain to match backend format (e.g., user_jane.doe_databricks.com)
        const primaryGroupId = `user_${emailUser.replace(/\./g, '_')}_${emailDomain.replace(/\./g, '_')}`;

        const personalGroup: GroupWithRole = {
          id: primaryGroupId,
          name: 'My Workspace',
          status: 'active',
          auto_created: true,
          created_at: new Date().toISOString(),
          updated_at: new Date().toISOString(),
          user_count: 1,
          user_role: undefined
        };

        // Add personal group at the beginning
        const allGroups = [personalGroup, ...userGroups.filter(g => g.id !== primaryGroupId)];
        
        // Determine effective group id. Validate the previously-selected group
        // against the workspaces the user actually has RIGHT NOW: a stored group
        // can be stale (e.g. after a redeploy the workspace list is temporarily
        // empty until Lakebase is reconnected, or the user lost access). Falling
        // back to the personal workspace keeps the app usable instead of sending a
        // group_id header the backend rejects; the user re-selects the workspace
        // once it reappears.
        const selectedFromStorage = localStorage.getItem('selectedGroupId');
        const prevCurrent = get().currentGroupId || selectedFromStorage;
        const isPrevValid = !!prevCurrent && allGroups.some(g => g.id === prevCurrent);
        const effectiveGroupId = isPrevValid ? (prevCurrent as string) : primaryGroupId;

        // Update store with groups and effective current group
        set({
          groups: allGroups,
          isLoading: false,
          currentGroupId: effectiveGroupId
        });

        // Persist whenever the effective group differs from what's stored — covers
        // both "not set yet" and "stored group was stale/invalid → reset to personal".
        if (effectiveGroupId !== selectedFromStorage) {
          localStorage.setItem('selectedGroupId', effectiveGroupId);
        }
      } catch (error) {
        console.error('Failed to fetch user groups:', error);
        set({ isLoading: false });
      }
    },

    refresh: async () => {
      await get().fetchMyGroups();
    },

    setCurrentGroup: (groupId: string) => {
      set({ currentGroupId: groupId });
      localStorage.setItem('selectedGroupId', groupId);
      
      // Fire custom event for other components
      const event = new CustomEvent('group-changed', { 
        detail: { groupId } 
      });
      window.dispatchEvent(event);
    }
  }))
);
