import { create } from 'zustand';
import { devtools } from 'zustand/middleware';
import { GroupService, GroupWithRole } from '../api/groups/GroupService';
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

let groupsRequestVersion = 0;

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
      const requestVersion = ++groupsRequestVersion;
      let currentUser = useUserStore.getState().currentUser;
      if (!currentUser?.email) return;

      const email = currentUser.email;
      const isCurrent = () => requestVersion === groupsRequestVersion
        && useUserStore.getState().currentUser?.email === email;
      set({ isLoading: true });
      try {
        if (!currentUser.personal_group_id) {
          await useUserStore.getState().fetchCurrentUser();
          currentUser = useUserStore.getState().currentUser;
        }
        const primaryGroupId = currentUser?.personal_group_id;
        if (!primaryGroupId) {
          throw new Error('Personal workspace allocation is unavailable');
        }
        const groupService = GroupService.getInstance();
        const userGroups = await groupService.getMyGroups();
        if (!isCurrent()) return;

        // The authenticated user's allocated ID is the only personal scope.
        const personalGroup: GroupWithRole = {
          id: primaryGroupId,
          name: 'Personal Space',
          status: 'active',
          auto_created: true,
          created_at: new Date().toISOString(),
          updated_at: new Date().toISOString(),
          user_count: 1,
          user_role: undefined
        };

        // Add personal group at the beginning
        const allGroups = [personalGroup, ...userGroups.filter(g => g.id !== primaryGroupId)];
        
        // Only a successful membership response can invalidate the selection.
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
          window.dispatchEvent(new CustomEvent('group-changed', { detail: { groupId: effectiveGroupId } }));
        }
      } catch (error) {
        if (!isCurrent()) return;
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
