import { create } from 'zustand';
import { devtools } from 'zustand/middleware';
import { GroupService, GroupWithRole } from '../api/groups/GroupService';
import { useUserStore } from './user';
import {
  adoptTabGroupId, getOwnTabGroupId, getSharedGroupId, getTabGroupId, setTabGroupId,
} from '../shared/api/tabGroup';

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
    // This tab's own teamspace (see shared/api/tabGroup).
    currentGroupId: getTabGroupId(),

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
        // Start from this tab's own group, never the shared fallback: in a tab
        // that opened with nothing stored, that is whatever another tab picked since.
        const ownGroupId = getOwnTabGroupId();
        const requestGroupId = getTabGroupId();
        const prevCurrent = get().currentGroupId || ownGroupId;
        const isMember = (id: string | null) => !!id && allGroups.some(g => g.id === id);
        const effectiveGroupId = isMember(prevCurrent) ? (prevCurrent as string) : primaryGroupId;

        // Update store with groups and effective current group
        set({
          groups: allGroups,
          isLoading: false,
          currentGroupId: effectiveGroupId
        });

        // Pin the tab whenever its own group changes — "not pinned yet" or "stale →
        // reset to personal". Seed new tabs too, unless another tab's valid choice
        // is already the default: tabs must not overwrite each other's selection.
        if (effectiveGroupId !== ownGroupId) {
          if (isMember(getSharedGroupId())) adoptTabGroupId(effectiveGroupId);
          else setTabGroupId(effectiveGroupId);
        }
        if (effectiveGroupId !== requestGroupId) {
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
      // This tab, and the default for tabs opened later.
      setTabGroupId(groupId);

      // Fire custom event for other components
      const event = new CustomEvent('group-changed', { 
        detail: { groupId } 
      });
      window.dispatchEvent(event);
    }
  }))
);
