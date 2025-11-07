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
        // Keep dots in domain to match backend format (e.g., user_nehme.tohme_databricks.com)
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
        
        // Determine effective group id
        const selectedFromStorage = localStorage.getItem('selectedGroupId');
        const prevCurrent = get().currentGroupId || selectedFromStorage;
        const effectiveGroupId = prevCurrent || primaryGroupId;

        // Update store with groups and effective current group
        set({
          groups: allGroups,
          isLoading: false,
          currentGroupId: effectiveGroupId
        });

        // Persist to localStorage if not set yet
        if (!selectedFromStorage) {
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
