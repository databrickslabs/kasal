import { create } from 'zustand';
import { devtools } from 'zustand/middleware';
import { apiClient } from '../config/api/ApiConfig';

interface UserInfo {
  id: string;
  email: string;
  username: string;
  displayName?: string;
  is_system_admin?: boolean;
  is_personal_workspace_manager?: boolean;
}

interface UserState {
  // State
  currentUser: UserInfo | null;
  isLoading: boolean;
  error: string | null;
  lastFetched: number | null;

  // Actions
  fetchCurrentUser: () => Promise<void>;
  clearUser: () => void;
}

export const useUserStore = create<UserState>()(
  devtools(
    (set, get) => ({
      // Initial state
      currentUser: null,
      isLoading: false,
      error: null,
      lastFetched: null,

      // Fetch current user from backend (which reads X-Forwarded-Email header)
      fetchCurrentUser: async () => {
        const { isLoading } = get();

        // Prevent concurrent fetches
        if (isLoading) return;

        set({ isLoading: true, error: null });

        try {
          // This endpoint reads the X-Forwarded-Email header from the browser
          const response = await apiClient.get('/users/me');
          const userInfo = response.data;

          if (userInfo && userInfo.email) {
            const newUser: UserInfo = {
              id: userInfo.id,
              email: userInfo.email,
              username: userInfo.username,
              displayName: userInfo.profile?.display_name || userInfo.username,
              is_system_admin: userInfo.is_system_admin || false,
              is_personal_workspace_manager: userInfo.is_personal_workspace_manager || false
            };

            // Check if user has changed
            const currentUser = get().currentUser;
            const hasChanged = !currentUser || currentUser.email !== newUser.email;

            if (hasChanged) {
              console.log('User changed detected via X-Forwarded-Email:', newUser.email);

              // Update localStorage for other components that might use it
              localStorage.setItem('userId', newUser.id);
              localStorage.setItem('userEmail', newUser.email);
              localStorage.setItem('user', JSON.stringify({
                id: newUser.id,
                email: newUser.email,
                name: newUser.displayName
              }));

              // Only update state if user actually changed
              set({
                currentUser: newUser,
                isLoading: false,
                lastFetched: Date.now(),
                error: null
              });
            } else {
              // Just update the loading state and timestamp if no change
              set({
                isLoading: false,
                lastFetched: Date.now()
              });
            }
          }
        } catch (error) {
          console.error('Failed to fetch current user:', error);

          // Try to get from localStorage as fallback
          const cachedEmail = localStorage.getItem('userEmail');
          const cachedUserId = localStorage.getItem('userId');

          if (cachedEmail && cachedUserId) {
            const userStr = localStorage.getItem('user');
            let displayName = cachedEmail.split('@')[0];

            if (userStr) {
              try {
                const userData = JSON.parse(userStr);
                displayName = userData.name || displayName;
              } catch (e) {
                console.error('Failed to parse cached user data:', e);
              }
            }

            set({
              currentUser: {
                id: cachedUserId,
                email: cachedEmail,
                username: cachedEmail.split('@')[0],
                displayName,
                is_system_admin: false,
                is_personal_workspace_manager: false
              },
              isLoading: false,
              error: 'Using cached user data'
            });
          } else {
            set({
              currentUser: null,
              isLoading: false,
              error: error instanceof Error ? error.message : 'Failed to fetch user'
            });
          }
        }
      },


      // Clear user data
      clearUser: () => {
        set({
          currentUser: null,
          isLoading: false,
          error: null,
          lastFetched: null
        });

        // Clear localStorage
        localStorage.removeItem('userId');
        localStorage.removeItem('userEmail');
        localStorage.removeItem('user');
      }
    }),
    {
      name: 'UserStore'
    }
  )
);