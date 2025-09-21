import { create } from 'zustand';
import { persist } from 'zustand/middleware';

interface UserPreferences {
  useNewExecutionUI: boolean;
}

interface UserPreferencesStore extends UserPreferences {
  setUseNewExecutionUI: (value: boolean) => void;
  resetPreferences: () => void;
}

const defaultPreferences: UserPreferences = {
  useNewExecutionUI: true, // Default to streamlined view
};

export const useUserPreferencesStore = create<UserPreferencesStore>()(
  persist(
    (set) => ({
      ...defaultPreferences,

      setUseNewExecutionUI: (value: boolean) =>
        set({ useNewExecutionUI: value }),

      resetPreferences: () =>
        set(defaultPreferences),
    }),
    {
      name: 'user-preferences',
    }
  )
);