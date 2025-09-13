import { create } from 'zustand';
import { persist } from 'zustand/middleware';

interface UserPreferences {
  useNewExecutionUI: boolean;
  hasSeenUIPreferenceDialog: boolean;
}

interface UserPreferencesStore extends UserPreferences {
  setUseNewExecutionUI: (value: boolean) => void;
  setHasSeenUIPreferenceDialog: (value: boolean) => void;
  resetPreferences: () => void;
}

const defaultPreferences: UserPreferences = {
  useNewExecutionUI: false,
  hasSeenUIPreferenceDialog: false,
};

export const useUserPreferencesStore = create<UserPreferencesStore>()(
  persist(
    (set) => ({
      ...defaultPreferences,
      
      setUseNewExecutionUI: (value: boolean) => 
        set({ useNewExecutionUI: value }),
      
      setHasSeenUIPreferenceDialog: (value: boolean) => 
        set({ hasSeenUIPreferenceDialog: value }),
      
      resetPreferences: () => 
        set(defaultPreferences),
    }),
    {
      name: 'user-preferences',
    }
  )
);