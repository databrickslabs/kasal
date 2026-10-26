import { create } from 'zustand';
import { ThemeService } from '../api/config/ThemeService';

const isDarkTheme = (name: string) => name === 'deepOcean' || name === 'dark';
const normalizeTheme = (name: string) => isDarkTheme(name) ? 'deepOcean' : 'professional';

interface ThemeState {
  currentTheme: string;
  isDarkMode: boolean;
  toggleTheme: () => Promise<void>;
  changeTheme: (themeName: string) => Promise<void>;
  initializeTheme: () => Promise<void>;
}

export const useThemeStore = create<ThemeState>((set, get) => ({
  currentTheme: 'professional',
  isDarkMode: false,
  initializeTheme: async () => {
    try {
      const config = await ThemeService.getInstance().getThemeConfig();
      const currentTheme = normalizeTheme(config.theme);
      set({ currentTheme, isDarkMode: isDarkTheme(currentTheme) });
    } catch (error) {
      console.error('Failed to load theme:', error);
    }
  },
  toggleTheme: async () => get().changeTheme(get().isDarkMode ? 'professional' : 'deepOcean'),
  changeTheme: async (name: string) => {
    const currentTheme = normalizeTheme(name);
    set({ currentTheme, isDarkMode: isDarkTheme(currentTheme) });
    try {
      await ThemeService.getInstance().setThemeConfig({ theme: currentTheme });
    } catch (error) {
      // Keep the selected appearance usable even when browser storage is unavailable.
      console.error('Failed to save theme preference:', error);
    }
  },
}));
