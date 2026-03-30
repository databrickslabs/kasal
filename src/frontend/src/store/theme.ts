import { create } from 'zustand';
import { ThemeService } from '../api/ThemeService';

// Helper to determine if a theme is dark mode
const isDarkTheme = (themeName: string): boolean => {
  return ['deepOcean'].includes(themeName);
};

interface ThemeState {
  currentTheme: string;
  isDarkMode: boolean;
  toggleTheme: () => Promise<void>;
  changeTheme: (themeName: string) => Promise<void>;
  initializeTheme: () => Promise<void>;
}

export const useThemeStore = create<ThemeState>((set) => ({
  currentTheme: 'professional',
  isDarkMode: false,
  
  // Initialize theme from server
  initializeTheme: async () => {
    try {
      const themeService = ThemeService.getInstance();
      const config = await themeService.getThemeConfig();
      set({
        currentTheme: config.theme,
        isDarkMode: isDarkTheme(config.theme),
      });
    } catch (error) {
      console.error('Failed to load theme:', error);
    }
  },

  toggleTheme: async () => {
    set((state) => {
      // Toggle between professional and the deep ocean dark theme
      const newTheme = state.isDarkMode 
        ? 'professional' 
        : 'deepOcean';
      
      return {
        currentTheme: newTheme,
        isDarkMode: isDarkTheme(newTheme),
      };
    });

    try {
      const themeService = ThemeService.getInstance();
      const newTheme = (await useThemeStore.getState()).currentTheme;
      await themeService.setThemeConfig({ theme: newTheme });
      window.location.reload();
    } catch (error) {
      console.error('Failed to toggle theme:', error);
      // Revert the state if the API call fails
      set((state) => {
        const revertedTheme = state.isDarkMode ? 'professional' : 'deepOcean';
        return {
          currentTheme: revertedTheme,
          isDarkMode: isDarkTheme(revertedTheme),
        };
      });
    }
  },
  
  changeTheme: async (themeName: string) => {
    set(() => ({
      currentTheme: themeName,
      isDarkMode: isDarkTheme(themeName),
    }));

    try {
      const themeService = ThemeService.getInstance();
      await themeService.setThemeConfig({ theme: themeName });
      window.location.reload();
    } catch (error) {
      console.error('Failed to change theme:', error);
      // Revert the state if the API call fails
      set((state) => ({
        currentTheme: state.currentTheme,
        isDarkMode: isDarkTheme(state.currentTheme),
      }));
    }
  },
})); 