import { useThemeStore } from '../../store/theme';
import { ThemeContextType } from '../../types/theme';

/**
 * Hook for theme management, provides backward compatibility with 
 * previous ThemeContext implementation while using the Zustand store
 */
export const useThemeContext = (): ThemeContextType => {
  const { isDarkMode } = useThemeStore();
  
  // Map Zustand store values to the ThemeContext interface
  return {
    mode: isDarkMode ? 'dark' : 'light',
    setMode: (mode: 'light' | 'dark') => {
      // Toggle theme if needed
      if ((mode === 'dark' && !isDarkMode) || 
          (mode === 'light' && isDarkMode)) {
        useThemeStore.getState().toggleTheme();
      }
    }
  };
}; 