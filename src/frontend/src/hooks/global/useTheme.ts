import { useThemeStore } from '../../store/theme';

export const useTheme = () => {
  const { currentTheme, isDarkMode, toggleTheme, changeTheme } = useThemeStore();

  return {
    currentTheme,
    isDarkMode,
    toggleTheme,
    changeTheme,
  };
}; 