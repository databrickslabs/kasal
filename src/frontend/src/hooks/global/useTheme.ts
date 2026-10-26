import { useShallow } from 'zustand/react/shallow';
import { useThemeStore } from '../../store/theme';

export const useTheme = () => {
  const { currentTheme, isDarkMode, toggleTheme, changeTheme } = useThemeStore(useShallow(state => ({
    currentTheme: state.currentTheme,
    isDarkMode: state.isDarkMode,
    toggleTheme: state.toggleTheme,
    changeTheme: state.changeTheme,
  })));

  return {
    currentTheme,
    isDarkMode,
    toggleTheme,
    changeTheme,
  };
}; 