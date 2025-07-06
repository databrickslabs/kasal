import { useTheme } from '../../hooks/global/useTheme';

export const useThemeManager = () => {
  // Use the global theme hook which uses Zustand store
  return useTheme();
}; 