import React, { ReactNode, useEffect } from 'react';
import { ThemeProvider as MuiThemeProvider, createTheme } from '@mui/material/styles';
import { CssBaseline } from '@mui/material';
import { getThemeOptions } from '../../theme';
import { useThemeStore } from '../../store/theme';

interface ThemeProviderProps {
  children: ReactNode;
}

// Custom ThemeProvider that uses Zustand for theme state management
// This helps prevent styling conflicts that can cause the 'insertBefore' Node error
const ThemeProvider: React.FC<ThemeProviderProps> = ({ children }) => {
  const { currentTheme, initializeTheme } = useThemeStore();
  
  // Initialize theme on component mount
  useEffect(() => {
    initializeTheme();
  }, [initializeTheme]);
  
  // Create the theme based on the current theme name from Zustand store
  const theme = createTheme(getThemeOptions(currentTheme));
  
  return (
    <MuiThemeProvider theme={theme}>
      <CssBaseline />
      {children}
    </MuiThemeProvider>
  );
};

export default ThemeProvider; 