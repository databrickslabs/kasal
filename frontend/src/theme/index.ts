import { createTheme } from '@mui/material/styles';
export * from './theme';

// Re-export commonly used theme types
export type { ThemeColors } from './theme';

// Export a default theme instance using the professional theme
export const defaultTheme = createTheme({
  palette: {
    primary: {
      main: '#1976d2',
      light: '#4791db',
      dark: '#115293',
      contrastText: '#ffffff',
    },
    secondary: {
      main: '#455a64',
      light: '#718792',
      dark: '#1c313a',
      contrastText: '#ffffff',
    },
    background: {
      default: '#f5f7fa',
      paper: '#ffffff',
      subtle: '#eef2f6',
    },
    text: {
      primary: '#2a3747',
      secondary: '#546e7a',
      disabled: '#a0aec0',
    },
    success: {
      main: '#4caf50',
      light: '#81c784',
      dark: '#388e3c',
    },
    info: {
      main: '#2196f3',
      light: '#64b5f6',
      dark: '#1976d2',
    },
    warning: {
      main: '#ff9800',
      light: '#ffb74d',
      dark: '#f57c00',
    },
    error: {
      main: '#f44336',
      light: '#e57373',
      dark: '#d32f2f',
    },
    divider: 'rgba(0, 0, 0, 0.12)',
  },
}); 