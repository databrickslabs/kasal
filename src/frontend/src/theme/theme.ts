import { kasalStageSurface } from './kasalSurfaces';
import { createTheme, ThemeOptions } from '@mui/material/styles';

// Extend TypeBackground to include 'subtle' property
declare module '@mui/material/styles' {
  interface TypeBackground {
    subtle: string;
  }
}

// Define theme interfaces
export interface ThemeColors {
  primary: {
    main: string;
    light: string;
    dark: string;
    contrastText: string;
  };
  secondary: {
    main: string;
    light: string;
    dark: string;
    contrastText: string;
  };
  background: {
    default: string;
    paper: string;
    subtle: string;
  };
  text: {
    primary: string;
    secondary: string;
    disabled: string;
  };
  success: {
    main: string;
    light: string;
    dark: string;
  };
  info: {
    main: string;
    light: string;
    dark: string;
  };
  warning: {
    main: string;
    light: string;
    dark: string;
  };
  error: {
    main: string;
    light: string;
    dark: string;
  };
  divider: string;
}

// Global styles
const globalStyles = {
  components: {
    MuiCssBaseline: {
      styleOverrides: {
        body: {
          margin: 0,
          fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", "Roboto", "Oxygen", "Ubuntu", "Cantarell", "Fira Sans", "Droid Sans", "Helvetica Neue", sans-serif',
          WebkitFontSmoothing: 'antialiased',
          MozOsxFontSmoothing: 'grayscale',
        },
        code: {
          fontFamily: 'source-code-pro, Menlo, Monaco, Consolas, "Courier New", monospace',
        },
      },
    },
  },
};

// One Kasal palette in light and dark. Legacy names are accepted for saved preferences.
const lightTheme: ThemeColors = {
  primary: { main: '#343D46', light: '#586570', dark: '#1B1F23', contrastText: '#FFFFFF' },
  secondary: { main: '#5A6872', light: '#8D99A4', dark: '#343D46', contrastText: '#FFFFFF' },
  background: { default: '#F5F7FA', paper: '#FFFFFF', subtle: '#F0F2F5' },
  text: { primary: '#1B1F23', secondary: '#5A6872', disabled: '#8D99A4' },
  success: { main: '#35805B', light: '#69A587', dark: '#246342' },
  info: { main: '#5A6872', light: '#8D99A4', dark: '#343D46' },
  warning: { main: '#A77421', light: '#D5AD68', dark: '#805617' },
  error: { main: '#BB514C', light: '#D77D77', dark: '#983C37' },
  divider: '#DFE3E8',
};
const darkTheme: ThemeColors = {
  primary: { main: '#D4DCE3', light: '#E8ECEF', dark: '#A0AAB4', contrastText: '#1B1F23' },
  secondary: { main: '#A0AAB4', light: '#D4DCE3', dark: '#6B7785', contrastText: '#1B1F23' },
  background: { default: '#15191E', paper: '#1B1F23', subtle: '#232930' },
  text: { primary: '#E8ECEF', secondary: '#A0AAB4', disabled: '#6B7785' },
  success: { main: '#69A587', light: '#93C6AA', dark: '#35805B' },
  info: { main: '#A0AAB4', light: '#D4DCE3', dark: '#6B7785' },
  warning: { main: '#D5AD68', light: '#E8C78D', dark: '#A77421' },
  error: { main: '#D77D77', light: '#E9A19B', dark: '#BB514C' },
  divider: '#333C45',
};

// Function to get theme options based on theme name
export const getThemeOptions = (themeName: string): ThemeOptions => ({
  ...globalStyles,
  palette: {
    ...(themeName === 'deepOcean' || themeName === 'dark' ? darkTheme : lightTheme),
    mode: themeName === 'deepOcean' || themeName === 'dark' ? 'dark' : 'light',
  },
  typography: {
    fontFamily: "'Inter', 'Roboto', 'Helvetica', 'Arial', sans-serif",
    h1: {
      fontWeight: 600,
    },
    h2: {
      fontWeight: 600,
    },
    h3: {
      fontWeight: 600,
    },
    h4: {
      fontWeight: 600,
    },
    h5: {
      fontWeight: 600,
    },
    h6: {
      fontWeight: 600,
    },
    subtitle1: {
      fontWeight: 500,
    },
    button: {
      fontWeight: 600,
      textTransform: 'none',
    },
  },
  shape: {
    borderRadius: 8,
  },
  components: {
    MuiButton: {
      styleOverrides: {
        root: {
          textTransform: 'none',
          borderRadius: '8px',
          fontWeight: 500,
          padding: '6px 16px',
        },
        containedPrimary: {
          boxShadow: 'none',
          '&:hover': {
            boxShadow: '0 2px 8px rgba(0, 0, 0, 0.15)',
          },
        },
        outlinedPrimary: {
          borderWidth: '1.5px',
          '&:hover': {
            borderWidth: '1.5px',
          },
        },
      },
    },
    MuiDialogTitle: { styleOverrides: { root: { ...kasalStageSurface(themeName === 'deepOcean' || themeName === 'dark') } } },
    MuiDialogContent: { styleOverrides: { root: { ...kasalStageSurface(themeName === 'deepOcean' || themeName === 'dark') } } },
    MuiDialogActions: { styleOverrides: { root: { ...kasalStageSurface(themeName === 'deepOcean' || themeName === 'dark') } } },
    MuiCard: {
      styleOverrides: {
        root: {
          borderRadius: '12px',
          boxShadow: '0 2px 12px rgba(0, 0, 0, 0.08)',
        },
      },
    },
    MuiPaper: {
      styleOverrides: {
        root: {
          ...kasalStageSurface(themeName === 'deepOcean' || themeName === 'dark'),
          borderRadius: '12px',
        },
      },
    },
  },
});

// Default theme
const theme = createTheme(getThemeOptions('professional'));

export default theme; 