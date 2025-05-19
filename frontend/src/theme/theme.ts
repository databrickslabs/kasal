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

// Professional Theme (Blue-based)
// A clean, professional palette with blue as the primary color
const professionalTheme: ThemeColors = {
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
};

// Calm Earth Theme
// A soothing, nature-inspired palette with green and earth tones
const calmEarthTheme: ThemeColors = {
  primary: {
    main: '#4caf50',
    light: '#80e27e',
    dark: '#087f23',
    contrastText: '#ffffff',
  },
  secondary: {
    main: '#795548',
    light: '#a98274',
    dark: '#4b2c20',
    contrastText: '#ffffff',
  },
  background: {
    default: '#f8f9f2',
    paper: '#ffffff',
    subtle: '#eef2e6',
  },
  text: {
    primary: '#33402a',
    secondary: '#5d6852',
    disabled: '#a5bdaa',
  },
  success: {
    main: '#689f38',
    light: '#99d066',
    dark: '#387002',
  },
  info: {
    main: '#03a9f4',
    light: '#67daff',
    dark: '#007ac1',
  },
  warning: {
    main: '#f57c00',
    light: '#ffad42',
    dark: '#bc5100',
  },
  error: {
    main: '#d32f2f',
    light: '#ff6659',
    dark: '#9a0007',
  },
  divider: 'rgba(0, 0, 0, 0.09)',
};

// Deep Ocean Theme
// A sophisticated dark palette with deep blue tones
const deepOceanTheme: ThemeColors = {
  primary: {
    main: '#0277bd',
    light: '#58a5f0',
    dark: '#004c8c',
    contrastText: '#ffffff',
  },
  secondary: {
    main: '#263238',
    light: '#4f5b62',
    dark: '#000a12',
    contrastText: '#ffffff',
  },
  background: {
    default: '#121212',
    paper: '#1e1e2d',
    subtle: '#1a1a27',
  },
  text: {
    primary: '#e0e0e0',
    secondary: '#b0bec5',
    disabled: '#636363',
  },
  success: {
    main: '#00c853',
    light: '#5efc82',
    dark: '#009624',
  },
  info: {
    main: '#00b0ff',
    light: '#69e2ff',
    dark: '#0081cb',
  },
  warning: {
    main: '#ffd600',
    light: '#ffff52',
    dark: '#c7a500',
  },
  error: {
    main: '#ff3d00',
    light: '#ff7539',
    dark: '#c30000',
  },
  divider: 'rgba(255, 255, 255, 0.12)',
};

// Vibrant Creative Theme
// A bold, creative palette with purple as the primary color
const vibrantCreativeTheme: ThemeColors = {
  primary: {
    main: '#6200ea',
    light: '#9d46ff',
    dark: '#0a00b6',
    contrastText: '#ffffff',
  },
  secondary: {
    main: '#ff4081',
    light: '#ff79b0',
    dark: '#c60055',
    contrastText: '#ffffff',
  },
  background: {
    default: '#f5f5f7',
    paper: '#ffffff',
    subtle: '#f0f0f5',
  },
  text: {
    primary: '#2c2c2c',
    secondary: '#666666',
    disabled: '#a3a3a3',
  },
  success: {
    main: '#00bfa5',
    light: '#5df2d6',
    dark: '#008e76',
  },
  info: {
    main: '#304ffe',
    light: '#7a7cff',
    dark: '#0026ca',
  },
  warning: {
    main: '#ffab00',
    light: '#ffdd4b',
    dark: '#c67c00',
  },
  error: {
    main: '#ff1744',
    light: '#ff616f',
    dark: '#c4001d',
  },
  divider: 'rgba(0, 0, 0, 0.08)',
};

// Theme dictionary
const themes: Record<string, ThemeColors> = {
  professional: professionalTheme,
  calmEarth: calmEarthTheme, 
  deepOcean: deepOceanTheme,
  vibrantCreative: vibrantCreativeTheme,
};

// Function to get theme options based on theme name
export const getThemeOptions = (themeName: string): ThemeOptions => ({
  ...globalStyles,
  palette: themes[themeName] || professionalTheme,
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
          borderRadius: '12px',
        },
      },
    },
  },
});

// Default theme
const theme = createTheme(getThemeOptions('professional'));

export default theme; 