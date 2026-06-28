import { createTheme, type Theme } from '@mui/material/styles';

/**
 * Chat-mode MUI theme — the replacement for the hand-written CSS variables in
 * chat.css. Chat components style themselves through this theme (palette + sx)
 * instead of `var(--…)` + Tailwind utilities, so the chat shell can drop its
 * dependency on chat.css / Tailwind.
 *
 * The token VALUES mirror chat.css exactly (light + dark) so the look is preserved
 * one-for-one through the migration. This is the foundation other slices build on:
 * a component is "migrated" when its `className`/`var(--…)` styling is replaced by
 * `sx` referencing this theme.
 *
 * Standard MUI palette slots carry the common tokens; the `chat` extension holds
 * the chat-specific extras (secondary surfaces, rail tints, accent hover, shadows)
 * that have no standard MUI slot.
 */
export interface ChatTokens {
  bgPrimary: string;
  bgSecondary: string;
  bgSidebar: string;
  bgInput: string;
  bgUserMsg: string;
  textMuted: string;
  accentHover: string;
  bgRail: string;
  bgRailHover: string;
  bgActiveChip: string;
  shadowInput: string;
  shadowInputFocus: string;
  shadowPopover: string;
}

// Augment the MUI Theme with the chat-specific tokens (no standard slot) so they
// are available as `theme.chat.*` / typed in `sx` callbacks.
declare module '@mui/material/styles' {
  interface Theme {
    chat: ChatTokens;
  }
  interface ThemeOptions {
    chat?: ChatTokens;
  }
}

const FONT_STACK =
  "'Söhne', 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif";

// Values copied verbatim from chat.css (.kasal-chat-root[data-theme=…]).
const PALETTES = {
  light: {
    bgPrimary: '#FFFFFF',
    bgSecondary: '#F5F7FA',
    bgSidebar: '#F5F7FA',
    bgInput: '#FFFFFF',
    bgUserMsg: '#EEF1F5',
    textPrimary: '#1B1F23',
    textSecondary: '#5A6872',
    textMuted: '#8D99A4',
    border: '#DFE3E8',
    accent: '#FF3621',
    accentHover: '#E02E1B',
    bgRail: '#F7F8FA',
    bgRailHover: 'rgba(27, 31, 35, 0.045)',
    bgActiveChip: 'rgba(27, 31, 35, 0.075)',
    shadowInput: '0 1px 2px rgba(16, 24, 40, 0.05), 0 4px 16px rgba(16, 24, 40, 0.06)',
    shadowInputFocus: '0 0 0 3px rgba(255, 54, 33, 0.14)',
    shadowPopover: '0 8px 28px rgba(16, 24, 40, 0.12)',
  },
  dark: {
    bgPrimary: '#1B1F23',
    bgSecondary: '#232930',
    bgSidebar: '#15191E',
    bgInput: '#232930',
    bgUserMsg: '#2A3038',
    textPrimary: '#E8ECEF',
    textSecondary: '#A0AAB4',
    textMuted: '#6B7785',
    border: '#333C45',
    accent: '#FF3621',
    accentHover: '#FF5A47',
    bgRail: '#15191E',
    bgRailHover: 'rgba(255, 255, 255, 0.05)',
    bgActiveChip: 'rgba(255, 255, 255, 0.085)',
    shadowInput: '0 1px 2px rgba(0, 0, 0, 0.3), 0 4px 16px rgba(0, 0, 0, 0.35)',
    shadowInputFocus: '0 0 0 3px rgba(255, 54, 33, 0.22)',
    shadowPopover: '0 8px 28px rgba(0, 0, 0, 0.45)',
  },
} as const;

export function createChatTheme(mode: 'light' | 'dark'): Theme {
  const p = PALETTES[mode];
  return createTheme({
    palette: {
      mode,
      primary: { main: p.accent, dark: p.accentHover, contrastText: '#FFFFFF' },
      background: { default: p.bgPrimary, paper: p.bgInput },
      text: { primary: p.textPrimary, secondary: p.textSecondary, disabled: p.textMuted },
      divider: p.border,
    },
    typography: { fontFamily: FONT_STACK },
    shape: { borderRadius: 8 },
    chat: {
      bgPrimary: p.bgPrimary,
      bgSecondary: p.bgSecondary,
      bgSidebar: p.bgSidebar,
      bgInput: p.bgInput,
      bgUserMsg: p.bgUserMsg,
      textMuted: p.textMuted,
      accentHover: p.accentHover,
      bgRail: p.bgRail,
      bgRailHover: p.bgRailHover,
      bgActiveChip: p.bgActiveChip,
      shadowInput: p.shadowInput,
      shadowInputFocus: p.shadowInputFocus,
      shadowPopover: p.shadowPopover,
    },
  });
}
