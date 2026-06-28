/**
 * Shared `sx` style atoms for the chat shell — the MUI replacements for a few
 * cross-cutting chat.css rules that individual components relied on implicitly.
 *
 * A component is "migrated off chat.css" when it styles itself through the chat
 * MUI theme (see {@link ./chatTheme}) + these atoms instead of Tailwind
 * utilities / `var(--…)`. These atoms make a migrated component self-sufficient:
 * it looks correct even once chat.css / Tailwind are removed entirely.
 */
import type { Theme } from '@mui/material/styles';
import type { SystemStyleObject } from '@mui/system';

/**
 * Native `<button>` reset. chat.css applied this to every `.kasal-chat-root
 * button` (preflight is disabled globally to protect MUI, so raw buttons would
 * otherwise keep the user-agent beveled border + gray background). Spread this
 * into a button's `sx` and add only the button's own background/radius/padding.
 */
export const buttonResetSx: SystemStyleObject<Theme> = {
  m: 0,
  p: 0,
  border: 0,
  backgroundColor: 'transparent',
  backgroundImage: 'none',
  color: 'inherit',
  fontFamily: 'inherit',
  fontSize: '100%',
  lineHeight: 'inherit',
  letterSpacing: 'inherit',
  textAlign: 'inherit',
  textTransform: 'none',
  WebkitAppearance: 'button',
  appearance: 'button',
  cursor: 'pointer',
  '&:disabled': { cursor: 'default' },
};

/**
 * Native `<input>` reset. chat.css re-applied these preflight bits to form
 * controls (preflight is disabled globally). Spread into an input's `sx` and add
 * its own background/color/border/font-size. (textarea needs `border: 0` +
 * `p: 0` on top of this — add those at the call site.)
 */
export const inputResetSx: SystemStyleObject<Theme> = {
  m: 0,
  fontFamily: 'inherit',
  fontSize: '100%',
  lineHeight: 'inherit',
  letterSpacing: 'inherit',
  color: 'inherit',
  backgroundColor: 'transparent',
  outline: 'none',
  WebkitAppearance: 'none',
  appearance: 'none',
};

/**
 * Markdown ("prose") styling. chat.css styled `.prose` descendants directly
 * (the Tailwind Typography plugin is NOT installed), so this mirrors those rules
 * one-for-one. Apply to the wrapper that contains a ReactMarkdown render. List
 * bullets / default block margins come from the browser UA styles, exactly as
 * before (no global reset is applied in the chat subtree).
 */
export const chatMarkdownSx: SystemStyleObject<Theme> = {
  '& p': { color: 'text.primary', lineHeight: 1.65 },
  '& a': {
    color: 'primary.main',
    textDecoration: 'none',
    '&:hover': { textDecoration: 'underline' },
  },
  '& strong': { color: 'text.primary', fontWeight: 600 },
  '& h1, & h2, & h3, & h4': { color: 'text.primary', fontWeight: 600 },
  '& ul, & ol': { color: 'text.primary' },
  '& table': { fontSize: 13 },
  '& th': { color: 'text.secondary', fontWeight: 600, textAlign: 'left' },
  '& td': { color: 'text.primary' },
};

/**
 * Entrance animation for inline notices (mirrors chat.css `.animate-slide-up`
 * → `kasalChatSlideUp`). Spread into the notice's `sx`.
 */
export const slideUpSx: SystemStyleObject<Theme> = {
  animation: 'kasalChatSlideUp 0.2s ease-out',
  '@keyframes kasalChatSlideUp': {
    from: { opacity: 0, transform: 'translateY(8px)' },
    to: { opacity: 1, transform: 'translateY(0)' },
  },
};

/**
 * Pulsing-opacity animation (mirrors Tailwind `animate-pulse`). Spread into a
 * `<Box>`'s `sx`.
 */
export const pulseSx: SystemStyleObject<Theme> = {
  animation: 'kasalPulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite',
  '@keyframes kasalPulse': { '0%, 100%': { opacity: 1 }, '50%': { opacity: 0.5 } },
};

/**
 * Entrance fade (mirrors chat.css `.animate-fade-in` → `kasalChatFadeIn`).
 * Spread into a `<Box>`'s `sx`.
 */
export const fadeInSx: SystemStyleObject<Theme> = {
  animation: 'kasalChatFadeIn 0.2s ease-out',
  '@keyframes kasalChatFadeIn': {
    from: { opacity: 0, transform: 'translateY(4px)' },
    to: { opacity: 1, transform: 'translateY(0)' },
  },
};

/**
 * Expanding "ping" ring for live indicators (mirrors Tailwind `animate-ping`).
 * Spread into the absolutely-positioned ring element; set its base
 * background/opacity at the call site.
 */
export const pingSx: SystemStyleObject<Theme> = {
  animation: 'kasalPing 1s cubic-bezier(0, 0, 0.2, 1) infinite',
  '@keyframes kasalPing': {
    '75%, 100%': { transform: 'scale(2)', opacity: 0 },
  },
};

/**
 * 12px circular spinner used in the card "Execute" buttons (mirrors the
 * Tailwind `animate-spin` border-spinner). Spread into a `<Box>`'s `sx`.
 */
export const spinnerSx: SystemStyleObject<Theme> = {
  width: 12,
  height: 12,
  borderRadius: '50%',
  border: '2px solid #fff',
  borderTopColor: 'transparent',
  animation: 'kasalSpin 1s linear infinite',
  '@keyframes kasalSpin': { to: { transform: 'rotate(360deg)' } },
};
