/**
 * Tailwind is used ONLY by the embedded Chat workspace (src/components/ChatMode).
 *
 * To avoid clobbering the Material-UI styling used everywhere else in Kasal:
 *  - `corePlugins.preflight` is disabled, so Tailwind injects no global CSS reset.
 *  - `important` scopes every generated utility under `#kasal-chat-root`, so the
 *    utilities only take effect inside the chat container and never leak into MUI.
 *  - `content` only scans the ChatMode tree, keeping the generated CSS small.
 */
export default {
  content: ['./src/components/ChatMode/**/*.{js,ts,jsx,tsx}'],
  darkMode: ['class', '[data-theme="dark"]'],
  important: '#kasal-chat-root',
  corePlugins: {
    preflight: false,
  },
  theme: {
    extend: {},
  },
  plugins: [],
};
