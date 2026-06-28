/**
 * Tailwind is used ONLY by the embedded Chat workspace (src/components/ChatMode)
 * and the shared A2UI renderer it hosts (src/shared/a2ui — the SAME renderer the
 * exported Databricks app bundles).
 *
 * To avoid clobbering the Material-UI styling used everywhere else in Kasal:
 *  - `corePlugins.preflight` is disabled, so Tailwind injects no global CSS reset.
 *  - `important` scopes every generated utility under `.kasal-chat-root`, so the
 *    utilities only take effect inside a chat container and never leak into MUI.
 *    It's a CLASS (not an id) so the same chat-mode UI can be reused in more than
 *    one place at once — e.g. the chat workspace AND the Jobs "Show result" dialog
 *    (the chat workspace keeps the `kasal-chat-root` id too, for appStore theming).
 *  - `content` only scans the ChatMode tree + the shared renderer.
 *
 * Design tokens for the A2UI renderer (the shadcn-style `bg-card`, `text-muted-
 * foreground`, `ring-ring`, … utilities its components use) are the canonical
 * shadcn token set, mapped here to CSS variables defined in ChatMode/chat.css.
 * The variables are PREFIXED `--a2-*` deliberately: the chat already defines
 * `--accent` (Kasal red) with a different meaning than shadcn's `--accent`
 * (a muted hover background), so an un-prefixed token set would collide. The
 * `<alpha-value>` form keeps opacity modifiers (e.g. `bg-primary/90`) working.
 */
const a2 = (name) => `hsl(var(--a2-${name}) / <alpha-value>)`;

export default {
  content: [
    './src/components/ChatMode/**/*.{js,ts,jsx,tsx}',
    './src/shared/a2ui/**/*.{js,ts,jsx,tsx}',
  ],
  darkMode: ['class', '[data-theme="dark"]'],
  important: '.kasal-chat-root',
  corePlugins: {
    preflight: false,
  },
  theme: {
    extend: {
      colors: {
        border: a2('border'),
        input: a2('input'),
        ring: a2('ring'),
        background: a2('background'),
        foreground: a2('foreground'),
        primary: {
          DEFAULT: a2('primary'),
          foreground: a2('primary-foreground'),
        },
        secondary: {
          DEFAULT: a2('secondary'),
          foreground: a2('secondary-foreground'),
        },
        destructive: {
          DEFAULT: a2('destructive'),
          foreground: a2('destructive-foreground'),
        },
        muted: {
          DEFAULT: a2('muted'),
          foreground: a2('muted-foreground'),
        },
        accent: {
          DEFAULT: a2('accent'),
          foreground: a2('accent-foreground'),
        },
        popover: {
          DEFAULT: a2('popover'),
          foreground: a2('popover-foreground'),
        },
        card: {
          DEFAULT: a2('card'),
          foreground: a2('card-foreground'),
        },
      },
      borderRadius: {
        lg: 'var(--a2-radius)',
        md: 'calc(var(--a2-radius) - 2px)',
        sm: 'calc(var(--a2-radius) - 4px)',
      },
    },
  },
  plugins: [],
};
