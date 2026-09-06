import { readFileSync } from 'fs';
import { join } from 'path';
import { describe, it, expect } from 'vitest';

// Guards the specificity contract between chat.css and Tailwind. The config sets
// `important: '.kasal-chat-root'`, so every utility is generated as
// `.kasal-chat-root .text-xs` (specificity 0,2,0). If the button font reset were
// scoped to the #id (`#kasal-chat-root button`, specificity 1,0,1) it would
// outrank ALL text utilities and pin every pill/chip button to font-size:100% —
// the exact bug that made `text-xs` / `text-[11px]` on the answer-mode, memory,
// model, Activity, Customize, Save-to-catalog and Memory-graph buttons do
// nothing. The font normalization for buttons MUST stay class-scoped so per-
// button `text-*` classes win.
describe('chat.css — button font-size must be overridable by Tailwind text utilities', () => {
  const css = readFileSync(join(__dirname, 'chat.css'), 'utf8');

  it('normalizes button font-size via the .kasal-chat-root CLASS selector', () => {
    // A `.kasal-chat-root button { ... font-size: 100% ... }` block must exist.
    const classButtonBlock = css.match(/\.kasal-chat-root button\s*\{[^}]*\}/);
    expect(classButtonBlock, 'expected a `.kasal-chat-root button { … }` rule').not.toBeNull();
    expect(classButtonBlock![0]).toMatch(/font-size:\s*100%/);
  });

  it('does NOT set button font-size from an #id selector (would beat utilities)', () => {
    // Any `#kasal-chat-root button { … }` block must not declare font-size, or its
    // (1,0,1) specificity defeats the class-scoped (0,2,0) text utilities.
    const idButtonBlocks = css.match(/#kasal-chat-root button[^{]*\{[^}]*\}/g) ?? [];
    for (const block of idButtonBlocks) {
      expect(block, `#id button rule must not pin font-size:\n${block}`).not.toMatch(
        /font-size/,
      );
    }
  });
});

// The chat-TRANSCRIPT markdown prose overrides (color: var(--text-primary), …) are
// scoped to #kasal-chat-root and therefore leak into any A2UI surface rendered in
// chat, pinning its themed body/heading text to the chat page color. On a dark deck
// theme with a light chat page that paints the slide text dark-on-dark (invisible).
// An isolation rule must reset prose color to `inherit` inside .kasal-a2ui so the
// surface's own deck theme / --tw-prose-* / --a2-* colors win. Guards a CSS-cascade
// bug that jsdom cannot exercise.
describe('chat.css — A2UI surfaces must be isolated from transcript prose colors', () => {
  const css = readFileSync(join(__dirname, 'chat.css'), 'utf8');
  const normalized = css.replace(/\s+/g, ' ');

  it('resets prose text to inherit inside .kasal-a2ui surfaces', () => {
    expect(normalized).toMatch(/#kasal-chat-root \.kasal-a2ui \.prose/);
    const block = normalized.match(/#kasal-chat-root \.kasal-a2ui \.prose[^{]*\{([^}]*)\}/);
    expect(block?.[1] ?? '').toContain('color: inherit');
  });

  it('sanity: the leaking transcript prose color override still exists (what we isolate)', () => {
    expect(normalized).toMatch(/#kasal-chat-root \.prose p \{[^}]*var\(--text-primary\)/);
  });
});
