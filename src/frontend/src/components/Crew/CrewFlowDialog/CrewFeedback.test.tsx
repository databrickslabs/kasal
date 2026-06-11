/**
 * Catalog crew feedback: SVG thumb icons (matching the chat actions bar) and
 * the click-isolation guard so expanding/reading feedback never loads the crew.
 *
 * Source-level guards — the dialog is heavy to mount; these pin the two
 * regressions (emoji → SVG, and the feedback region swallowing card clicks).
 */
import { describe, it, expect } from 'vitest';
import { readFileSync } from 'fs';
import { resolve } from 'path';

const dialogSrc = readFileSync(resolve(__dirname, 'CrewFlowDialog.tsx'), 'utf-8');
const chatBarSrc = readFileSync(
  resolve(__dirname, '../../ChatMode/components/Cards/CrewActionsBar.tsx'),
  'utf-8',
);

describe('catalog crew feedback', () => {
  it('uses SVG thumb icons, not emoji', () => {
    expect(dialogSrc).toContain('const ThumbUp');
    expect(dialogSrc).toContain('const ThumbDown');
    expect(dialogSrc).not.toContain('👍');
    expect(dialogSrc).not.toContain('👎');
  });

  it('reuses the SAME thumb paths the chat actions bar submits with', () => {
    // The exact Heroicons outline path — identical control in both places.
    const upPath = 'M6.633 10.5c.806 0 1.533-.446 2.031-1.08';
    const downPath = 'M7.498 15.25H4.372c-1.026 0-1.945-.694-2.054-1.715';
    expect(chatBarSrc).toContain(upPath);
    expect(dialogSrc).toContain(upPath);
    expect(chatBarSrc).toContain(downPath);
    expect(dialogSrc).toContain(downPath);
  });

  it('isolates the feedback region from the card load-crew click', () => {
    // The whole feedback wrapper stops propagation, so expanding/reading the
    // comments never triggers handleCrewSelect (which closes the dialog).
    expect(dialogSrc).toContain('onClick={(e) => e.stopPropagation()}');
    expect(dialogSrc).toMatch(/toggleFeedback[\s\S]*?e\.stopPropagation\(\)/);
  });
});
