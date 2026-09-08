/**
 * Catalog → Optimize Prompts entry point.
 *
 * Source-level guards, following CrewFeedback.test.tsx — the catalog dialog
 * is heavy to mount. These pin the wiring that must not regress: the per-crew
 * optimize action stops card-click propagation (clicking ✨ must never LOAD
 * the crew), and CrewOptimizeDialog is mounted with the selected crew's
 * id + name and a closer that clears the selection.
 */
import { describe, it, expect } from 'vitest';
import { readFileSync } from 'fs';
import { resolve } from 'path';

const dialogSrc = readFileSync(resolve(__dirname, 'CrewFlowDialog.tsx'), 'utf-8');
const actionsSrc = readFileSync(resolve(__dirname, 'CrewCatalogActions.tsx'), 'utf-8');

describe('catalog optimize entry', () => {
  it('has an Optimize Prompts action per crew card', () => {
    expect(actionsSrc).toContain('title="Optimize Prompts"');
    expect(actionsSrc).toContain('AutoFixHighIcon');
  });

  it('stops propagation so the click never loads the crew', () => {
    expect(actionsSrc).toContain('event.stopPropagation()');
    expect(actionsSrc).toContain('onClick={() => onOptimize()}');
    expect(dialogSrc).toContain('onOptimize={() => setOptimizeCrew(crew)}');
  });

  it('mounts CrewOptimizeDialog wired to the selected crew', () => {
    expect(dialogSrc).toContain('open={optimizeCrew !== null}');
    expect(dialogSrc).toContain('crewId={optimizeCrew?.id ?? null}');
    expect(dialogSrc).toContain('crewName={optimizeCrew?.name}');
    expect(dialogSrc).toContain('onClose={() => setOptimizeCrew(null)}');
  });
});
