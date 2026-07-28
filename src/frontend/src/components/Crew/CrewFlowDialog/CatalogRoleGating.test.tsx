/**
 * Catalog cards must not offer authoring actions to an operator.
 *
 * An operator can run what is in the catalog but not change it — the backend
 * rejects optimize / publish / edit / delete for that role — so a visible
 * button is a button whose only possible outcome is a 403.
 *
 * Source-level guards, following CrewOptimizeEntry.test.tsx: the catalog
 * dialogs pull in the crew, flow, agent and task services on mount, so these
 * pin the wiring rather than re-mounting the whole dialog.
 */
import { describe, it, expect } from 'vitest';
import { readFileSync } from 'fs';
import { resolve } from 'path';

const catalogSrc = readFileSync(resolve(__dirname, 'CrewFlowDialog.tsx'), 'utf-8');
const flowDialogSrc = readFileSync(
  resolve(__dirname, '../../Flow/FlowDialog.tsx'),
  'utf-8',
);

/** The guard, if any, immediately preceding an action in the source. */
const guardBefore = (src: string, marker: string): string => {
  const at = src.indexOf(marker);
  expect(at, `missing action: ${marker}`).toBeGreaterThan(-1);
  return src.slice(Math.max(0, at - 200), at);
};

describe('crew/flow catalog role gating', () => {
  it('reads the role from the permission store', () => {
    expect(catalogSrc).toContain('const { canEdit, canDelete } = usePermissions();');
    expect(flowDialogSrc).toContain(
      'const { canEdit, canDelete } = usePermissions();',
    );
  });

  it.each([
    ['title="Optimize Prompts"', 'canEdit'],
    ['entityType="crew"', 'canEdit'],
    ['title="Delete Crew"', 'canDelete'],
    ['title="Edit Flow"', 'canEdit'],
    ['entityType="flow"', 'canEdit'],
    ['title="Delete Flow"', 'canDelete'],
  ])('gates %s behind %s', (marker, permission) => {
    expect(guardBefore(catalogSrc, marker)).toContain(`{${permission} && (`);
  });

  it.each([
    ['entityType="flow"', 'canEdit'],
    ['title="Delete Flow"', 'canDelete'],
  ])('gates %s behind %s in the standalone flow dialog', (marker, permission) => {
    expect(guardBefore(flowDialogSrc, marker)).toContain(`{${permission} && (`);
  });

  it('leaves export ungated — reading a definition is not authoring', () => {
    expect(guardBefore(catalogSrc, 'title="Export Crew"')).not.toContain('canEdit && (');
    expect(guardBefore(flowDialogSrc, 'title="Export Flow"')).not.toContain(
      'canDelete && (',
    );
  });
});
