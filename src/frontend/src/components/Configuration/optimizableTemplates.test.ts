/**
 * The optimizable-templates list must mirror the backend's TEMPLATE_TASKS
 * registry — the six wired templates, no duplicates, labels for the picker.
 */

import { describe, it, expect } from 'vitest';
import { OPTIMIZABLE_TEMPLATES } from './optimizableTemplates';

describe('OPTIMIZABLE_TEMPLATES', () => {
  it('lists exactly the six wired templates', () => {
    expect(OPTIMIZABLE_TEMPLATES.map((t) => t.name)).toEqual([
      'detect_intent',
      'generate_agent',
      'generate_task',
      'generate_crew',
      'generate_crew_plan',
      'generate_job_name',
    ]);
  });

  it('has unique names and a human label for each', () => {
    const names = OPTIMIZABLE_TEMPLATES.map((t) => t.name);
    expect(new Set(names).size).toBe(names.length);
    for (const template of OPTIMIZABLE_TEMPLATES) {
      expect(template.label).toContain(template.name);
    }
  });
});
