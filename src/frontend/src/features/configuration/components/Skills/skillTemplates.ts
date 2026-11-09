import type { SkillInput } from '../../../../api/tools/SkillService';

/**
 * The metadata tag that makes a skill feed a TOOL parameter instead of only the
 * agent's prompt, and the UCMV domain-context shape built on it.
 *
 * A skill tagged `kasal-tool-param: domain_context` and attached to an agent is
 * handed to that agent's UC Metric View Generator (and Drift Monitor) as its
 * `domain_context` — see backend `execution/kernel/skill_tool_context.py`.
 */

export const TOOL_PARAM_KEY = 'kasal-tool-param';
export const DOMAIN_CONTEXT_PARAM = 'domain_context';

/** The tool parameter a skill feeds, if it is tagged for one. */
export function toolParamOf(metadata: Record<string, unknown> | undefined | null): string | undefined {
  const value = metadata?.[TOOL_PARAM_KEY];
  return typeof value === 'string' && value ? value : undefined;
}

export function isDomainContextSkill(skill: { metadata?: Record<string, unknown> | null }): boolean {
  return toolParamOf(skill.metadata) === DOMAIN_CONTEXT_PARAM;
}

/** Shown greyed in the empty field — the shape to write, not a value. */
export const DOMAIN_CONTEXT_EXAMPLE = `# Domain context — <your model / report name>

Fiscal calendar: describe it (e.g. 4-4-5). Define how "YTD <year>" is computed
(e.g. periods 001..latest closed month) and any period-column overrides
(e.g. latest_month_label → IsCurrentMonth).

Vocabulary / acronyms (one per line):
- <ACRONYM> = <what it means in this model>
- <ACRONYM> = <what it means in this model>

Measure / naming conventions:
- Scenario suffixes, e.g. "… PY" = prior year, "… Bud" = budget, "… Act" = actual.
- Units, e.g. volumes are counts unless suffixed _hl (hectolitres).
- Business rules, e.g. prefer booked/stored columns over recomputing from components.

Key dimensions and their members:
- <dimension>: '<member1>','<member2>', …
- Regions / entities: '<region1>','<region2>', …`;

/** A skill name from a model name: lowercase, hyphenated, ≤64 chars. */
export function domainContextSkillName(model: string): string {
  const slug = model
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 47)
    .replace(/-+$/g, '');
  return slug ? `${slug}-domain-context` : 'ucmv-domain-context';
}

export function domainContextDescription(model: string): string {
  const subject = model.trim() ? `the ${model.trim()} Power BI model` : 'a Power BI model';
  return (
    `Business vocabulary, fiscal calendar and naming conventions of ${subject}. ` +
    'Fed to the UC Metric View Generator so DAX→SQL translation uses the model’s semantics.'
  );
}

export function buildDomainContextSkill(name: string, description: string, body: string): SkillInput {
  return {
    name,
    description,
    body,
    metadata: { [TOOL_PARAM_KEY]: DOMAIN_CONTEXT_PARAM },
    enabled: true,
    global_enabled: false,
  };
}
