import { describe, it, expect } from 'vitest';
import { extractPlanItems } from './TracePlanView';

const RENDERED = `Plan (1/5 completed):
[x] 1. Create the swiss_tech_companies table in swiss_company_db
[>] 2. Research and gather 300 Swiss tech companies
[ ] 3. Insert companies into the database table
[ ] 4. Create max one agent and one task
[ ] 5. Verify data and report summary`;

describe('a plan is recognised in every shape a run writes it', () => {
  it('reads the tool\'s own rendering — the only copy some rows carry', () => {
    // This is what the `todo` tool RETURNS. Unparsed, the step's content showed
    // the bracket markers themselves instead of a checklist.
    const items = extractPlanItems(RENDERED);
    expect(items).toHaveLength(5);
    expect(items?.[0]).toMatchObject({ content: expect.stringContaining('Create the swiss_tech_companies'), status: 'completed' });
    expect(items?.[1].status).toBe('in_progress');
    expect(items?.[2].status).toBe('pending');
  });

  it('reads it out of a result envelope', () => {
    expect(extractPlanItems({ content: RENDERED })).toHaveLength(5);
  });

  it('reads the light-agent path\'s `input` arguments', () => {
    const input = JSON.stringify({ todos: [{ id: '1', content: 'Do it', status: 'in_progress' }] });
    expect(extractPlanItems({ tool_name: 'todo', input })).toEqual([
      { id: '1', content: 'Do it', status: 'in_progress' },
    ]);
  });

  it('still reads the crew path\'s tool_args', () => {
    const args = JSON.stringify({ todos: [{ id: '1', content: 'Do it', status: 'completed' }] });
    expect(extractPlanItems({ extra_data: { tool_args: args } })).toHaveLength(1);
  });

  it('does not mistake prose with a bracket for a plan', () => {
    expect(extractPlanItems('The result [see above] was fine.')).toBeNull();
    expect(extractPlanItems('just a sentence')).toBeNull();
  });
});
