import type { Agent } from '../../../../types/workflow/agent';

/**
 * Add a skill to a saved agent, by name. Runs read skills from the SAVED agent
 * (JobExecutionService), so this is what makes an attachment take effect —
 * the canvas node is not consulted. Returns the saved agent, or null.
 *
 * AgentService and the agent store load on first use: AgentService builds a
 * ModelService at import, a side effect the task form (which renders this via
 * the UCMV config) should not inherit.
 */
export async function attachSkillToAgent(agentId: string | number, skillName: string): Promise<Agent | null> {
  const { AgentService } = await import('../../../../api/workflow/AgentService');
  const current = await AgentService.getAgent(agentId);
  if (!current) return null;
  const skills = current.skills || [];
  if (skills.includes(skillName)) return current;
  const saved = await AgentService.updateAgentFull(agentId, {
    ...current,
    skills: [...skills, skillName],
  } as Omit<Agent, 'id' | 'created_at'>);
  if (saved?.id) {
    const { useAgentStore } = await import('../../../../store/agent');
    useAgentStore.getState().updateAgent(String(saved.id), saved);
  }
  return saved;
}
