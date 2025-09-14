/**
 * Utility functions for cleaning up knowledge sources when agents are removed
 */

export class KnowledgeSourceCleanup {
  /**
   * Remove all knowledge sources associated with a specific execution from an agent
   */
  static async removeExecutionKnowledgeSources(agentId: string, executionId: string): Promise<boolean> {
    try {
      // Dynamic import to avoid circular dependencies
      const { AgentService } = await import('../api/AgentService');

      // Get the current agent data
      const agent = await AgentService.getAgent(agentId);
      if (!agent || !agent.knowledge_sources) {
        return true; // Nothing to clean up
      }

      // Filter out knowledge sources from the specified execution
      const updatedKnowledgeSources = agent.knowledge_sources.filter(
        (source: any) => source.metadata?.execution_id !== executionId
      );

      // Update the agent with cleaned knowledge sources
      const updatedAgent = {
        ...agent,
        knowledge_sources: updatedKnowledgeSources
      };

      // Remove id and created_at for update
      const { id, created_at, ...agentData } = updatedAgent as any;

      // Update the agent in the backend
      await AgentService.updateAgentFull(agentId, agentData as any);

      console.log(`[DEBUG] Successfully removed execution ${executionId} knowledge sources from agent ${agent.name}`);
      return true;
    } catch (err) {
      console.error(`Failed to remove execution ${executionId} knowledge sources from agent ${agentId}:`, err);
      return false;
    }
  }

  /**
   * Remove all knowledge sources from an agent (used when agent is deleted)
   */
  static async removeAllKnowledgeSources(agentId: string): Promise<boolean> {
    try {
      // Dynamic import to avoid circular dependencies
      const { AgentService } = await import('../api/AgentService');

      // Get the current agent data
      const agent = await AgentService.getAgent(agentId);
      if (!agent) {
        return true; // Agent doesn't exist, nothing to clean up
      }

      console.log(`[DEBUG] Cleaning up all knowledge sources for deleted agent ${agent.name}`);

      // Note: Since the agent is being deleted, we don't need to update it
      // This function is mainly for logging and potential future cleanup tasks
      // The actual cleanup happens when the agent record is deleted from the database

      return true;
    } catch (err) {
      console.error(`Failed to clean up knowledge sources for deleted agent ${agentId}:`, err);
      return false;
    }
  }

  /**
   * Notify other components about agent knowledge source changes
   */
  static notifyAgentUpdate(updatedAgent: any, onAgentsUpdated?: (agents: any[]) => void) {
    if (onAgentsUpdated) {
      onAgentsUpdated([updatedAgent]);
    }
  }
}