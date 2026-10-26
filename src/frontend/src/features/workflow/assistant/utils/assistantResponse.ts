import type { DispatchResult, CatalogListResult, FlowListResult } from '../../../../api/execution/DispatcherService';
import type { GeneratedAgent, GeneratedTask, GeneratedCrew } from '../types';

export const getAssistantResponse = (result: DispatchResult): string => {
  const { dispatcher, generation_result } = result;

  if (dispatcher.intent === 'unknown') {
    return "I'm not sure what you want to create. Please specify if you want to create an agent, a task, or a complete crew.";
  }

  if (!generation_result) {
    return "I understood your request but couldn't generate the result. Please try again.";
  }

  switch (dispatcher.intent) {
    case 'generate_agent': {
      const agent = generation_result as GeneratedAgent;
      return `I've created an agent: **${agent.name}** (${agent.role})\n- Goal: ${agent.goal}\n- Backstory: ${agent.backstory}`;
    }
    case 'generate_task': {
      const task = generation_result as GeneratedTask;
      return `I've created a task: **${task.name}**\n- Description: ${task.description}\n- Expected Output: ${task.expected_output}`;
    }
    case 'generate_crew': {
      const crew = generation_result as GeneratedCrew;
      let response = "I've created a crew with:\n";

      if (crew.agents && crew.agents.length > 0) {
        response += "\n**Agents & Tasks:**\n";
        crew.agents.forEach((agent, index) => {
          response += `${index + 1}. **${agent.name}** (${agent.role}) - ${agent.goal}\n`;

          const agentTasks = crew.tasks?.filter((task) =>
            task.agent_id === agent.id || task.agent_id?.toString() === agent.id?.toString()
          ) || [];

          if (agentTasks.length > 0) {
            agentTasks.forEach((task) => {
              response += `   → ${task.name}: ${task.description}\n`;
            });
          }
        });

        const unassignedTasks = crew.tasks?.filter((task) => !task.agent_id) || [];
        if (unassignedTasks.length > 0) {
          response += "\n**Unassigned Tasks:**\n";
          unassignedTasks.forEach((task, index) => {
            response += `${index + 1}. **${task.name}** - ${task.description}\n`;
          });
        }
      }

      response += "\nClick the **▶ Play** control on the canvas to run the crew.";
      return response;
    }
    case 'generate_plan': {
      const crew = generation_result as GeneratedCrew;
      let response = "I've created a plan with:\n";

      if (crew.agents && crew.agents.length > 0) {
        response += "\n**Agents & Tasks:**\n";
        crew.agents.forEach((agent, index) => {
          response += `${index + 1}. **${agent.name}** (${agent.role}) - ${agent.goal}\n`;

          const agentTasks = crew.tasks?.filter((task) =>
            task.agent_id === agent.id || task.agent_id?.toString() === agent.id?.toString()
          ) || [];

          if (agentTasks.length > 0) {
            agentTasks.forEach((task) => {
              response += `   → ${task.name}: ${task.description}\n`;
            });
          }
        });

        const unassignedTasks = crew.tasks?.filter((task) => !task.agent_id) || [];
        if (unassignedTasks.length > 0) {
          response += "\n**Unassigned Tasks:**\n";
          unassignedTasks.forEach((task, index) => {
            response += `${index + 1}. **${task.name}** - ${task.description}\n`;
          });
        }
      }

      response += "\nClick the **▶ Play** control on the canvas to run the crew.";
      return response;
    }
    case 'catalog_list': {
      const listResult = generation_result as CatalogListResult;
      let msg = listResult.message + '\n';
      if (listResult.plans?.length > 0) {
        listResult.plans.forEach((p, i) => {
          msg += `${i + 1}. **${p.name}** — ${p.agent_count || 0} agents, ${p.task_count || 0} tasks — \`/load crew ${p.name}\` \`/run crew ${p.name}\`\n`;
        });
      }
      return msg;
    }
    case 'catalog_load': {
      // When multiple matches or no name given, backend returns type "catalog_list" with plans array
      const genResult = generation_result as Record<string, unknown>;
      if (genResult.type === 'catalog_list' && Array.isArray(genResult.plans)) {
        const plans = genResult.plans as Array<{ id: string; name: string; agent_count?: number; task_count?: number }>;
        let msg = (genResult.message as string) + '\n';
        plans.forEach((p, i) => {
          msg += `${i + 1}. **${p.name}**`;
          if (p.agent_count !== undefined || p.task_count !== undefined) {
            msg += ` — ${p.agent_count || 0} agents, ${p.task_count || 0} tasks`;
          }
          msg += ` — \`/load crew ${p.name}\` \`/run crew ${p.name}\`\n`;
        });
        return msg;
      }
      return (genResult.message as string) || 'Plan loaded.';
    }
    case 'catalog_save':
    case 'catalog_schedule':
    case 'catalog_help':
      return (generation_result as { message: string }).message;
    case 'flow_list': {
      const flowListResult = generation_result as FlowListResult;
      let flowListMsg = flowListResult.message + '\n';
      if (flowListResult.flows?.length > 0) {
        flowListResult.flows.forEach((f, i) => {
          flowListMsg += `${i + 1}. **${f.name}** — ${f.node_count || 0} crew nodes — \`/load flow ${f.name}\` \`/run flow ${f.name}\`\n`;
        });
      }
      return flowListMsg;
    }
    case 'flow_load': {
      const flowGenResult = generation_result as Record<string, unknown>;
      if (flowGenResult.type === 'flow_list' && Array.isArray(flowGenResult.flows)) {
        const flows = flowGenResult.flows as Array<{ id: string; name: string; node_count?: number }>;
        let flowMsg = (flowGenResult.message as string) + '\n';
        flows.forEach((f, i) => {
          flowMsg += `${i + 1}. **${f.name}**`;
          if (f.node_count !== undefined) flowMsg += ` — ${f.node_count} crew nodes`;
          flowMsg += ` — \`/load flow ${f.name}\` \`/run flow ${f.name}\`\n`;
        });
        return flowMsg;
      }
      return (flowGenResult.message as string) || 'Flow loaded.';
    }
    case 'flow_save':
      return (generation_result as { message: string }).message;
    case 'execute_crew':
    case 'execute_flow':
    case 'catalog_delete':
    case 'flow_delete':
      return (generation_result as { message: string }).message;
    default:
      return "Your request has been processed successfully.";
  }
};

