import { Node } from 'reactflow';

export const hasCrewContent = (nodes: Node[]) => {
  const hasAgents = nodes.some(node => node.type === 'agentNode');
  const hasTask = nodes.some(node => node.type === 'taskNode');
  return hasAgents && hasTask;
};

export const isExecuteCommand = (message: string) => {
  const trimmed = message.trim().toLowerCase();
  return trimmed === 'execute crew' || trimmed === 'ec' || trimmed === 'run' || trimmed === 'execute' || trimmed.startsWith('ec ') || trimmed.startsWith('execute crew ');
};

export const extractJobIdFromCommand = (message: string): string | null => {
  const trimmed = message.trim().toLowerCase();
  if (trimmed.startsWith('ec ')) {
    return message.trim().substring(3).trim();
  }
  if (trimmed.startsWith('execute crew ')) {
    return message.trim().substring(13).trim();
  }
  return null;
};