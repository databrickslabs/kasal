import type { Node } from 'reactflow';
import { useWorkflowStore } from '../../../../store/workflow';
import { TaskService } from '../../../../api/workflow/TaskService';
import type { Agent } from '../../../../api/workflow/AgentService';

/**
 * Wire knowledge files uploaded from the canvas composer into the crew.
 *
 * KnowledgeFileUpload runs its whole per-file loop with the callbacks of the
 * render in which the files were picked, and each upload can take a while to
 * embed. So these read the canvas from the store at the moment each file
 * lands, and patch only the fields the upload changes. Working from the
 * pick-time nodes kept only the last file searchable (each file rebuilt
 * file_paths from the same snapshot) and reverted any canvas edit made while
 * a file was embedding.
 */

const KNOWLEDGE_TOOL = 'DatabricksKnowledgeSearchTool';

type ToolConfigs = Record<string, Record<string, unknown> | undefined>;
interface TaskToolPatch { tools: string[]; tool_configs: ToolConfigs }

const hasKnowledgeTool = (tools: string[]) => tools.includes(KNOWLEDGE_TOOL) || tools.includes('36');

/** The task's tools and tool_configs with `path` added to its knowledge search. */
function withKnowledgeFile(data: Node['data'], path: string, agentId: unknown): TaskToolPatch {
  const tools: string[] = data.tools || [];
  const toolConfigs: ToolConfigs = data.tool_configs || {};
  const knowledge = toolConfigs[KNOWLEDGE_TOOL] || {};
  const filePaths = (knowledge.file_paths as string[] | undefined) || [];
  return {
    tools: hasKnowledgeTool(tools) ? tools : [...tools, KNOWLEDGE_TOOL],
    tool_configs: {
      ...toolConfigs,
      [KNOWLEDGE_TOOL]: {
        ...knowledge,
        file_paths: filePaths.includes(path) ? filePaths : [...filePaths, path],
        agent_id: agentId, // access control filtering
      },
    },
  };
}

// One write at a time per task, in order: a later file's list must land last.
const taskWrites = new Map<string, Promise<unknown>>();

function persistTaskTools(taskId: string, patch: TaskToolPatch): void {
  const previous = taskWrites.get(taskId) ?? Promise.resolve();
  const write = previous
    .then(() => TaskService.updateTask(taskId, patch))
    .catch(err => console.error(`Failed to update task ${taskId}:`, err));
  taskWrites.set(taskId, write);
  void write.finally(() => { if (taskWrites.get(taskId) === write) taskWrites.delete(taskId); });
}

/** The task searches the file at run time: add its path to every task on the canvas. */
export function wireKnowledgeFileIntoTasks(uploadedFilePath: string): void {
  const { nodes, setNodes } = useWorkflowStore.getState();
  const agentNode = nodes.find(n => n.type === 'agentNode');
  const agentId = agentNode?.data?.agentId || agentNode?.id;

  const patches = new Map<string, TaskToolPatch>();
  for (const node of nodes) {
    if (node.type !== 'taskNode') continue;
    const patch = withKnowledgeFile(node.data, uploadedFilePath, agentId);
    patches.set(node.id, patch);
    if (node.data.taskId) persistTaskTools(node.data.taskId, patch);
  }
  setNodes(current => current.map(node => {
    const patch = patches.get(node.id);
    return patch ? { ...node, data: { ...node.data, ...patch } } : node;
  }));
}

const isAgentOfNode = (agent: Agent, node: Node) =>
  agent.id === node.data.agentId ||
  agent.id === node.data.id ||
  (!!agent.id && `agent-${agent.id}` === node.id) ||
  `agent-${agent.name}` === node.id;

/** Show the recorded knowledge sources on the canvas agents, and the search tool on tasks. */
export function showUploadedAgentsOnCanvas(updatedAgents: Agent[]): void {
  const hasKnowledgeSources = updatedAgents.some(agent => (agent.knowledge_sources?.length ?? 0) > 0);
  useWorkflowStore.getState().setNodes(current => current.map(node => {
    if (node.type === 'agentNode') {
      const agent = updatedAgents.find(candidate => isAgentOfNode(candidate, node));
      return agent
        ? { ...node, data: { ...node.data, agentId: agent.id, knowledge_sources: agent.knowledge_sources } }
        : node;
    }
    const tools: string[] = node.data?.tools || [];
    if (node.type === 'taskNode' && hasKnowledgeSources && !hasKnowledgeTool(tools)) {
      return { ...node, data: { ...node.data, tools: [...tools, KNOWLEDGE_TOOL] } };
    }
    return node;
  }));
}
