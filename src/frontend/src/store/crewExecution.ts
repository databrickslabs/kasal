import { create } from 'zustand';
import { Node, Edge } from 'reactflow';
import { jobExecutionService } from '../api/execution/JobExecutionService';
import { useWorkflowStore } from './workflow';
import { useTabManagerStore } from './tabManager';
import { useFlowExecutionStore } from './flowExecutionStore';
import { Tool } from '../types/workflow/tool';
import { assessTrifecta, TrifectaAssessment } from '../utils/toolCapabilityManifest';
import { ToolService } from '../api/tools/ToolService';
import { placeholdersInFlowNodes } from '../utils/flowInputs';

interface RunHistoryItem {
  id: string;
  jobId: string;
  status: string;
  createdAt: string;
  updatedAt: string;
  error?: string;
}

// Reasoning controls (the model's native thinking budget). Defined in types/crews
// (a leaf module) and re-exported here so existing imports keep working without an
// import cycle (tabManager <-> crewExecution). Sent to the backend as
// `reasoning_config` only when reasoning is enabled.
export type { ReasoningConfig } from '../types/workflow/crews';
import type { ReasoningConfig } from '../types/workflow/crews';

type ToolConfigs = Record<string, unknown>;

/**
 * Merge the canvas node's ``tool_configs`` with the DB row's before a run.
 *
 * The pre-execution refresh below re-reads every agent/task from the database and
 * spreads it over the node's data. A plain spread makes the DB authoritative,
 * which silently DISCARDS anything configured on the canvas that never made it to
 * the database — most visibly MCP servers (``tool_configs.MCP_SERVERS``), whose
 * chips stay on the node while the run receives none, so the crew executes
 * tool-less with no error anywhere.
 *
 * Union instead: neither side loses an entry, and on conflict the canvas wins
 * because that is what the user is looking at. The one case not covered is a
 * config the user removed on the canvas while the DB copy still has it — the
 * stale entry survives. That only diverges when a save failed; a successful save
 * updates both sides.
 */
export const mergeToolConfigs = (canvas: unknown, db: unknown): ToolConfigs | undefined => {
  const isPlainObject = (v: unknown): v is ToolConfigs =>
    typeof v === 'object' && v !== null && !Array.isArray(v);
  const canvasConfigs = isPlainObject(canvas) ? canvas : {};
  const dbConfigs = isPlainObject(db) ? db : {};
  const merged = { ...dbConfigs, ...canvasConfigs };
  // Keep the field absent rather than writing an empty object, so a node that
  // never had tool_configs stays that way.
  if (Object.keys(merged).length === 0) {
    return isPlainObject(canvas) || isPlainObject(db) ? merged : undefined;
  }
  return merged;
};

/**
 * Log the canvas/DB differences that the merge above papered over, so an
 * unsaved edit is visible instead of silently changing what the run does.
 * Covers tool_configs as well as tools — an MCP-only mismatch used to be
 * invisible because only `tools` was compared.
 */
const warnOnConfigMismatch = (
  kind: 'agent' | 'task',
  name: string,
  canvasData: Record<string, unknown>,
  dbData: Record<string, unknown>,
): void => {
  const normalizeTools = (v: unknown) =>
    JSON.stringify((Array.isArray(v) ? v : []).map(String).sort());
  if (normalizeTools(canvasData?.tools) !== normalizeTools(dbData?.tools)) {
    console.warn(
      `[CrewExecution] Tool mismatch for ${kind} "${name}" — canvas: [${canvasData?.tools}], ` +
      `DB: [${dbData?.tools}]. Using DB version. If unexpected, ensure you saved after editing tools.`
    );
  }
  const canvasConfigs = JSON.stringify(canvasData?.tool_configs ?? {});
  const dbConfigs = JSON.stringify(dbData?.tool_configs ?? {});
  if (canvasConfigs !== dbConfigs) {
    console.warn(
      `[CrewExecution] tool_configs mismatch for ${kind} "${name}" — canvas: ${canvasConfigs}, ` +
      `DB: ${dbConfigs}. Merging (canvas wins). A canvas-only entry means the save did not reach ` +
      `the database — MCP server selections live here.`
    );
  }
};

// Low by default: the smallest thinking budget is the fastest and cheapest, and
// models without a native reasoning budget ignore the setting entirely. Users can
// raise it per crew.
export const DEFAULT_REASONING_CONFIG: ReasoningConfig = {
  reasoning_effort: 'low',
};

interface CrewExecutionState {
  // Execution state
  isExecuting: boolean;
  selectedModel: string;
  /**
   * Which agent runtime runs the next job: 'kasal' | 'crewai', or '' for the
   * configured default. Sits beside the model because it is the other thing you
   * choose when starting a run, and is sent with every execution payload.
   */
  selectedHarness: string;
  reasoningEnabled: boolean;
  reasoningLLM: string;
  reasoningConfig: ReasoningConfig;
  schemaDetectionEnabled: boolean;
  processType: 'sequential' | 'hierarchical' | 'parallel';
  managerLLM: string;
  managerNodeId: string | null;  // ID of the manager node (if exists)
  isLoadingCrew: boolean;  // Flag to prevent manager removal during crew loading
  isCrewPlanningOpen: boolean;
  isScheduleDialogOpen: boolean;
  inputMode: 'dialog' | 'chat';
  tools: Tool[];
  selectedTools: Tool[];
  jobId: string | null;
  nodes: Node[];
  edges: Edge[];
  currentTaskId: string | null;
  completedTaskIds: string[];
  runHistory: RunHistoryItem[];
  userActive: boolean;
  inputVariables: Record<string, string>;
  showInputVariablesDialog: boolean;
  /**
   * The run the input dialog is collecting for. Carries its own nodes/edges
   * because the store's `nodes` holds whichever canvas was last synced, and
   * only the CREW canvas syncs — running a flow from `state.nodes` would send
   * agent/task nodes and fail with "requires at least one crew node".
   */
  pendingVariableExecution: { nodes: Node[]; edges: Edge[]; type: 'crew' | 'flow' } | null;

  // UI state
  errorMessage: string;
  showError: boolean;
  successMessage: string;
  showSuccess: boolean;

  // Trifecta warning dialog state
  showTrifectaDialog: boolean;
  trifectaAssessment: TrifectaAssessment | null;
  trifectaAcknowledged: boolean;
  pendingTrifectaExecution: { nodes: Node[]; edges: Edge[]; type: 'crew' | 'flow' } | null;

  // Setters
  setSelectedModel: (model: string) => void;
  setSelectedHarness: (harness: string) => void;
  setReasoningEnabled: (enabled: boolean) => void;
  setReasoningLLM: (model: string) => void;
  setReasoningConfig: (cfg: Partial<ReasoningConfig>) => void;
  setSchemaDetectionEnabled: (enabled: boolean) => void;
  setProcessType: (type: 'sequential' | 'hierarchical' | 'parallel') => void;
  setManagerLLM: (model: string) => void;
  setManagerNodeId: (id: string | null) => void;
  setIsLoadingCrew: (loading: boolean) => void;
  setCrewPlanningOpen: (open: boolean) => void;
  setScheduleDialogOpen: (open: boolean) => void;
  setSelectedTools: (tools: Tool[]) => void;
  setJobId: (id: string | null) => void;
  setErrorMessage: (message: string) => void;
  setShowError: (show: boolean) => void;
  setSuccessMessage: (message: string) => void;
  setShowSuccess: (show: boolean) => void;
  setNodes: (nodes: Node[]) => void;
  setEdges: (edges: Edge[]) => void;
  setIsExecuting: (isExecuting: boolean) => void;
  setTools: (tools: Tool[]) => void;
  setCurrentTaskId: (taskId: string | null) => void;
  setInputVariables: (variables: Record<string, string>) => void;
  setShowInputVariablesDialog: (show: boolean) => void;
  setInputMode: (mode: 'dialog' | 'chat') => void;
  setCompletedTaskIds: (taskIds: string[]) => void;
  setRunHistory: (history: RunHistoryItem[]) => void;
  setUserActive: (active: boolean) => void;
  cleanup: () => void;

  // Trifecta warning dialog methods
  setShowTrifectaDialog: (show: boolean) => void;
  handleTrifectaProceed: () => void;
  handleTrifectaCancel: () => void;

  // Execution methods
  executeCrew: (nodes: Node[], edges: Edge[]) => Promise<{ job_id: string } | null>;
  executeFlow: (nodes: Node[], edges: Edge[], savedFlowId?: string) => Promise<{ job_id: string } | null>;
  executeTab: (tabId: string, nodes: Node[], edges: Edge[], tabName?: string) => Promise<{ job_id: string } | null>;
  handleModelChange: (event: React.ChangeEvent<{ value: unknown }>) => void;
  handleRunClick: (type: 'crew' | 'flow') => Promise<void>;
  handleGenerateCrew: () => Promise<void>;
  executeWithVariables: (variables: Record<string, string>) => Promise<void>;
}

export const useCrewExecutionStore = create<CrewExecutionState>((set, get) => ({
  // Initial state
  isExecuting: false,
  selectedModel: 'databricks-gpt-5-3-codex',
  // Empty means "whatever Configuration -> Engines says". Persisted like the
  // process type so a choice survives a reload.
  selectedHarness: localStorage.getItem('kasal-harness') || '',
  reasoningEnabled: false,
  reasoningLLM: '',
  reasoningConfig: { ...DEFAULT_REASONING_CONFIG },
  schemaDetectionEnabled: true,
  processType: (localStorage.getItem('crewai-process-type') as 'sequential' | 'hierarchical' | 'parallel') || 'sequential',
  managerLLM: localStorage.getItem('crewai-manager-llm') || '',
  managerNodeId: null,
  isLoadingCrew: false,
  isCrewPlanningOpen: false,
  isScheduleDialogOpen: false,
  inputMode: (localStorage.getItem('crewai-input-mode') as 'dialog' | 'chat') || 'dialog',
  tools: [],
  selectedTools: [],
  jobId: null,
  nodes: [],
  edges: [],
  currentTaskId: null,
  completedTaskIds: [],
  runHistory: [],
  userActive: false,
  inputVariables: {},
  showInputVariablesDialog: false,
  pendingVariableExecution: null,
  errorMessage: '',
  showError: false,
  successMessage: '',
  showSuccess: false,

  // Trifecta warning dialog state
  showTrifectaDialog: false,
  trifectaAssessment: null,
  trifectaAcknowledged: false,
  pendingTrifectaExecution: null,

  // State setters
  setSelectedModel: (model) => set({ selectedModel: model as string }),
  setSelectedHarness: (harness) => {
    if (harness) {
      localStorage.setItem('kasal-harness', harness);
    } else {
      localStorage.removeItem('kasal-harness');
    }
    set({ selectedHarness: harness });
  },
  setReasoningEnabled: (enabled) => set({ reasoningEnabled: enabled }),
  setReasoningLLM: (model) => set({ reasoningLLM: model }),
  setReasoningConfig: (cfg) => set((state) => ({ reasoningConfig: { ...state.reasoningConfig, ...cfg } })),
  setSchemaDetectionEnabled: (enabled) => set({ schemaDetectionEnabled: enabled }),
  setProcessType: (type) => {
    console.log('[CrewExecutionStore] Setting process type to:', type);
    localStorage.setItem('crewai-process-type', type);
    set({ processType: type });
    console.log('[CrewExecutionStore] Process type set, new state:', get().processType);
  },
  setManagerLLM: (model) => {
    localStorage.setItem('crewai-manager-llm', model);
    set({ managerLLM: model });
  },
  setManagerNodeId: (id) => set({ managerNodeId: id }),
  setIsLoadingCrew: (loading) => set({ isLoadingCrew: loading }),
  setCrewPlanningOpen: (open) => set({ isCrewPlanningOpen: open }),
  setScheduleDialogOpen: (open) => set({ isScheduleDialogOpen: open }),
  setSelectedTools: (tools) => set({ selectedTools: tools }),
  setJobId: (id) => set({ jobId: id }),
  setErrorMessage: (message) => set({ errorMessage: message }),
  setShowError: (show) => set({ showError: show }),
  setSuccessMessage: (message) => set({ successMessage: message }),
  setShowSuccess: (show) => set({ showSuccess: show }),
  setNodes: (nodes) => {
    set({ nodes });
  },
  setEdges: (edges) => {
    set({ edges });
  },
  setIsExecuting: (isExecuting) => set({ isExecuting }),
  setTools: (tools) => set({ tools }),
  setCurrentTaskId: (taskId) => set({ currentTaskId: taskId }),
  setInputVariables: (variables) => set({ inputVariables: variables }),
  setShowInputVariablesDialog: (show) => set({ showInputVariablesDialog: show }),
  setInputMode: (mode) => {
    localStorage.setItem('crewai-input-mode', mode);
    set({ inputMode: mode });
  },
  setCompletedTaskIds: (taskIds) => set({ completedTaskIds: taskIds }),
  setRunHistory: (history) => set({ runHistory: history }),
  setUserActive: (active) => set({ userActive: active }),
  cleanup: () => set({
    isExecuting: false,
    jobId: null,
    currentTaskId: null,
    completedTaskIds: [],
    runHistory: [],
    userActive: false,
    errorMessage: '',
    showError: false,
    successMessage: '',
    showSuccess: false
  }),

  // Trifecta warning dialog methods
  setShowTrifectaDialog: (show) => set({ showTrifectaDialog: show }),
  handleTrifectaProceed: () => {
    const { pendingTrifectaExecution } = get();
    set({ showTrifectaDialog: false, trifectaAcknowledged: true, pendingTrifectaExecution: null });
    if (pendingTrifectaExecution) {
      void get().handleRunClick(pendingTrifectaExecution.type);
    }
  },
  handleTrifectaCancel: () => {
    set({ showTrifectaDialog: false, trifectaAssessment: null, pendingTrifectaExecution: null, trifectaAcknowledged: false });
  },

  // Execution methods
  executeCrew: async (nodes, edges) => {
    console.log('[CrewExecution] ========== executeCrew CALLED ==========');
    console.log('[CrewExecution] executeCrew - nodes:', nodes);
    console.log('[CrewExecution] executeCrew - edges:', edges);

    const { selectedModel, selectedHarness, reasoningEnabled, reasoningLLM, reasoningConfig, schemaDetectionEnabled, inputVariables, processType, managerLLM } = get();
    set({ isExecuting: true });

    try {
      const hasAgentNodes = nodes.some(node => node.type === 'agentNode');
      const hasTaskNodes = nodes.some(node => node.type === 'taskNode');

      if (!hasAgentNodes || !hasTaskNodes) {
        throw new Error('Crew execution requires at least one agent and one task node');
      }

      // Force refresh agents from database to get latest tools and knowledge_sources
      console.log('[CrewExecution] Refreshing agent data from database before execution');
      const { useAgentStore } = await import('./agent');
      const agentStore = useAgentStore.getState();

      const refreshedNodes = await Promise.all(
        nodes.map(async (node) => {
          if (node.type === 'agentNode' && node.data?.id) {
            try {
              // Force refresh from database
              const freshAgent = await agentStore.getAgent(node.data.id, true);
              if (freshAgent) {
                console.log(`[CrewExecution] Refreshed agent ${freshAgent.name} - tools:`, freshAgent.tools);
                warnOnConfigMismatch(
                  'agent',
                  freshAgent.name,
                  node.data as Record<string, unknown>,
                  freshAgent as unknown as Record<string, unknown>,
                );
                const mergedToolConfigs = mergeToolConfigs(
                  node.data?.tool_configs,
                  (freshAgent as unknown as Record<string, unknown>)?.tool_configs,
                );
                return {
                  ...node,
                  data: {
                    ...node.data,
                    ...freshAgent,
                    // Preserve canvas-specific data
                    position: node.data.position,
                    // The DB row must not erase a canvas-only tool config (MCP servers)
                    ...(mergedToolConfigs !== undefined ? { tool_configs: mergedToolConfigs } : {}),
                  }
                };
              }
            } catch (error) {
              console.error(`[CrewExecution] Failed to refresh agent ${node.data.id}:`, error);
            }
          }
          return node;
        })
      );

      // Use refreshed nodes for execution
      nodes = refreshedNodes;

      // Force refresh tasks from database to get latest tools and configs
      console.log('[CrewExecution] Refreshing task data from database before execution');
      const { TaskService } = await import('../api/workflow/TaskService');
      nodes = await Promise.all(
        nodes.map(async (node) => {
          if (node.type === 'taskNode' && (node.data?.taskId || node.data?.id)) {
            const taskId = node.data.taskId || node.data.id;
            try {
              const freshTask = await TaskService.getTask(taskId);
              if (freshTask) {
                // Surface DB/canvas divergence in tools AND tool_configs — an
                // unsaved edit otherwise changes the run with no trace anywhere.
                warnOnConfigMismatch(
                  'task',
                  freshTask.name,
                  node.data as Record<string, unknown>,
                  freshTask as unknown as Record<string, unknown>,
                );
                console.log(`[CrewExecution] Refreshed task ${freshTask.name} - tools:`, freshTask.tools);
                const mergedToolConfigs = mergeToolConfigs(
                  node.data?.tool_configs,
                  (freshTask as unknown as Record<string, unknown>)?.tool_configs,
                );
                return {
                  ...node,
                  data: {
                    ...node.data,
                    ...freshTask,
                    taskId: freshTask.id,
                    label: freshTask.name,
                    // The DB row must not erase a canvas-only tool config (MCP servers)
                    ...(mergedToolConfigs !== undefined ? { tool_configs: mergedToolConfigs } : {}),
                  }
                };
              }
            } catch (error) {
              console.error(`[CrewExecution] Failed to refresh task ${taskId}:`, error);
            }
          }
          return node;
        })
      );

      // Log the task nodes
      console.log('[CrewExecution] Task nodes before execution:',
        nodes.filter(node => node.type === 'taskNode')
          .map(node => ({
            id: node.id,
            type: node.type,
            data: {
              taskId: node.data?.taskId,
              label: node.data?.label,
              tools: node.data?.tools
            }
          }))
      );

      // Prepare additionalInputs with reasoning_llm, process type, and manager_llm
      const additionalInputs: Record<string, unknown> = {
        ...inputVariables,
        process: processType
      };
      if (reasoningEnabled && reasoningLLM) {
        additionalInputs.reasoning_llm = reasoningLLM;
      }
      if (reasoningEnabled) {
        additionalInputs.reasoning_config = reasoningConfig;
      }
      if (processType === 'hierarchical' && managerLLM) {
        additionalInputs.manager_llm = managerLLM;
      }
      
      console.log('[CrewExecution] Executing with inputs:', additionalInputs);

      // The tab's saved crew, when it has one. Recorded on the execution so a
      // resume from the history table can rebuild from the crew as it is NOW
      // rather than replaying the task text frozen into this run.
      const activeTab = useTabManagerStore.getState().tabs.find(
        tab => tab.id === useTabManagerStore.getState().activeTabId
      );
      const savedCrewId = activeTab?.savedCrewId || undefined;

      const response = await jobExecutionService.executeJob(
        nodes,
        edges,
        selectedModel,
        'crew',
        additionalInputs,
        schemaDetectionEnabled,
        reasoningEnabled,
        undefined,
        savedCrewId,
        selectedHarness || undefined
      );

      console.log('[CrewExecution] Job execution response:', response);

      set({ 
        successMessage: 'Crew executed successfully',
        showSuccess: true,
        jobId: response.job_id
      });

      // Open Execution History panel automatically when crew is executed
      const openExecutionHistoryEvent = new CustomEvent('openExecutionHistory');
      window.dispatchEvent(openExecutionHistoryEvent);

      // Dispatch custom jobCreated event to update the run history immediately
      const jobCreatedEvent = new CustomEvent('jobCreated', {
        detail: {
          jobId: response.execution_id || response.job_id,
          jobName: response.run_name || `Crew Execution (${new Date().toLocaleTimeString()})`,
          status: 'running',
          groupId: localStorage.getItem('selectedGroupId') // Include the group ID for security filtering
        }
      });
      console.log('[CrewExecution] Dispatching jobCreated event:', jobCreatedEvent.detail);
      window.dispatchEvent(jobCreatedEvent);

      // Dispatch task status update event to track task statuses
      const taskStatusUpdateEvent = new CustomEvent('taskStatusUpdate', {
        detail: {
          jobId: response.execution_id || response.job_id
        }
      });
      console.log('[CrewExecution] Dispatching taskStatusUpdate event:', taskStatusUpdateEvent.detail);
      window.dispatchEvent(taskStatusUpdateEvent);

      // Also dispatch the standard refreshRunHistory event
      window.dispatchEvent(new CustomEvent('refreshRunHistory'));
      return response;
    } catch (error) {
      console.error('[CrewExecution] Error executing crew:', error);
      
      // Check if this is a 409 conflict error (another job running)
      let errorMessage = 'Failed to execute crew';
      if (error instanceof Error) {
        if (error.message.includes('409:') || error.message.includes('another job is currently running')) {
          errorMessage = error.message.replace('409: ', '');
        } else {
          errorMessage = error.message;
        }
      }
      
      set({ 
        errorMessage,
        showError: true 
      });
      
      // Dispatch error event for chat panel to handle
      const errorEvent = new CustomEvent('executionError', {
        detail: {
          message: errorMessage,
          type: 'crew'
        }
      });
      console.log('[CrewExecution] Dispatching executionError event:', errorEvent.detail);
      window.dispatchEvent(errorEvent);
      
      return null;
    } finally {
      set({ isExecuting: false });
    }
  },

  executeFlow: async (nodes, edges, savedFlowId) => {
    console.log('[CrewExecution] ========== executeFlow CALLED ==========');
    console.log('[CrewExecution] executeFlow - nodes:', nodes);
    console.log('[CrewExecution] executeFlow - edges:', edges);
    console.log('[CrewExecution] executeFlow - savedFlowId:', savedFlowId);

    const { selectedModel, selectedHarness, reasoningEnabled, reasoningLLM, reasoningConfig, schemaDetectionEnabled, inputVariables } = get();
    set({ isExecuting: true });

    try {
      // Count the types of nodes for better debugging
      const nodeTypes: Record<string, number> = nodes.reduce((acc: Record<string, number>, node) => {
        const type = node.type || 'unknown';
        acc[type] = (acc[type] || 0) + 1;
        return acc;
      }, {});
      
      console.log('[FlowExecution] Node types on canvas:', nodeTypes);

      // Check for flow nodes (crewNode type)
      const hasFlowNodes = nodes.some(node => node.type === 'crewNode');

      if (!hasFlowNodes) {
        throw new Error('Flow execution requires at least one crew node on the canvas');
      }

      // Consider all node types as potential flow nodes for execution
      console.log('[FlowExecution] Flow nodes before execution:', 
        nodes.map(node => ({ 
          id: node.id, 
          type: node.type, 
          data: { 
            id: node.data?.id,
            label: node.data?.label,
            flowConfig: node.data?.flowConfig
          } 
        }))
      );

      // Prepare additionalInputs with reasoning_llm if enabled.
      // The collected values lead: they become the flow's kickoff inputs, get
      // merged into flow state, and are handed to each crew's kickoff, which is
      // what interpolates `{topic}` in its task text.
      const additionalInputs: Record<string, unknown> = { ...inputVariables };
      if (reasoningEnabled && reasoningLLM) {
        additionalInputs.reasoning_llm = reasoningLLM;
      }
      if (reasoningEnabled) {
        additionalInputs.reasoning_config = reasoningConfig;
      }

      console.log('[FlowExecution] Executing flow with model:', selectedModel);
      console.log('[FlowExecution] Reasoning enabled:', reasoningEnabled);
      console.log('[FlowExecution] Schema detection enabled:', schemaDetectionEnabled);

      const response = await jobExecutionService.executeJob(
        nodes,
        edges,
        selectedModel,
        'flow',
        additionalInputs,
        schemaDetectionEnabled,
        reasoningEnabled,
        savedFlowId,
        undefined,
        selectedHarness || undefined
      );

      console.log('[FlowExecution] Job execution response:', response);

      set({ 
        successMessage: 'Flow executed successfully',
        showSuccess: true,
        jobId: response.job_id
      });

      // Dispatch custom jobCreated event to update the run history immediately
      const jobCreatedEvent = new CustomEvent('jobCreated', {
        detail: {
          jobId: response.execution_id || response.job_id,
          jobName: response.run_name || `Flow Execution (${new Date().toLocaleTimeString()})`,
          status: 'running',
          groupId: localStorage.getItem('selectedGroupId'), // Include the group ID for security filtering
          isFlow: true // Flag to indicate this is a flow execution
        }
      });
      console.log('[FlowExecution] Dispatching jobCreated event:', jobCreatedEvent.detail);
      window.dispatchEvent(jobCreatedEvent);

      // Dispatch task status update event to track task statuses
      const taskStatusUpdateEvent = new CustomEvent('taskStatusUpdate', {
        detail: {
          jobId: response.execution_id || response.job_id
        }
      });
      console.log('[FlowExecution] Dispatching taskStatusUpdate event:', taskStatusUpdateEvent.detail);
      window.dispatchEvent(taskStatusUpdateEvent);

      // Also dispatch the standard refreshRunHistory event
      window.dispatchEvent(new CustomEvent('refreshRunHistory'));
      return response;
    } catch (error) {
      console.error('[FlowExecution] Error executing flow:', error);
      
      // Check if this is a 409 conflict error (another job running)
      let errorMessage = 'Failed to execute flow';
      if (error instanceof Error) {
        if (error.message.includes('409:') || error.message.includes('another job is currently running')) {
          errorMessage = error.message.replace('409: ', '');
        } else {
          errorMessage = error.message;
        }
      }
      
      set({ 
        errorMessage,
        showError: true 
      });
      return null;
    } finally {
      set({ isExecuting: false });
    }
  },

  executeTab: async (tabId, nodes, edges, tabName) => {
    const { selectedModel, selectedHarness, reasoningEnabled, reasoningLLM, reasoningConfig, schemaDetectionEnabled, processType, managerLLM } = get();
    set({ isExecuting: true });

    try {
      console.log(`[TabExecution] Executing tab ${tabId} (${tabName || 'Unnamed'}) with ${nodes.length} nodes and ${edges.length} edges`);

      // Determine execution type based on node types
      const hasAgentNodes = nodes.some(node => node.type === 'agentNode');
      const hasTaskNodes = nodes.some(node => node.type === 'taskNode');
      const hasFlowNodes = nodes.some(node => node.type === 'crewNode');

      let executionType: 'crew' | 'flow' = 'crew';

      if (hasFlowNodes) {
        executionType = 'flow';
      } else if (!hasAgentNodes || !hasTaskNodes) {
        throw new Error('Tab execution requires at least one agent and one task node for crew execution, or crew nodes for flow execution');
      }

      // Force refresh agents from database to get latest tools and knowledge_sources
      if (hasAgentNodes) {
        console.log('[TabExecution] Refreshing agent data from database before execution');
        const { useAgentStore } = await import('./agent');
        const agentStore = useAgentStore.getState();

        const refreshedNodes = await Promise.all(
          nodes.map(async (node) => {
            if (node.type === 'agentNode' && node.data?.id) {
              try {
                const freshAgent = await agentStore.getAgent(node.data.id, true);
                if (freshAgent) {
                  console.log(`[TabExecution] Refreshed agent ${freshAgent.name} - tools:`, freshAgent.tools);
                  return {
                    ...node,
                    data: {
                      ...node.data,
                      ...freshAgent,
                      position: node.data.position,
                    }
                  };
                }
              } catch (error) {
                console.error(`[TabExecution] Failed to refresh agent ${node.data.id}:`, error);
              }
            }
            return node;
          })
        );

        nodes = refreshedNodes;
      }

      // Force refresh tasks from database to get latest tools and configs
      if (hasTaskNodes) {
        console.log('[TabExecution] Refreshing task data from database before execution');
        const { TaskService } = await import('../api/workflow/TaskService');
        nodes = await Promise.all(
          nodes.map(async (node) => {
            if (node.type === 'taskNode' && (node.data?.taskId || node.data?.id)) {
              const taskId = node.data.taskId || node.data.id;
              try {
                const freshTask = await TaskService.getTask(taskId);
                if (freshTask) {
                  // Warn if DB tools differ from canvas — helps catch unsaved edits
                  const canvasTools = Array.isArray(node.data?.tools) ? node.data.tools : [];
                  const dbTools = Array.isArray(freshTask.tools) ? freshTask.tools : [];
                  if (JSON.stringify(canvasTools.map(String).sort()) !== JSON.stringify(dbTools.map(String).sort())) {
                    console.warn(
                      `[TabExecution] Tool mismatch for task "${freshTask.name}" — canvas: [${canvasTools}], DB: [${dbTools}]. ` +
                      `Using DB version. If unexpected, ensure you saved the task after editing tools.`
                    );
                  }
                  console.log(`[TabExecution] Refreshed task ${freshTask.name} - tools:`, freshTask.tools);
                  return {
                    ...node,
                    data: {
                      ...node.data,
                      ...freshTask,
                      taskId: freshTask.id,
                      label: freshTask.name,
                    }
                  };
                }
              } catch (error) {
                console.error(`[TabExecution] Failed to refresh task ${taskId}:`, error);
              }
            }
            return node;
          })
        );
      }

      // Prepare additionalInputs with reasoning_llm, process type, and manager_llm
      const additionalInputs: Record<string, unknown> = {
        process: processType
      };
      if (reasoningEnabled && reasoningLLM) {
        additionalInputs.reasoning_llm = reasoningLLM;
      }
      if (reasoningEnabled) {
        additionalInputs.reasoning_config = reasoningConfig;
      }
      if (processType === 'hierarchical' && managerLLM) {
        additionalInputs.manager_llm = managerLLM;
      }

      console.log(`[TabExecution] Executing tab as ${executionType} with model:`, selectedModel);

      // Record which saved definition the run came from, so a resume can rebuild
      // from it. Whichever of the two is set follows from executionType, and an
      // unsaved tab sends neither.
      const tab = useTabManagerStore.getState().tabs.find(t => t.id === tabId);

      const response = await jobExecutionService.executeJob(
        nodes,
        edges,
        selectedModel,
        executionType,
        additionalInputs,
        schemaDetectionEnabled,
        reasoningEnabled,
        tab?.savedFlowId || undefined,
        tab?.savedCrewId || undefined,
        selectedHarness || undefined
      );

      console.log('[TabExecution] Job execution response:', response);

      set({ 
        successMessage: `Tab "${tabName || 'Unnamed'}" executed successfully`,
        showSuccess: true,
        jobId: response.job_id
      });

      // Dispatch custom jobCreated event to update the run history immediately
      const jobCreatedEvent = new CustomEvent('jobCreated', {
        detail: {
          jobId: response.execution_id || response.job_id,
          jobName: response.run_name || `${tabName || 'Unnamed Tab'} (${new Date().toLocaleTimeString()})`,
          status: 'running',
          groupId: localStorage.getItem('selectedGroupId') // Include the group ID for security filtering
        }
      });
      console.log('[TabExecution] Dispatching jobCreated event:', jobCreatedEvent.detail);
      window.dispatchEvent(jobCreatedEvent);

      // Dispatch task status update event to track task statuses
      const taskStatusUpdateEvent = new CustomEvent('taskStatusUpdate', {
        detail: {
          jobId: response.execution_id || response.job_id
        }
      });
      console.log('[TabExecution] Dispatching taskStatusUpdate event:', taskStatusUpdateEvent.detail);
      window.dispatchEvent(taskStatusUpdateEvent);

      // Also dispatch the standard refreshRunHistory event
      window.dispatchEvent(new CustomEvent('refreshRunHistory'));
      return response;
    } catch (error) {
      console.error('[TabExecution] Error executing tab:', error);
      set({ 
        errorMessage: error instanceof Error ? error.message : `Failed to execute tab "${tabName || 'Unnamed'}"`,
        showError: true 
      });
      return null;
    } finally {
      set({ isExecuting: false });
    }
  },

  handleModelChange: (event) => {
    set({ selectedModel: event.target.value as string });
  },

  handleRunClick: async (type) => {
    const state = get();

    console.log('[CrewExecution] handleRunClick called with type:', type);
    console.log('[CrewExecution] Current nodes:', state.nodes);

    // Resolve correct nodes/edges based on execution type from tab manager
    // The crewExecution store has a single nodes/edges property that gets overwritten
    // when switching between crew and flow canvases. Read directly from tab state instead.
    const tabState = useTabManagerStore.getState();
    const activeTab = tabState.tabs.find(t => t.id === tabState.activeTabId);
    let resolvedNodes: Node[];
    let resolvedEdges: Edge[];
    if (type === 'crew' && activeTab) {
      resolvedNodes = activeTab.nodes;
      resolvedEdges = activeTab.edges;
    } else if (type === 'flow' && activeTab) {
      resolvedNodes = activeTab.flowNodes;
      resolvedEdges = activeTab.flowEdges;
    } else {
      resolvedNodes = state.nodes;
      resolvedEdges = state.edges;
    }
    console.log('[CrewExecution] Resolved nodes for', type, ':', resolvedNodes.length);

    // ── Trifecta pre-flight security check ──────────────────────────────────
    // Runs on every "Run" click unless the user has already acknowledged the
    // warning for this execution (trifectaAcknowledged is set by handleTrifectaProceed).
    if (!state.trifectaAcknowledged) {
      // Collect all tool IDs from agent + task nodes
      const toolIdSet = new Set<string>();
      for (const node of resolvedNodes) {
        const data = node.data as Record<string, unknown>;
        const nodeTools = data.tools;
        if (Array.isArray(nodeTools)) {
          for (const t of nodeTools) toolIdSet.add(String(t));
        }
      }
      // Fetch the authoritative tool list from the API to resolve IDs → titles
      // (state.tools is local component state in CrewCanvas and not available here)
      let toolTitles: string[] = [];
      try {
        const allTools = await ToolService.listEnabledTools();
        toolTitles = allTools
          .filter(t => toolIdSet.has(String(t.id)))
          .map(t => t.title);
      } catch {
        // If the fetch fails, skip the check and proceed with execution
        console.warn('[CrewExecution] Could not fetch tools for trifecta check — skipping');
        set({ trifectaAcknowledged: false });
      }

      const assessment = assessTrifecta(toolTitles);
      if (assessment.hasTrifecta) {
        console.warn('[CrewExecution] Lethal trifecta detected — showing pre-flight warning');
        set({
          showTrifectaDialog: true,
          trifectaAssessment: assessment,
          pendingTrifectaExecution: { nodes: resolvedNodes, edges: resolvedEdges, type: type as 'crew' | 'flow' },
        });
        return; // wait for user choice in TrifectaWarningDialog
      }
    }
    // Reset acknowledgment so the next independent run will check again
    set({ trifectaAcknowledged: false });
    // ────────────────────────────────────────────────────────────────────────

    /**
     * Run the flow on the canvas, always from the beginning.
     *
     * Pressing Run used to first list the flow's checkpoints and offer to
     * resume from one. That prompt existed only for flows — a crew run has no
     * saved-crew link on its execution row to list checkpoints by — so the two
     * canvases disagreed about what Run meant, and the resume-point picker it
     * opened duplicated the one in the execution history table with the
     * opposite wording ("resume after X" vs "redo from X").
     *
     * Resuming now lives in one place, the history table's checkpoint dialog,
     * which is execution-scoped and therefore works the same for both paths.
     */
    const startFlowExecution = async (nodes: Node[], edges: Edge[]) => {
      // Clear the flow's visual indicators up front, so crew nodes drop their
      // green/red state the moment Run is pressed rather than when the first
      // event arrives.
      useFlowExecutionStore.getState().clearStates();

      const tabManagerState = useTabManagerStore.getState();
      const activeTab = tabManagerState.tabs.find(tab => tab.id === tabManagerState.activeTabId);
      const savedFlowId = activeTab?.savedFlowId || undefined;

      window.dispatchEvent(new CustomEvent('openExecutionHistory'));
      await state.executeFlow(nodes, edges, savedFlowId);
    };

    // Check if we need to show input variables dialog
    // Only check for variables in the nodes relevant to the execution type
    const variablePattern = /\{([a-zA-Z_][a-zA-Z0-9_-]*)\}/g;
    // A flow's placeholders are not where a crew's are. Its canvas holds
    // `crewNode`s, so the scan below skips every node on type alone, and the
    // text that carries `{topic}` sits nested under the referenced crew's
    // `allTasks[].description` rather than on the node itself. The deep walk in
    // utils/flowInputs is the one that reaches it, and is already what the
    // publish dialog and ChatMode use — so a flow asks for the same values
    // however it is started.
    const hasVariables = type === 'flow'
      ? placeholdersInFlowNodes(resolvedNodes).length > 0
      : resolvedNodes.some(node => {
      // For crew execution, check agent and task nodes
      if (type === 'crew' && (node.type === 'agentNode' || node.type === 'taskNode')) {
        const data = node.data as Record<string, unknown>;
        const fieldsToCheck = [
          data.role,
          data.goal,
          data.backstory,
          data.description,
          data.expected_output,
          data.label
        ];

        // Also collect string values from tool_configs (e.g. {user_question} in Reducer config).
        // Skip keys that contain raw pipeline config / SQL / YAML content — those may contain
        // {placeholder} patterns that are part of the config syntax, not Kasal execution variables.
        const RAW_CONTENT_KEYS = new Set([
          'config_json', 'measures_json', 'mquery_json', 'relationships_json',
          'scan_data_json', 'yaml_specs_json', 'sql_specs_json', 'dax_measures_json',
        ]);
        const toolConfigs = (data.tool_configs || (data.task as Record<string, unknown>)?.tool_configs) as Record<string, Record<string, unknown>> | undefined;
        if (toolConfigs && typeof toolConfigs === 'object') {
          Object.values(toolConfigs).forEach(toolCfg => {
            if (toolCfg && typeof toolCfg === 'object') {
              Object.entries(toolCfg).forEach(([key, val]) => {
                if (!RAW_CONTENT_KEYS.has(key) && val && typeof val === 'string') {
                  fieldsToCheck.push(val);
                }
              });
            }
          });
        }

        console.log('[CrewExecution] Checking node:', node.id, 'type:', node.type);

        const hasVar = fieldsToCheck.some(field => {
          if (field && typeof field === 'string') {
            // Reset regex lastIndex to ensure proper matching
            variablePattern.lastIndex = 0;
            const matches = variablePattern.test(field);
            if (matches) {
              console.log('[CrewExecution] Found variable in field:', field);
            }
            return matches;
          }
          return false;
        });

        if (hasVar) {
          console.log('[CrewExecution] Node has variables:', node.id);
        }

        return hasVar;
      }
      return false;
    });

    console.log('[CrewExecution] Has variables:', hasVariables);
    console.log('[CrewExecution] Input mode:', state.inputMode);

    if (hasVariables) {
      if (state.inputMode === 'dialog') {
        // Show the input variables dialog instead of executing immediately
        set({
          showInputVariablesDialog: true,
          pendingVariableExecution: {
            nodes: resolvedNodes,
            edges: resolvedEdges,
            type: type as 'crew' | 'flow',
          },
        });
      } else {
        // Chat mode: Will be handled by chat interface
        console.log('[CrewExecution] Chat mode selected - variables will be collected via chat');
        // For now, execute without variables - chat collection will be implemented next
        set({ isExecuting: true });
        try {
          if (type === 'crew') {
            await state.executeCrew(resolvedNodes, resolvedEdges);
          } else {
            await startFlowExecution(resolvedNodes, resolvedEdges);
          }
        } catch (error) {
          set({
            errorMessage: error instanceof Error ? error.message : 'Failed to execute',
            showError: true
          });
        } finally {
          set({ isExecuting: false });
        }
      }
    } else {
      // No variables, execute immediately
      set({ isExecuting: true });

      try {
        console.log('[CrewExecution] Type check - type:', type, 'comparison result:', type === 'crew');
        if (type === 'crew') {
          console.log('[CrewExecution] Executing CREW path');
          await state.executeCrew(resolvedNodes, resolvedEdges);
        } else {
          console.log('[CrewExecution] Executing FLOW path');
          await startFlowExecution(resolvedNodes, resolvedEdges);
        }
      } catch (error) {
        set({
          errorMessage: error instanceof Error ? error.message : 'Failed to execute',
          showError: true
        });
      } finally {
        set({ isExecuting: false });
      }
    }
  },

  handleGenerateCrew: async () => {
    const { nodes, edges } = useWorkflowStore.getState();
    const { reasoningEnabled, reasoningLLM, reasoningConfig, selectedModel, schemaDetectionEnabled } = get();
    set({ isExecuting: true });

    try {
      // Prepare additionalInputs with reasoning_llm if enabled
      const additionalInputs: Record<string, unknown> = { generate: true };
      if (reasoningEnabled && reasoningLLM) {
        additionalInputs.reasoning_llm = reasoningLLM;
      }
      if (reasoningEnabled) {
        additionalInputs.reasoning_config = reasoningConfig;
      }

      const response = await jobExecutionService.executeJob(
        nodes,
        edges,
        selectedModel,
        'crew',
        additionalInputs,
        schemaDetectionEnabled,
        reasoningEnabled
      );

      set({
        successMessage: 'Crew generated successfully',
        showSuccess: true,
        jobId: response.job_id
      });

      // Dispatch custom jobCreated event to update the run history immediately
      window.dispatchEvent(new CustomEvent('jobCreated', {
        detail: {
          jobId: response.execution_id || response.job_id,
          jobName: response.run_name || `Crew Generation (${new Date().toLocaleTimeString()})`,
          status: 'running',
          groupId: localStorage.getItem('selectedGroupId') // Include the group ID for security filtering
        }
      }));

      // Also dispatch the standard refreshRunHistory event
      window.dispatchEvent(new CustomEvent('refreshRunHistory'));
    } catch (error) {
      set({ 
        errorMessage: error instanceof Error ? error.message : 'Failed to generate crew',
        showError: true 
      });
    } finally {
      set({ isExecuting: false });
    }
  },

  executeWithVariables: async (variables: Record<string, string>) => {
    const state = get();
    set({
      inputVariables: variables,
      showInputVariablesDialog: false,
      isExecuting: true
    });

    try {
      // Run exactly what the dialog was opened for. Falling back to the
      // store's nodes only when nothing is pending keeps older callers working.
      const pending = state.pendingVariableExecution;
      const executionType = pending?.type ?? 'crew';
      const nodes = pending?.nodes ?? state.nodes;
      const edges = pending?.edges ?? state.edges;
      set({ pendingVariableExecution: null });

      if (executionType === 'crew') {
        await state.executeCrew(nodes, edges);
      } else {
        // Get savedFlowId from tab manager for flow executions
        const tabManagerState = useTabManagerStore.getState();
        const activeTab = tabManagerState.tabs.find(tab => tab.id === tabManagerState.activeTabId);
        const savedFlowId = activeTab?.savedFlowId || undefined;
        await state.executeFlow(nodes, edges, savedFlowId);
      }
    } catch (error) {
      set({
        errorMessage: error instanceof Error ? error.message : 'Failed to execute',
        showError: true
      });
    } finally {
      set({ isExecuting: false });
    }
  }
}));

// Expose store on window for debugging
if (typeof window !== 'undefined') {
  (window as unknown as Record<string, unknown>).useCrewExecutionStore = useCrewExecutionStore;
}