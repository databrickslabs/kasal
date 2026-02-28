import { create } from 'zustand';
import { Node, Edge } from 'reactflow';
import { jobExecutionService } from '../api/JobExecutionService';
import { useWorkflowStore } from './workflow';
import { useTabManagerStore } from './tabManager';
import { useFlowExecutionStore } from './flowExecutionStore';
import { Tool } from '../types/tool';
import { FlowService, FlowCheckpoint } from '../api/FlowService';

interface RunHistoryItem {
  id: string;
  jobId: string;
  status: string;
  createdAt: string;
  updatedAt: string;
  error?: string;
}

interface CrewExecutionState {
  // Execution state
  isExecuting: boolean;
  selectedModel: string;
  planningEnabled: boolean;
  planningLLM: string;
  reasoningEnabled: boolean;
  reasoningLLM: string;
  schemaDetectionEnabled: boolean;
  processType: 'sequential' | 'hierarchical';
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
  pendingExecutionType: string | null;

  // UI state
  errorMessage: string;
  showError: boolean;
  successMessage: string;
  showSuccess: boolean;

  // Checkpoint dialog state
  showCheckpointDialog: boolean;
  checkpoints: FlowCheckpoint[];
  checkpointsLoading: boolean;
  checkpointsError: string | null;
  pendingFlowExecution: {
    nodes: Node[];
    edges: Edge[];
    savedFlowId: string;
    savedFlowName?: string;
  } | null;

  // Setters
  setSelectedModel: (model: string) => void;
  setPlanningEnabled: (enabled: boolean) => void;
  setPlanningLLM: (model: string) => void;
  setReasoningEnabled: (enabled: boolean) => void;
  setReasoningLLM: (model: string) => void;
  setSchemaDetectionEnabled: (enabled: boolean) => void;
  setProcessType: (type: 'sequential' | 'hierarchical') => void;
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

  // Checkpoint dialog methods
  setShowCheckpointDialog: (show: boolean) => void;
  setCheckpoints: (checkpoints: FlowCheckpoint[]) => void;
  setCheckpointsLoading: (loading: boolean) => void;
  setCheckpointsError: (error: string | null) => void;
  setPendingFlowExecution: (pending: { nodes: Node[]; edges: Edge[]; savedFlowId: string; savedFlowName?: string } | null) => void;
  handleCheckpointStartFresh: () => Promise<void>;
  handleCheckpointResume: (checkpoint: FlowCheckpoint, selectedCrewSequence?: number) => Promise<void>;
  handleCheckpointDelete: (executionId: number) => Promise<void>;
  refreshCheckpoints: () => Promise<void>;

  // Execution methods
  executeCrew: (nodes: Node[], edges: Edge[]) => Promise<{ job_id: string } | null>;
  executeFlow: (nodes: Node[], edges: Edge[], resumeFromFlowUuid?: string, resumeFromExecutionId?: number, savedFlowId?: string, resumeFromCrewSequence?: number) => Promise<{ job_id: string } | null>;
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
  planningEnabled: false,
  planningLLM: '',
  reasoningEnabled: false,
  reasoningLLM: '',
  schemaDetectionEnabled: true,
  processType: (localStorage.getItem('crewai-process-type') as 'sequential' | 'hierarchical') || 'sequential',
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
  pendingExecutionType: null,
  errorMessage: '',
  showError: false,
  successMessage: '',
  showSuccess: false,

  // Checkpoint dialog state
  showCheckpointDialog: false,
  checkpoints: [],
  checkpointsLoading: false,
  checkpointsError: null,
  pendingFlowExecution: null,

  // State setters
  setSelectedModel: (model) => set({ selectedModel: model as string }),
  setPlanningEnabled: (enabled) => set({ planningEnabled: enabled }),
  setPlanningLLM: (model) => set({ planningLLM: model }),
  setReasoningEnabled: (enabled) => set({ reasoningEnabled: enabled }),
  setReasoningLLM: (model) => set({ reasoningLLM: model }),
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

  // Checkpoint dialog setters
  setShowCheckpointDialog: (show) => set({ showCheckpointDialog: show }),
  setCheckpoints: (checkpoints) => set({ checkpoints }),
  setCheckpointsLoading: (loading) => set({ checkpointsLoading: loading }),
  setCheckpointsError: (error) => set({ checkpointsError: error }),
  setPendingFlowExecution: (pending) => set({ pendingFlowExecution: pending }),

  // Checkpoint dialog handlers
  handleCheckpointStartFresh: async () => {
    const { pendingFlowExecution, executeFlow } = get();
    if (pendingFlowExecution) {
      const { nodes, edges, savedFlowId } = pendingFlowExecution;
      set({
        showCheckpointDialog: false,
        pendingFlowExecution: null,
        checkpoints: [],
        checkpointsError: null
      });
      console.log('[CrewExecution] Starting fresh flow execution with savedFlowId:', savedFlowId);
      await executeFlow(nodes, edges, undefined, undefined, savedFlowId);
    }
  },

  handleCheckpointResume: async (checkpoint: FlowCheckpoint, selectedCrewSequence?: number) => {
    const { pendingFlowExecution, executeFlow } = get();
    if (pendingFlowExecution) {
      const { nodes, edges, savedFlowId } = pendingFlowExecution;
      set({
        showCheckpointDialog: false,
        pendingFlowExecution: null,
        checkpoints: [],
        checkpointsError: null
      });
      console.log('[CrewExecution] Resuming from checkpoint:', checkpoint.flow_uuid, 'with savedFlowId:', savedFlowId);
      if (selectedCrewSequence !== undefined && selectedCrewSequence !== null) {
        console.log('[CrewExecution] Resuming from crew sequence:', selectedCrewSequence);
      }
      await executeFlow(nodes, edges, checkpoint.flow_uuid, checkpoint.execution_id, savedFlowId, selectedCrewSequence);
    }
  },

  handleCheckpointDelete: async (executionId: number) => {
    const { pendingFlowExecution, checkpoints } = get();
    if (pendingFlowExecution) {
      const success = await FlowService.deleteFlowCheckpoint(pendingFlowExecution.savedFlowId, executionId);
      if (success) {
        // Remove the deleted checkpoint from the list
        const updatedCheckpoints = checkpoints.filter(cp => cp.execution_id !== executionId);
        set({ checkpoints: updatedCheckpoints });
        // If no checkpoints left, close dialog and start fresh
        if (updatedCheckpoints.length === 0) {
          const { handleCheckpointStartFresh } = get();
          await handleCheckpointStartFresh();
        }
      }
    }
  },

  refreshCheckpoints: async () => {
    const { pendingFlowExecution } = get();
    if (pendingFlowExecution) {
      set({ checkpointsLoading: true, checkpointsError: null });
      try {
        const response = await FlowService.getFlowCheckpoints(pendingFlowExecution.savedFlowId);
        set({ checkpoints: response.checkpoints, checkpointsLoading: false });
      } catch (error) {
        set({
          checkpointsError: error instanceof Error ? error.message : 'Failed to fetch checkpoints',
          checkpointsLoading: false
        });
      }
    }
  },

  // Execution methods
  executeCrew: async (nodes, edges) => {
    console.log('[CrewExecution] ========== executeCrew CALLED ==========');
    console.log('[CrewExecution] executeCrew - nodes:', nodes);
    console.log('[CrewExecution] executeCrew - edges:', edges);

    const { selectedModel, planningEnabled, planningLLM, reasoningEnabled, reasoningLLM, schemaDetectionEnabled, inputVariables, processType, managerLLM } = get();
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
                return {
                  ...node,
                  data: {
                    ...node.data,
                    ...freshAgent,
                    // Preserve canvas-specific data
                    position: node.data.position,
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
      const { TaskService } = await import('../api/TaskService');
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
                    `[CrewExecution] Tool mismatch for task "${freshTask.name}" — canvas: [${canvasTools}], DB: [${dbTools}]. ` +
                    `Using DB version. If unexpected, ensure you saved the task after editing tools.`
                  );
                }
                console.log(`[CrewExecution] Refreshed task ${freshTask.name} - tools:`, freshTask.tools);
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

      // Prepare additionalInputs with planning_llm, reasoning_llm, process type, and manager_llm
      const additionalInputs: Record<string, unknown> = {
        ...inputVariables,
        process: processType
      };
      if (planningEnabled && planningLLM) {
        additionalInputs.planning_llm = planningLLM;
      }
      if (reasoningEnabled && reasoningLLM) {
        additionalInputs.reasoning_llm = reasoningLLM;
      }
      if (processType === 'hierarchical' && managerLLM) {
        additionalInputs.manager_llm = managerLLM;
      }
      
      console.log('[CrewExecution] Executing with inputs:', additionalInputs);

      const response = await jobExecutionService.executeJob(
        nodes,
        edges,
        planningEnabled,
        selectedModel,
        'crew',
        additionalInputs,
        schemaDetectionEnabled,
        reasoningEnabled
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
          jobName: `Crew Execution (${new Date().toLocaleTimeString()})`,
          status: 'running',
          groupId: localStorage.getItem('selectedGroupId'), // Include the group ID for security filtering
          planningEnabled
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

  executeFlow: async (nodes, edges, resumeFromFlowUuid, resumeFromExecutionId, savedFlowId, resumeFromCrewSequence) => {
    console.log('[CrewExecution] ========== executeFlow CALLED ==========');
    console.log('[CrewExecution] executeFlow - nodes:', nodes);
    console.log('[CrewExecution] executeFlow - edges:', edges);
    console.log('[CrewExecution] executeFlow - resumeFromFlowUuid:', resumeFromFlowUuid);
    console.log('[CrewExecution] executeFlow - resumeFromExecutionId:', resumeFromExecutionId);
    console.log('[CrewExecution] executeFlow - savedFlowId:', savedFlowId);
    console.log('[CrewExecution] executeFlow - resumeFromCrewSequence:', resumeFromCrewSequence);

    const { selectedModel, planningEnabled, planningLLM, reasoningEnabled, reasoningLLM, schemaDetectionEnabled } = get();
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

      // Prepare additionalInputs with planning_llm and reasoning_llm if enabled
      const additionalInputs: Record<string, unknown> = {};
      if (planningEnabled && planningLLM) {
        additionalInputs.planning_llm = planningLLM;
      }
      if (reasoningEnabled && reasoningLLM) {
        additionalInputs.reasoning_llm = reasoningLLM;
      }

      console.log('[FlowExecution] Executing flow with model:', selectedModel);
      console.log('[FlowExecution] Planning enabled:', planningEnabled);
      console.log('[FlowExecution] Reasoning enabled:', reasoningEnabled);
      console.log('[FlowExecution] Schema detection enabled:', schemaDetectionEnabled);

      const response = await jobExecutionService.executeJob(
        nodes,
        edges,
        planningEnabled,
        selectedModel,
        'flow',
        additionalInputs,
        schemaDetectionEnabled,
        reasoningEnabled,
        resumeFromFlowUuid,
        resumeFromExecutionId,
        savedFlowId,
        resumeFromCrewSequence
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
          jobName: `Flow Execution (${new Date().toLocaleTimeString()})`,
          status: 'running',
          groupId: localStorage.getItem('selectedGroupId'), // Include the group ID for security filtering
          isFlow: true, // Flag to indicate this is a flow execution
          planningEnabled
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
    const { selectedModel, planningEnabled, planningLLM, reasoningEnabled, reasoningLLM, schemaDetectionEnabled, processType, managerLLM } = get();
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
        const { TaskService } = await import('../api/TaskService');
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

      // Prepare additionalInputs with planning_llm, reasoning_llm, process type, and manager_llm
      const additionalInputs: Record<string, unknown> = {
        process: processType
      };
      if (planningEnabled && planningLLM) {
        additionalInputs.planning_llm = planningLLM;
      }
      if (reasoningEnabled && reasoningLLM) {
        additionalInputs.reasoning_llm = reasoningLLM;
      }
      if (processType === 'hierarchical' && managerLLM) {
        additionalInputs.manager_llm = managerLLM;
      }

      console.log(`[TabExecution] Executing tab as ${executionType} with model:`, selectedModel);

      const response = await jobExecutionService.executeJob(
        nodes,
        edges,
        planningEnabled,
        selectedModel,
        executionType,
        additionalInputs,
        schemaDetectionEnabled,
        reasoningEnabled
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
          jobName: `${tabName || 'Unnamed Tab'} (${new Date().toLocaleTimeString()})`,
          status: 'running',
          groupId: localStorage.getItem('selectedGroupId'), // Include the group ID for security filtering
          planningEnabled
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

    // Helper function to check for checkpoints and handle flow execution
    const checkForCheckpointsAndExecuteFlow = async (nodes: Node[], edges: Edge[]) => {
      console.log('[CrewExecution] Checking for checkpoints before flow execution');

      // IMMEDIATELY clear flow execution visual indicators before starting new execution
      // This ensures crew node states reset to default (no green/red indicators) right when user clicks Run
      console.log('[CrewExecution] Clearing flow execution visual states before starting');
      useFlowExecutionStore.getState().clearStates();

      // Get the current tab's saved flow ID
      const tabManagerState = useTabManagerStore.getState();
      const activeTab = tabManagerState.tabs.find(tab => tab.id === tabManagerState.activeTabId);
      const savedFlowId = activeTab?.savedFlowId || null;
      const savedFlowName = activeTab?.savedFlowName || undefined;

      console.log('[CrewExecution] Checkpoint check - savedFlowId:', savedFlowId);

      // Check if any edge has checkpoint enabled
      const hasPersistenceEnabled = edges.some(edge => edge.data?.checkpoint === true);
      console.log('[CrewExecution] Checkpoint check - hasPersistenceEnabled:', hasPersistenceEnabled);

      if (savedFlowId && hasPersistenceEnabled) {
        console.log('[CrewExecution] Checking for available checkpoints...');
        set({ checkpointsLoading: true });

        try {
          const response = await FlowService.getFlowCheckpoints(savedFlowId);
          console.log('[CrewExecution] Available checkpoints:', response.checkpoints);

          if (response.checkpoints.length > 0) {
            console.log('[CrewExecution] Found checkpoints, showing resume dialog');
            // Store the pending execution and show the dialog
            set({
              checkpoints: response.checkpoints,
              checkpointsLoading: false,
              pendingFlowExecution: { nodes, edges, savedFlowId, savedFlowName },
              showCheckpointDialog: true
            });
            return; // Don't execute yet, wait for user choice
          }
          console.log('[CrewExecution] No checkpoints found, starting fresh');
          set({ checkpointsLoading: false });
        } catch (error) {
          console.error('[CrewExecution] Error checking checkpoints:', error);
          set({ checkpointsLoading: false });
          // Continue with fresh execution on error
        }
      } else {
        console.log('[CrewExecution] Skipping checkpoint check - flow not saved or persistence not enabled');
      }

      // No checkpoints or persistence not enabled, execute immediately
      window.dispatchEvent(new CustomEvent('openExecutionHistory'));
      await state.executeFlow(nodes, edges, undefined, undefined, savedFlowId || undefined);
    };

    // Check if we need to show input variables dialog
    // Only check for variables in the nodes relevant to the execution type
    const variablePattern = /\{([a-zA-Z_][a-zA-Z0-9_-]*)\}/g;
    const hasVariables = resolvedNodes.some(node => {
      // For crew execution, check agent and task nodes
      // For flow execution, we don't check for input variables (flows use crew configurations)
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

        console.log('[CrewExecution] Checking node:', node.id, 'type:', node.type);
        console.log('[CrewExecution] Node data:', data);

        const hasVar = fieldsToCheck.some(field => {
          if (field && typeof field === 'string') {
            console.log('[CrewExecution] Checking field:', field);
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
        set({ showInputVariablesDialog: true, pendingExecutionType: type });
      } else {
        // Chat mode: Will be handled by chat interface
        console.log('[CrewExecution] Chat mode selected - variables will be collected via chat');
        // For now, execute without variables - chat collection will be implemented next
        set({ isExecuting: true });
        try {
          if (type === 'crew') {
            await state.executeCrew(resolvedNodes, resolvedEdges);
          } else {
            // Check for checkpoints before executing flow
            await checkForCheckpointsAndExecuteFlow(resolvedNodes, resolvedEdges);
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
          // Check for checkpoints before executing flow
          await checkForCheckpointsAndExecuteFlow(resolvedNodes, resolvedEdges);
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
    const { planningEnabled, planningLLM, reasoningEnabled, reasoningLLM, selectedModel, schemaDetectionEnabled } = get();
    set({ isExecuting: true });

    try {
      // Prepare additionalInputs with planning_llm and reasoning_llm if enabled
      const additionalInputs: Record<string, unknown> = { generate: true };
      if (planningEnabled && planningLLM) {
        additionalInputs.planning_llm = planningLLM;
      }
      if (reasoningEnabled && reasoningLLM) {
        additionalInputs.reasoning_llm = reasoningLLM;
      }

      const response = await jobExecutionService.executeJob(
        nodes,
        edges,
        planningEnabled,
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
          jobName: `Crew Generation (${new Date().toLocaleTimeString()})`,
          status: 'running',
          groupId: localStorage.getItem('selectedGroupId'), // Include the group ID for security filtering
          planningEnabled
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
      // Get the pending execution type from store state
      const executionType = state.pendingExecutionType || 'crew';
      set({ pendingExecutionType: null });

      if (executionType === 'crew') {
        await state.executeCrew(state.nodes, state.edges);
      } else {
        // Get savedFlowId from tab manager for flow executions
        const tabManagerState = useTabManagerStore.getState();
        const activeTab = tabManagerState.tabs.find(tab => tab.id === tabManagerState.activeTabId);
        const savedFlowId = activeTab?.savedFlowId || undefined;
        await state.executeFlow(state.nodes, state.edges, undefined, undefined, savedFlowId);
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