import { create } from 'zustand';
import { Node, Edge } from 'reactflow';
import { jobExecutionService } from '../api/JobExecutionService';
import { useWorkflowStore } from './workflow';
import { Tool } from '../types/tool';

interface Crew {
  id: string;
  name: string;
  description: string;
  // Add other crew properties as needed
}

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
  isCrewPlanningOpen: boolean;
  isScheduleDialogOpen: boolean;
  tools: Tool[];
  selectedTools: Tool[];
  jobId: string | null;
  nodes: Node[];
  edges: Edge[];
  currentTaskId: string | null;
  completedTaskIds: string[];
  runHistory: RunHistoryItem[];
  userActive: boolean;
  
  // UI state
  errorMessage: string;
  showError: boolean;
  successMessage: string;
  showSuccess: boolean;

  // Setters
  setSelectedModel: (model: string) => void;
  setPlanningEnabled: (enabled: boolean) => void;
  setPlanningLLM: (model: string) => void;
  setReasoningEnabled: (enabled: boolean) => void;
  setReasoningLLM: (model: string) => void;
  setSchemaDetectionEnabled: (enabled: boolean) => void;
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
  setCompletedTaskIds: (taskIds: string[]) => void;
  setRunHistory: (history: RunHistoryItem[]) => void;
  setUserActive: (active: boolean) => void;
  cleanup: () => void;

  // Execution methods
  executeCrew: (nodes: Node[], edges: Edge[]) => Promise<{ job_id: string } | null>;
  executeFlow: (nodes: Node[], edges: Edge[]) => Promise<{ job_id: string } | null>;
  executeTab: (tabId: string, nodes: Node[], edges: Edge[], tabName?: string) => Promise<{ job_id: string } | null>;
  handleModelChange: (event: React.ChangeEvent<{ value: unknown }>) => void;
  handleRunClick: (type: 'crew' | 'flow') => Promise<void>;
  handleGenerateCrew: () => Promise<void>;
  handleCrewSelect: (crew: Crew) => void;
}

export const useCrewExecutionStore = create<CrewExecutionState>((set, get) => ({
  // Initial state
  isExecuting: false,
  selectedModel: 'databricks-llama-4-maverick',
  planningEnabled: false,
  planningLLM: '',
  reasoningEnabled: false,
  reasoningLLM: '',
  schemaDetectionEnabled: true,
  isCrewPlanningOpen: false,
  isScheduleDialogOpen: false,
  tools: [],
  selectedTools: [],
  jobId: null,
  nodes: [],
  edges: [],
  currentTaskId: null,
  completedTaskIds: [],
  runHistory: [],
  userActive: false,
  errorMessage: '',
  showError: false,
  successMessage: '',
  showSuccess: false,

  // State setters
  setSelectedModel: (model) => set({ selectedModel: model as string }),
  setPlanningEnabled: (enabled) => set({ planningEnabled: enabled }),
  setPlanningLLM: (model) => set({ planningLLM: model }),
  setReasoningEnabled: (enabled) => set({ reasoningEnabled: enabled }),
  setReasoningLLM: (model) => set({ reasoningLLM: model }),
  setSchemaDetectionEnabled: (enabled) => set({ schemaDetectionEnabled: enabled }),
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

  // Execution methods
  executeCrew: async (nodes, edges) => {
    const { selectedModel, planningEnabled, planningLLM, reasoningEnabled, reasoningLLM, schemaDetectionEnabled } = get();
    set({ isExecuting: true });

    try {
      const hasAgentNodes = nodes.some(node => node.type === 'agentNode');
      const hasTaskNodes = nodes.some(node => node.type === 'taskNode');

      if (!hasAgentNodes || !hasTaskNodes) {
        throw new Error('Crew execution requires at least one agent and one task node');
      }

      // Log the task nodes
      console.log('[CrewExecution] Task nodes before execution:', 
        nodes.filter(node => node.type === 'taskNode')
          .map(node => ({ 
            id: node.id, 
            type: node.type, 
            data: { 
              taskId: node.data?.taskId,
              label: node.data?.label 
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

      // Dispatch custom jobCreated event to update the run history immediately
      const jobCreatedEvent = new CustomEvent('jobCreated', { 
        detail: { 
          jobId: response.execution_id || response.job_id,
          jobName: `Crew Execution (${new Date().toLocaleTimeString()})`,
          status: 'running'
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

  executeFlow: async (nodes, edges) => {
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
      
      // Check for flow nodes with expanded criteria to include crewNode
      // Since we now know the canvas uses crewNode type for flows
      const hasFlowNodes = nodes.some(node => 
        node.type === 'flowNode' || 
        node.type === 'crewNode' ||  // Accept crewNode as a valid flow node
        (node.type && node.type.toLowerCase().includes('flow'))
      );

      if (!hasFlowNodes) {
        throw new Error('Flow execution requires at least one flow node on the canvas');
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
        reasoningEnabled
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
          status: 'running'
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
    const { selectedModel, planningEnabled, planningLLM, reasoningEnabled, reasoningLLM, schemaDetectionEnabled } = get();
    set({ isExecuting: true });

    try {
      console.log(`[TabExecution] Executing tab ${tabId} (${tabName || 'Unnamed'}) with ${nodes.length} nodes and ${edges.length} edges`);

      // Determine execution type based on node types
      const hasAgentNodes = nodes.some(node => node.type === 'agentNode');
      const hasTaskNodes = nodes.some(node => node.type === 'taskNode');
      const hasFlowNodes = nodes.some(node => 
        node.type === 'flowNode' || 
        node.type === 'crewNode' ||
        (node.type && node.type.toLowerCase().includes('flow'))
      );

      let executionType: 'crew' | 'flow' = 'crew';
      
      if (hasFlowNodes) {
        executionType = 'flow';
      } else if (!hasAgentNodes || !hasTaskNodes) {
        throw new Error('Tab execution requires at least one agent and one task node for crew execution, or flow nodes for flow execution');
      }

      // Prepare additionalInputs with planning_llm and reasoning_llm if enabled
      const additionalInputs: Record<string, unknown> = {};
      if (planningEnabled && planningLLM) {
        additionalInputs.planning_llm = planningLLM;
      }
      if (reasoningEnabled && reasoningLLM) {
        additionalInputs.reasoning_llm = reasoningLLM;
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
          status: 'running'
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
    set({ isExecuting: true });

    try {
      if (type === 'crew') {
        await state.executeCrew(state.nodes, state.edges);
      } else {
        await state.executeFlow(state.nodes, state.edges);
      }
    } catch (error) {
      set({ 
        errorMessage: error instanceof Error ? error.message : 'Failed to execute',
        showError: true 
      });
    } finally {
      set({ isExecuting: false });
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
          status: 'running'
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

  handleCrewSelect: (crew) => {
    console.log('CrewExecutionStore - Handling crew select:', crew);
    // Add any additional crew selection logic here
  }
})); 