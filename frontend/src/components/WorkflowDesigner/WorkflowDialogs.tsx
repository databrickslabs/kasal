import React from 'react';
import { Dialog, DialogContent } from '@mui/material';
import { Node, Edge } from 'reactflow';
import type { CrewResponse as FlowCrewResponse, FlowConfiguration } from '../../types/flow';
import type { CrewResponse as CrewsCrewResponse } from '../../types/crews';
import { Crew } from '../../types/crewPlan';

// Dialog Imports
import AgentDialog from '../Agents/AgentDialog';
import TaskDialog from '../Tasks/TaskDialog';
import CrewPlanningDialog from '../Planning/CrewPlanningDialog';
import { CrewFlowSelectionDialog } from '../Crew/CrewFlowDialog';
import ScheduleDialog from '../Schedule/ScheduleDialog';
import APIKeys from '../Configuration/APIKeys/APIKeys';
import Logs from '../Jobs/LLMLogs';
import Configuration from '../Configuration/Configuration';
import ToolForm from '../Tools/ToolForm';
import { AddFlowDialog } from '../Flow';
import SaveCrew from '../Crew/SaveCrew';

interface WorkflowDialogsProps {
  // Dialog state
  isAgentDialogOpen: boolean;
  setIsAgentDialogOpen: (open: boolean) => void;
  isTaskDialogOpen: boolean;
  setIsTaskDialogOpen: (open: boolean) => void;
  isCrewPlanningOpen: boolean;
  setIsCrewPlanningOpen: (open: boolean) => void;
  isCrewFlowDialogOpen: boolean;
  setIsCrewFlowDialogOpen: (open: boolean) => void;
  isScheduleDialogOpen: boolean;
  setIsScheduleDialogOpen: (open: boolean) => void;
  isAPIKeysDialogOpen: boolean;
  setIsAPIKeysDialogOpen: (open: boolean) => void;
  isToolsDialogOpen: boolean;
  setIsToolsDialogOpen: (open: boolean) => void;
  isLogsDialogOpen: boolean;
  setIsLogsDialogOpen: (open: boolean) => void;
  isConfigurationDialogOpen: boolean;
  setIsConfigurationDialogOpen: (open: boolean) => void;
  isFlowDialogOpen: boolean;
  setIsFlowDialogOpen: (open: boolean) => void;

  // Dialog handlers
  handleAgentSelect: (agent: any) => void;
  handleTaskSelect: (task: any) => void;
  handleGenerateCrew: (plan: Crew, executeAfterGeneration: boolean) => void;
  handleCrewSelect: (crew: any) => void;
  handleFlowSelect: (nodes: Node[], edges: Edge[], flowConfig?: FlowConfiguration) => void;
  onAddCrews: (selectedCrews: CrewsCrewResponse[], positions: { [key: string]: { x: number; y: number } }, flowConfig?: FlowConfiguration, shouldSave?: boolean) => void;

  // Data
  agents: any[];
  tasks: any[];
  nodes: Node[];
  edges: Edge[];
  selectedModel: string;
  tools: any[];
  selectedTools: any[];
  onToolsChange: (tools: any[]) => void;
  planningEnabled: boolean;
  saveCrewRef: React.RefObject<HTMLButtonElement>;

  // Form handlers
  handleShowAgentForm: () => void;
  handleShowTaskForm: () => void;
  fetchAgents: () => Promise<void>;
  fetchTasks: () => Promise<void>;

  // Error handling
  showErrorMessage: (message: string, severity?: 'error' | 'warning' | 'info' | 'success') => void;
}

const WorkflowDialogs: React.FC<WorkflowDialogsProps> = ({
  // Dialog state
  isAgentDialogOpen,
  setIsAgentDialogOpen,
  isTaskDialogOpen,
  setIsTaskDialogOpen,
  isCrewPlanningOpen,
  setIsCrewPlanningOpen,
  isCrewFlowDialogOpen,
  setIsCrewFlowDialogOpen,
  isScheduleDialogOpen,
  setIsScheduleDialogOpen,
  isAPIKeysDialogOpen,
  setIsAPIKeysDialogOpen,
  isToolsDialogOpen,
  setIsToolsDialogOpen,
  isLogsDialogOpen,
  setIsLogsDialogOpen,
  isConfigurationDialogOpen,
  setIsConfigurationDialogOpen,
  isFlowDialogOpen,
  setIsFlowDialogOpen,

  // Dialog handlers
  handleAgentSelect,
  handleTaskSelect,
  handleGenerateCrew,
  handleCrewSelect,
  handleFlowSelect,
  onAddCrews,

  // Data
  agents,
  tasks,
  nodes,
  edges,
  selectedModel,
  tools,
  selectedTools,
  onToolsChange,
  planningEnabled,
  saveCrewRef,

  // Form handlers
  handleShowAgentForm,
  handleShowTaskForm,
  fetchAgents,
  fetchTasks,

  // Error handling
  showErrorMessage,
}) => {
  return (
    <>
      <AgentDialog
        open={isAgentDialogOpen}
        onClose={() => setIsAgentDialogOpen(false)}
        onAgentSelect={handleAgentSelect}
        agents={agents}
        onShowAgentForm={handleShowAgentForm}
        fetchAgents={fetchAgents}
        showErrorMessage={showErrorMessage}
      />

      <TaskDialog
        open={isTaskDialogOpen}
        onClose={() => setIsTaskDialogOpen(false)}
        onTaskSelect={handleTaskSelect}
        tasks={tasks}
        onShowTaskForm={handleShowTaskForm}
        fetchTasks={fetchTasks}
      />

      <CrewPlanningDialog
        open={isCrewPlanningOpen}
        onClose={() => setIsCrewPlanningOpen(false)}
        onGenerateCrew={handleGenerateCrew}
        selectedModel={selectedModel}
        tools={tools}
        selectedTools={selectedTools}
        onToolsChange={onToolsChange}
      />

      <CrewFlowSelectionDialog
        open={isCrewFlowDialogOpen}
        onClose={() => setIsCrewFlowDialogOpen(false)}
        onCrewSelect={handleCrewSelect}
        onFlowSelect={handleFlowSelect}
      />

      <ScheduleDialog
        open={isScheduleDialogOpen}
        onClose={() => setIsScheduleDialogOpen(false)}
        nodes={nodes}
        edges={edges}
        planningEnabled={planningEnabled}
        selectedModel={selectedModel}
      />

      <Dialog
        open={isAPIKeysDialogOpen}
        onClose={() => setIsAPIKeysDialogOpen(false)}
        maxWidth="lg"
        fullWidth
      >
        <DialogContent>
          <APIKeys />
        </DialogContent>
      </Dialog>

      <Dialog
        open={isToolsDialogOpen}
        onClose={() => setIsToolsDialogOpen(false)}
        maxWidth="lg"
        fullWidth
      >
        <DialogContent>
          <ToolForm />
        </DialogContent>
      </Dialog>

      <Dialog
        open={isLogsDialogOpen}
        onClose={() => setIsLogsDialogOpen(false)}
        maxWidth="lg"
        fullWidth
      >
        <DialogContent>
          <Logs />
        </DialogContent>
      </Dialog>

      <Dialog
        open={isConfigurationDialogOpen}
        onClose={() => setIsConfigurationDialogOpen(false)}
        fullWidth
        maxWidth="xl"
        PaperProps={{
          sx: { 
            width: '80vw',
            maxWidth: 'none',
            height: '80vh'
          }
        }}
      >
        <DialogContent sx={{ p: 0 }}>
          <Configuration onClose={() => setIsConfigurationDialogOpen(false)} />
        </DialogContent>
      </Dialog>

      <AddFlowDialog 
        open={isFlowDialogOpen}
        onClose={() => setIsFlowDialogOpen(false)}
        onAddCrews={onAddCrews}
      />
    </>
  );
};

export default WorkflowDialogs; 