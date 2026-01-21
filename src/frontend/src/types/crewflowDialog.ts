import { Node, Edge } from 'reactflow';
import { FlowConfiguration } from './flow';

import { Agent } from './agent';
import { Task } from './task';

export interface CrewFlowSelectionDialogProps {
  open: boolean;
  onClose: () => void;
  onCrewSelect: (nodes: Node[], edges: Edge[], crewName?: string, crewId?: string) => void;
  onFlowSelect: (nodes: Node[], edges: Edge[], flowConfig?: FlowConfiguration) => void;
  onAgentSelect?: (agents: Agent[]) => void;
  onTaskSelect?: (tasks: Task[]) => void;
  initialTab?: number;
  showOnlyTab?: number; // If set, only show this specific tab (0=Plans, 1=Agents, 2=Tasks, 3=Flows)
  hideFlowsTab?: boolean; // If true, hide the Flows tab from the catalog
} 