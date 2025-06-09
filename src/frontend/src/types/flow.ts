import { Node, Edge } from 'reactflow';
import { ConditionFormData } from '../components/Flow/ConditionForm';
import { CrewTask } from './crewPlan';

export interface FlowResponse extends Flow {
  flow_config?: FlowConfiguration;
}

export interface FlowCreate {
  name: string;
  crew_id: string;
  nodes: Node[];
  edges: Edge[];
  flow_config?: FlowConfiguration;
}

export interface Flow {
  id: string;
  name: string;
  crew_id: string;
  nodes: Node[];
  edges: Edge[];
  created_at: string;
  updated_at: string;
  flow_config?: FlowConfiguration;
  flowConfig?: FlowConfiguration;
}

export interface FlowSaveData {
  name: string;
  crew_id: string;
  nodes: Node[];
  edges: Edge[];
  flowConfig?: FlowConfiguration;
}

export interface FlowSelectionDialogProps {
  open: boolean;
  onClose: () => void;
  onFlowSelect: (nodes: Node[], edges: Edge[], flowConfig?: FlowConfiguration) => void;
}

export interface SaveFlowProps {
  nodes: Node[];
  edges: Edge[];
  trigger: React.ReactElement;
  flowConfig?: FlowConfiguration;
}

export interface FlowFormData {
  name: string;
  crewName: string;
  crewRef?: string;
  type: 'start' | 'normal' | 'router' | 'listen';
  listenTo?: string[];
  conditionType?: 'and' | 'or' | 'router';
  routerCondition?: string;
  taskRef?: string;
  conditionData?: ConditionFormData;
}

export interface FlowEdgeFormData {
  stateType: 'structured' | 'unstructured';
  stateDefinition: string;
  stateData: Record<string, unknown>;
}

export enum WizardStep {
  SelectCrews = 0,
  DefineStartingPoints = 1,
  ConfigureListeners = 2,
  ConfigureState = 3,
  Review = 4,
}

export interface Listener {
  id: string;
  name: string;
  listenToTaskIds: string[];
  listenToTaskNames: string[];
  tasks: CrewTask[];
  state: {
    stateType: 'structured' | 'unstructured';
    stateDefinition: string;
    stateData: Record<string, unknown>;
  };
  conditionType: 'NONE' | 'AND' | 'OR' | 'ROUTER';
  crewId: string;
  crewName: string;
  waitForAll?: boolean;
  routerConfig?: {
    defaultRoute: string;
    routes: Array<{
      name: string;
      condition: string;
      taskIds: string[];
    }>;
  };
}

export interface Action {
  id: string;
  crewId: string;
  crewName: string;
  taskId: string;
  taskName: string;
}

export interface StartingPoint {
  crewId: string;
  taskId: string;
  isStartPoint: boolean;
  taskName: string;
  crewName: string;
}

export interface FlowConfiguration {
  id: string;
  name: string;
  type?: string;
  crewName?: string;
  crewRef?: string;
  listeners: Listener[];
  actions: Action[];
  startingPoints: StartingPoint[];
}

export interface CrewResponse {
  id: string;
  name: string;
  description?: string;
  agent_ids?: string[];
  task_ids?: string[];
  nodes?: Node[];
  edges?: Edge[];
  tasks?: CrewTask[];
  agents?: Array<{
    id: string;
    name: string;
    role?: string;
    goals?: string[];
  }>;
  created_at?: string;
  updated_at?: string;
  config?: Record<string, unknown>;
} 