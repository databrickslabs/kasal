import { Node, Edge } from 'reactflow';
import { TaskNode, AgentNode } from './crew';

export interface CrewResponse {
  id: string;
  name: string;
  agent_ids: string[];
  task_ids: string[];
  nodes?: Node[];
  edges?: Edge[];
  tasks?: TaskNode[];
  agents?: AgentNode[];
  created_at: string;
  updated_at: string;
}

export interface CrewSelectionDialogProps {
  open: boolean;
  onClose: () => void;
  onCrewSelect: (nodes: Node[], edges: Edge[]) => void;
}

export interface SaveCrewProps {
  nodes: Node[];
  edges: Edge[];
  trigger: React.ReactElement;
}

export interface CrewCreate {
  name: string;
  agent_ids: string[];
  task_ids: string[];
  nodes: Node[];
  edges: Edge[];
}

export interface Crew {
  id: string;
  name: string;
  agent_ids: string[];
  task_ids: string[];
  nodes: Node[];
  edges: Edge[];
  created_at: string;
  updated_at: string;
}

export interface CrewSaveData {
  name: string;
  nodes: Node[];
  edges: Edge[];
  agent_ids?: string[];
  task_ids?: string[];
} 