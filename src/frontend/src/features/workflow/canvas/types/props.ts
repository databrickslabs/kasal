import type { Node, Viewport } from 'reactflow';
import type React from 'react';
import type { NodeData, AgentNodeData } from '../../../../types/workflow/canvas';

export interface NodeActionsContextType {
  handleAgentEdit: (nodeId: string) => void;
  handleTaskEdit: (nodeId: string) => void;
  handleDeleteNode: (nodeId: string) => void;
}

export interface NodeComponentProps {
  data: NodeData;
  id: string;
}

export interface AgentNodeProps {
  data: AgentNodeData;
  id: string;
}

export interface FlowNodeManagerProps {
  nodes: Node[];
  setNodes: React.Dispatch<React.SetStateAction<Node[]>>;
  viewport?: Viewport;
}

export interface FlowControlsProps {
  onClearCanvas: () => void;
  onGenerateConnections: () => void;
  isHorizontal?: boolean;
  isLeftToRight?: boolean;
  isDarkMode?: boolean;
  onToggleTheme: () => void;
}
