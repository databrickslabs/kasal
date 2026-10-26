import AgentNode from '../../agents/components/AgentNode';
import ManagerNode from '../../agents/components/ManagerNode';
import TaskNode from '../../tasks/components/TaskNode';
import AnimatedEdge from './AnimatedEdge';
import { CrewNode } from '../../flows/components/index';
import CrewEdge from '../../flows/components/CrewEdge';

export const nodeTypes = {
  agentNode: AgentNode,
  managerNode: ManagerNode,
  taskNode: TaskNode,
  crewNode: CrewNode
};

export const edgeTypes = {
  default: AnimatedEdge,
  crewEdge: CrewEdge
};