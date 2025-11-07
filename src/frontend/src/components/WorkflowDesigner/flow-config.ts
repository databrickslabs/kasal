import AgentNode from '../Agents/AgentNode';
import ManagerNode from '../Agents/ManagerNode';
import TaskNode from '../Tasks/TaskNode';
import AnimatedEdge from '../Common/AnimatedEdge';
import { CrewNode } from '../Flow';
import CrewEdge from '../Flow/CrewEdge';

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