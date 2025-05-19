import AgentNode from '../Agents/AgentNode';
import TaskNode from '../Tasks/TaskNode';
import AnimatedEdge from '../Common/AnimatedEdge';
import { CrewNode } from '../Flow';
import CrewEdge from '../Flow/CrewEdge';

export const nodeTypes = {
  agentNode: AgentNode,
  taskNode: TaskNode,
  crewNode: CrewNode
};

export const edgeTypes = {
  default: AnimatedEdge,
  crewEdge: CrewEdge
}; 