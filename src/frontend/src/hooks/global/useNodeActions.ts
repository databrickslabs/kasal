import { useNodeActionsStore } from '../../store/nodeActions';

export const useNodeActions = () => {
  const {
    handleAgentEdit,
    handleTaskEdit,
    handleDeleteNode,
    setHandleAgentEdit,
    setHandleTaskEdit,
    setHandleDeleteNode
  } = useNodeActionsStore();

  return {
    handleAgentEdit,
    handleTaskEdit,
    handleDeleteNode,
    setHandleAgentEdit,
    setHandleTaskEdit,
    setHandleDeleteNode
  };
}; 