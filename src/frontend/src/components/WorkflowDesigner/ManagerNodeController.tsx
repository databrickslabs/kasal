import React, { useEffect } from 'react';
import { Node, Edge } from 'reactflow';
import PropTypes from 'prop-types';
import { useManagerNode } from '../../hooks/workflow/useManagerNode';

interface ManagerNodeControllerProps {
  nodes: Node[];
  edges: Edge[];
  setNodes: (nodes: Node[] | ((nodes: Node[]) => Node[])) => void;
  setEdges: (edges: Edge[] | ((edges: Edge[]) => Edge[])) => void;
}

/**
 * Component that must be rendered inside ReactFlow to manage manager nodes.
 * This component doesn't render anything visible - it just runs the useManagerNode hook.
 */
const ManagerNodeController: React.FC<ManagerNodeControllerProps> = ({ nodes, edges, setNodes, setEdges }) => {
  useEffect(() => {
    console.log('[ManagerNodeController] Component mounted ✅');
    return () => {
      console.log('[ManagerNodeController] Component unmounted ❌');
    };
  }, []);

  useManagerNode({ nodes, edges, setNodes, setEdges });
  return null;
};

ManagerNodeController.propTypes = {
  nodes: PropTypes.array.isRequired,
  edges: PropTypes.array.isRequired,
  setNodes: PropTypes.func.isRequired,
  setEdges: PropTypes.func.isRequired,
};

export default ManagerNodeController;

