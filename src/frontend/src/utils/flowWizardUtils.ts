import { Node } from 'reactflow';


/**
 * Ensures all nodes have valid position data
 */
export const validateNodePositions = (nodes: Node[]): Node[] => {
  return nodes.map(node => {
    // Check if position exists and has valid x and y coordinates
    if (!node.position || 
        typeof node.position.x !== 'number' || 
        !isFinite(node.position.x) ||
        typeof node.position.y !== 'number' || 
        !isFinite(node.position.y)) {
      
      // Generate a random position within a reasonable range
      return {
        ...node,
        position: {
          x: 100 + Math.random() * 300,
          y: 100 + Math.random() * 200
        }
      };
    }
    
    // Position exists but may have extreme values
    if (Math.abs(node.position.x) > 10000 || Math.abs(node.position.y) > 10000) {
      return {
        ...node,
        position: {
          x: 100 + Math.random() * 300,
          y: 100 + Math.random() * 200
        }
      };
    }
    
    // Position is valid, return node as is
    return node;
  });
};
