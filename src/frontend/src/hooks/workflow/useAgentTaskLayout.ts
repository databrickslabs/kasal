import { useEffect, useRef } from 'react';
import type { Node, Edge } from 'reactflow';
import { useUILayoutStore } from '../../store/uiLayout';
import { layoutAgentTaskGroups } from '../../features/workflow/canvas/lib/layoutAgentTaskGroups';

/** Reflow changed groups after creation/measurement, without reacting to dragging. */
export function useAgentTaskLayout(nodes: Node[], edges: Edge[], setNodes: (updater: (nodes: Node[]) => Node[]) => void) {
  const orientation = useUILayoutStore(state => state.layoutOrientation) || 'horizontal';
  const previous = useRef('');
  useEffect(() => {
    const agents = new Set(nodes.filter(node => node.type === 'agentNode').map(node => node.id));
    const tasks = new Set(nodes.filter(node => node.type === 'taskNode').map(node => node.id));
    const links = edges.filter(edge => agents.has(edge.source) && tasks.has(edge.target));
    const signature = JSON.stringify([orientation, nodes.map(node => [node.id, node.type, node.width, node.height, node.hidden]), links.map(edge => [edge.source, edge.target])]);
    if (signature === previous.current) return;
    previous.current = signature;
    if (!links.length) return;
    const relevant = nodes.filter(node => agents.has(node.id) || tasks.has(node.id));
    const origin = { x: Math.min(...relevant.map(node => node.position.x)), y: Math.min(...relevant.map(node => node.position.y)) };
    setNodes(current => {
      const arranged = layoutAgentTaskGroups(current, links, orientation, origin);
      return arranged.some((node, i) => node !== current[i]) ? arranged : current;
    });
    // Fit only after the reflow has reached React Flow's internal node store.
    const frame = requestAnimationFrame(() => window.dispatchEvent(new Event('fitViewToNodesInternal')));
    return () => cancelAnimationFrame(frame);
  }, [nodes, edges, orientation, setNodes]);
}
