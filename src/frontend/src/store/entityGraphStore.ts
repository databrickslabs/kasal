import { create } from 'zustand';
import ForceGraph2D from 'force-graph';
import * as d3 from 'd3-force';

interface EntityNode {
  id: string;
  name: string;
  type: string;
  attributes: Record<string, any>;
  color?: string;
  size?: number;
  x?: number;
  y?: number;
}

interface EntityLink {
  source: string;
  target: string;
  relationship: string;
}

interface GraphData {
  nodes: EntityNode[];
  links: EntityLink[];
}

interface EntityGraphState {
  // Graph instance
  graphInstance: any | null;
  
  // Data states
  graphData: GraphData;
  filteredGraphData: GraphData;
  loading: boolean;
  error: string | null;
  
  // UI states
  focusedNodeId: string | null;
  selectedNode: EntityNode | null;
  showInferredNodes: boolean;
  deduplicateNodes: boolean;
  showOrphanedNodes: boolean;
  forceStrength: number;
  linkDistance: number;
  linkCurvature: number; // 0 for straight lines, 0.2 for curved
  centerForce: number; // Controls how strongly nodes are pulled to center
  
  // Actions
  initializeGraph: (container: HTMLElement) => void;
  cleanupGraph: () => void;
  setGraphData: (data: GraphData) => void;
  setFilteredGraphData: (data: GraphData) => void;
  setLoading: (loading: boolean) => void;
  setError: (error: string | null) => void;
  setFocusedNode: (nodeId: string | null) => void;
  setSelectedNode: (node: EntityNode | null) => void;
  updateForceParameters: (strength: number, distance: number, centerForce?: number) => void;
  setLinkCurvature: (curvature: number) => void;
  resetFilters: () => void;
  toggleInferredNodes: () => void;
  toggleDeduplication: () => void;
  toggleOrphanedNodes: () => void;
  zoomToFit: () => void;
  zoomIn: () => void;
  zoomOut: () => void;
}

const useEntityGraphStore = create<EntityGraphState>((set, get) => ({
  // Initial state
  graphInstance: null,
  graphData: { nodes: [], links: [] },
  filteredGraphData: { nodes: [], links: [] },
  loading: false,
  error: null,
  focusedNodeId: null,
  selectedNode: null,
  showInferredNodes: true,
  deduplicateNodes: false,
  showOrphanedNodes: false, // Default to hiding orphaned nodes
  forceStrength: -300,
  linkDistance: 100,
  linkCurvature: 0, // Default to straight lines
  centerForce: 0.3, // Default center force

  // Initialize the force graph
  initializeGraph: (container: HTMLElement) => {
    const state = get();
    
    // Clean up existing instance
    if (state.graphInstance) {
      state.cleanupGraph();
    }

    console.log('[EntityGraphStore] Initializing force graph');
    
    try {
      // Create force graph instance
      const ForceGraphFactory = ForceGraph2D as any;
      const graph = ForceGraphFactory()(container)
        .backgroundColor('#fafafa')
        .nodeId('id')
        .nodeLabel((node: any) => `
          <div style="background: rgba(0,0,0,0.8); color: white; padding: 8px; border-radius: 4px; max-width: 200px;">
            <div style="font-weight: bold; margin-bottom: 4px;">${node.name}</div>
            <div style="font-size: 12px; color: #ccc; margin-bottom: 4px;">Type: ${node.type}</div>
            ${Object.entries(node.attributes || {}).length > 0 ? 
              '<div style="font-size: 11px;">' + 
              Object.entries(node.attributes || {}).slice(0, 3).map(([key, value]) => 
                `${key}: ${String(value)}`
              ).join('<br/>') + 
              '</div>' : ''}
          </div>
        `)
        .nodeCanvasObject((node: any, ctx: any, globalScale: number) => {
          const nodeSize = node.size || 5;
          
          // Draw node circle
          ctx.beginPath();
          ctx.arc(node.x, node.y, nodeSize, 0, 2 * Math.PI, false);
          ctx.fillStyle = node.color;
          ctx.fill();
          ctx.strokeStyle = 'rgba(0,0,0,0.3)';
          ctx.lineWidth = 2;
          ctx.stroke();
          
          // Draw node label
          const label = node.name || node.id || 'Unknown';
          const fontSize = 14;
          ctx.font = `bold ${fontSize}px Arial`;
          ctx.textAlign = 'center';
          ctx.textBaseline = 'top';
          
          // Measure text for background
          const textMetrics = ctx.measureText(label);
          const textWidth = textMetrics.width;
          const textHeight = fontSize * 1.2;
          
          // Draw text background
          const padding = 4;
          const bgY = node.y + nodeSize + 3;
          ctx.fillStyle = 'rgba(255, 255, 255, 0.95)';
          ctx.fillRect(
            node.x - (textWidth + padding) / 2,
            bgY,
            textWidth + padding,
            textHeight
          );
          
          // Draw border around text background
          ctx.strokeStyle = 'rgba(0,0,0,0.1)';
          ctx.lineWidth = 1;
          ctx.strokeRect(
            node.x - (textWidth + padding) / 2,
            bgY,
            textWidth + padding,
            textHeight
          );
          
          // Draw text
          ctx.fillStyle = '#333';
          ctx.fillText(label, node.x, bgY + padding / 2);
        })
        .linkWidth(2)
        .linkColor(() => 'rgba(100, 100, 100, 0.4)')
        .linkDirectionalArrowLength(6)
        .linkDirectionalArrowRelPos(1)
        .linkCurvature(state.linkCurvature)
        .onNodeClick((node: any) => {
          console.log('[EntityGraphStore] Node clicked:', node);
          set({ selectedNode: node });
          // Don't automatically focus, let user decide via button
        })
        .onBackgroundClick(() => {
          console.log('[EntityGraphStore] Background clicked, clearing selection');
          set({ selectedNode: null, focusedNodeId: null });
        });

      // Configure force simulation
      graph.d3Force('charge')?.strength(state.forceStrength);
      graph.d3Force('link')?.distance(state.linkDistance);
      graph.d3Force('center')?.strength(state.centerForce);
      graph.d3Force('collide')?.radius(25).strength(1.2);
      
      // Set x/y forces based on centerForce
      const width = container.offsetWidth;
      const height = container.offsetHeight;
      const centerX = width / 2;
      const centerY = height / 2;
      
      if (state.centerForce < 0.3) {
        // Minimal centering for spread mode
        graph.d3Force('x', null);
        graph.d3Force('y', null);
      } else if (state.centerForce < 0.7) {
        // Moderate centering for balanced mode
        graph.d3Force('x', d3.forceX(centerX).strength(0.1));
        graph.d3Force('y', d3.forceY(centerY).strength(0.1));
      } else {
        // Strong centering for compact mode
        graph.d3Force('x', d3.forceX(centerX).strength(0.5));
        graph.d3Force('y', d3.forceY(centerY).strength(0.5));
      }

      // Set the instance
      set({ graphInstance: graph });
      
      // If we have data, set it
      const { filteredGraphData } = get();
      if (filteredGraphData.nodes.length > 0) {
        console.log('[EntityGraphStore] Setting initial graph data with', filteredGraphData.nodes.length, 'nodes');
        graph.graphData(filteredGraphData);
        
        // Zoom to fit after a short delay
        setTimeout(() => {
          graph.zoomToFit(400, 50);
        }, 500);
      } else {
        console.log('[EntityGraphStore] No initial data to set');
        // Set empty data to initialize the graph
        graph.graphData({ nodes: [], links: [] });
      }
    } catch (error) {
      console.error('[EntityGraphStore] Error initializing graph:', error);
      set({ error: 'Failed to initialize graph visualization' });
    }
  },

  // Cleanup graph instance
  cleanupGraph: () => {
    const { graphInstance } = get();
    if (graphInstance) {
      console.log('[EntityGraphStore] Cleaning up graph instance');
      try {
        if (typeof graphInstance._destructor === 'function') {
          graphInstance._destructor();
        }
      } catch (err) {
        console.error('[EntityGraphStore] Error during cleanup:', err);
      }
      set({ graphInstance: null });
    }
  },

  // Set graph data
  setGraphData: (data: GraphData) => {
    set({ graphData: data });
  },

  // Set filtered graph data and update visualization
  setFilteredGraphData: (data: GraphData) => {
    const { graphInstance } = get();
    set({ filteredGraphData: data });
    
    if (graphInstance && data.nodes.length > 0) {
      console.log('[EntityGraphStore] Updating graph with', data.nodes.length, 'nodes');
      graphInstance.graphData(data);
    }
  },

  // Set loading state
  setLoading: (loading: boolean) => {
    set({ loading });
  },

  // Set error state
  setError: (error: string | null) => {
    set({ error });
  },

  // Set focused node
  setFocusedNode: (nodeId: string | null) => {
    set({ focusedNodeId: nodeId });
  },

  // Set selected node
  setSelectedNode: (node: EntityNode | null) => {
    set({ selectedNode: node });
  },

  // Update force parameters
  updateForceParameters: (strength: number, distance: number, centerForce?: number) => {
    const { graphInstance } = get();
    const previousCenterForce = get().centerForce;
    const newCenterForce = centerForce !== undefined ? centerForce : previousCenterForce;
    
    // Update state
    set({ forceStrength: strength, linkDistance: distance, centerForce: newCenterForce });
    
    if (graphInstance) {
      // Update forces
      graphInstance.d3Force('charge')?.strength(strength);
      graphInstance.d3Force('link')?.distance(distance);
      
      // Update center and position forces based on centerForce value
      if (centerForce !== undefined) {
        const width = graphInstance.width();
        const height = graphInstance.height();
        const centerX = width / 2;
        const centerY = height / 2;
        
        // Adjust center force
        graphInstance.d3Force('center')?.strength(newCenterForce);
        
        // For spread mode (low values), weaken or remove position forces
        if (newCenterForce < 0.3) {
          // Weak or no position forces for spread mode
          graphInstance.d3Force('x', null);
          graphInstance.d3Force('y', null);
          graphInstance.d3Force('center')?.strength(0.01);
        } 
        // For balanced mode
        else if (newCenterForce < 0.7) {
          // Moderate position forces
          graphInstance.d3Force('x', d3.forceX(centerX).strength(0.1));
          graphInstance.d3Force('y', d3.forceY(centerY).strength(0.1));
        } 
        // For compact mode (high values), use strong position forces
        else {
          // Create strong forceX and forceY to pull to center
          graphInstance.d3Force('x', d3.forceX(centerX).strength(0.5));
          graphInstance.d3Force('y', d3.forceY(centerY).strength(0.5));
          
          // Also reduce charge force to allow nodes to come closer
          graphInstance.d3Force('charge')?.strength(-100);
        }
        
        // Reheat simulation very aggressively for compact mode
        const alphaTarget = newCenterForce > 0.7 ? 1 : 0.8;
        graphInstance.d3ReheatSimulation(alphaTarget);
        
        // Reset charge force if not in compact mode
        if (newCenterForce < 0.7) {
          graphInstance.d3Force('charge')?.strength(strength);
        }
      } else {
        // Regular force updates (when not changing cluster spacing)
        graphInstance.d3ReheatSimulation(0.3);
      }
    }
  },

  // Set link curvature
  setLinkCurvature: (curvature: number) => {
    const { graphInstance } = get();
    set({ linkCurvature: curvature });
    
    if (graphInstance) {
      graphInstance.linkCurvature(curvature);
    }
  },

  // Reset all filters
  resetFilters: () => {
    set({ 
      focusedNodeId: null, 
      selectedNode: null,
      showInferredNodes: true,
      deduplicateNodes: false 
    });
  },

  // Toggle inferred nodes
  toggleInferredNodes: () => {
    set((state) => ({ showInferredNodes: !state.showInferredNodes }));
  },

  // Toggle deduplication
  toggleDeduplication: () => {
    set((state) => ({ deduplicateNodes: !state.deduplicateNodes }));
  },

  // Toggle orphaned nodes
  toggleOrphanedNodes: () => {
    set((state) => ({ showOrphanedNodes: !state.showOrphanedNodes }));
  },

  // Zoom controls
  zoomToFit: () => {
    const { graphInstance } = get();
    if (graphInstance) {
      graphInstance.zoomToFit(400, 50);
    }
  },

  zoomIn: () => {
    const { graphInstance } = get();
    if (graphInstance) {
      const currentZoom = graphInstance.zoom();
      graphInstance.zoom(currentZoom * 1.2, 300);
    }
  },

  zoomOut: () => {
    const { graphInstance } = get();
    if (graphInstance) {
      const currentZoom = graphInstance.zoom();
      graphInstance.zoom(currentZoom * 0.8, 300);
    }
  },
}));

export default useEntityGraphStore;