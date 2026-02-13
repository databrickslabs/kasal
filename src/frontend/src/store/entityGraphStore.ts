import { create } from 'zustand';
import ForceGraph2D from 'force-graph';
import * as d3 from 'd3-force';
import Logger from '../utils/logger';
import {
  getEntityColor,
  getRelationshipColor,
  lightenColor,
  darkenColor,
} from '../utils/entityColors';

const logger = new Logger({ prefix: 'EntityGraph' });

// Type definitions for ForceGraph2D instance (library lacks proper types)
interface ForceGraphInstance {
  backgroundColor: (color: string) => ForceGraphInstance;
  nodeId: (id: string) => ForceGraphInstance;
  nodeLabel: (fn: (node: EntityNode) => string) => ForceGraphInstance;
  nodeCanvasObject: (fn: (node: EntityNode, ctx: CanvasRenderingContext2D, scale: number) => void) => ForceGraphInstance;
  nodePointerAreaPaint: (fn: (node: EntityNode, color: string, ctx: CanvasRenderingContext2D) => void) => ForceGraphInstance;
  linkWidth: (fn: number | ((link: EntityLink) => number)) => ForceGraphInstance;
  linkColor: (fn: (link: EntityLink) => string) => ForceGraphInstance;
  linkDirectionalArrowLength: (length: number) => ForceGraphInstance;
  linkDirectionalArrowRelPos: (pos: number) => ForceGraphInstance;
  linkDirectionalArrowColor: (fn: (link: EntityLink) => string) => ForceGraphInstance;
  linkDirectionalParticles: (n: number | ((link: EntityLink) => number)) => ForceGraphInstance;
  linkDirectionalParticleSpeed: (speed: number) => ForceGraphInstance;
  linkDirectionalParticleWidth: (width: number) => ForceGraphInstance;
  linkDirectionalParticleColor: (fn: (link: EntityLink) => string) => ForceGraphInstance;
  linkCanvasObject: (fn: (link: EntityLink, ctx: CanvasRenderingContext2D, scale: number) => void) => ForceGraphInstance;
  linkCanvasObjectMode: (mode: string | (() => string)) => ForceGraphInstance;
  linkCurvature: (curvature: number) => ForceGraphInstance;
  onNodeClick: (fn: (node: EntityNode) => void) => ForceGraphInstance;
  onNodeHover: (fn: (node: EntityNode | null) => void) => ForceGraphInstance;
  onBackgroundClick: (fn: () => void) => ForceGraphInstance;
  centerAt: (x: number, y: number, ms: number) => ForceGraphInstance;
  d3Force: (name: string, force?: d3.Force<d3.SimulationNodeDatum, undefined> | null) => { strength: (s: number) => void; distance?: (d: number) => void; radius?: (r: number) => { strength: (s: number) => void } } | undefined;
  graphData: (data: GraphData) => void;
  zoomToFit: (duration: number, padding: number) => void;
  zoom: (level?: number, duration?: number) => number;
  width: () => number;
  height: () => number;
  d3ReheatSimulation: (alpha: number) => void;
  _destructor?: () => void;
}

export interface EntityNode {
  id: string;
  name: string;
  type: string;
  attributes: Record<string, unknown>;
  color?: string;
  size?: number;
  x?: number;
  y?: number;
}

export interface EntityLink {
  source: string | EntityNode;
  target: string | EntityNode;
  relationship: string;
}

export interface GraphData {
  nodes: EntityNode[];
  links: EntityLink[];
}

// Double-click detection state (module-level)
let lastClickTime = 0;
let lastClickNodeId: string | null = null;

// Helper to get link key for highlight lookup
function getLinkKey(link: EntityLink): string {
  const sourceId = typeof link.source === 'object' ? (link.source as EntityNode).id : link.source;
  const targetId = typeof link.target === 'object' ? (link.target as EntityNode).id : link.target;
  return `${sourceId}--${targetId}`;
}

// Helper to compute degree (connection count) for a node
function computeNodeDegree(nodeId: string, links: EntityLink[]): number {
  let count = 0;
  for (const link of links) {
    const sourceId = typeof link.source === 'object' ? (link.source as EntityNode).id : link.source;
    const targetId = typeof link.target === 'object' ? (link.target as EntityNode).id : link.target;
    if (sourceId === nodeId || targetId === nodeId) count++;
  }
  return count;
}

interface EntityGraphState {
  // Graph instance
  graphInstance: unknown | null;

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
  linkCurvature: number;
  centerForce: number;

  // Hover / highlight states
  hoveredNodeId: string | null;
  highlightedNodeIds: Set<string>;
  highlightedLinkKeys: Set<string>;

  // Entity type visibility
  hiddenEntityTypes: Set<string>;

  // Panel collapse states
  controlsPanelCollapsed: boolean;
  legendPanelCollapsed: boolean;

  // Dark mode
  isDarkMode: boolean;

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
  setHoveredNode: (nodeId: string | null) => void;
  toggleEntityTypeVisibility: (type: string) => void;
  setControlsPanelCollapsed: (collapsed: boolean) => void;
  setLegendPanelCollapsed: (collapsed: boolean) => void;
  setIsDarkMode: (dark: boolean) => void;
  centerOnNode: (nodeId: string) => void;
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
  showOrphanedNodes: false,
  forceStrength: -300,
  linkDistance: 100,
  linkCurvature: 0,
  centerForce: 0.3,

  // New states
  hoveredNodeId: null,
  highlightedNodeIds: new Set<string>(),
  highlightedLinkKeys: new Set<string>(),
  hiddenEntityTypes: new Set<string>(),
  controlsPanelCollapsed: false,
  legendPanelCollapsed: false,
  isDarkMode: false,

  // Initialize the force graph
  initializeGraph: (container: HTMLElement) => {
    const state = get();

    if (state.graphInstance) {
      state.cleanupGraph();
    }

    logger.debug('Initializing force graph');

    try {
      const ForceGraphFactory = ForceGraph2D as unknown as () => (container: HTMLElement) => ForceGraphInstance;
      const isDark = state.isDarkMode;

      const graph = ForceGraphFactory()(container)
        .backgroundColor(isDark ? '#1a1a2e' : '#fafafa')
        .nodeId('id')
        .nodeLabel((node: EntityNode) => `
          <div style="background: rgba(0,0,0,0.85); color: white; padding: 8px 12px; border-radius: 6px; max-width: 220px; font-family: Arial, sans-serif;">
            <div style="font-weight: bold; margin-bottom: 4px; font-size: 13px;">${node.name}</div>
            <div style="font-size: 11px; color: #aaa; margin-bottom: 4px;">Type: ${node.type}</div>
            ${Object.entries(node.attributes || {}).length > 0
            ? '<div style="font-size: 10px; color: #ccc;">' +
              Object.entries(node.attributes || {}).slice(0, 3).map(([key, value]) =>
                `${key}: ${String(value)}`
              ).join('<br/>') +
              '</div>' : ''}
          </div>
        `)
        .nodeCanvasObject((node: EntityNode, ctx: CanvasRenderingContext2D, globalScale: number) => {
          const { hoveredNodeId: hovered, highlightedNodeIds: hlNodes, selectedNode: selected, filteredGraphData: fData, isDarkMode: dark } = get();
          const x = node.x ?? 0;
          const y = node.y ?? 0;
          const baseColor = node.color || getEntityColor(node.type);

          // Dynamic sizing based on degree centrality
          const degree = computeNodeDegree(node.id, fData.links);
          const nodeSize = Math.max(5, Math.min(20, 4 + degree * 2));

          // Hover dimming: if something is hovered and this node is NOT highlighted
          const isHoverActive = hovered !== null;
          const isHighlighted = hlNodes.has(node.id);
          const isSelected = selected?.id === node.id;

          if (isHoverActive && !isHighlighted) {
            ctx.globalAlpha = 0.15;
          }

          // Radial gradient fill
          const gradient = ctx.createRadialGradient(x - nodeSize * 0.3, y - nodeSize * 0.3, nodeSize * 0.1, x, y, nodeSize);
          gradient.addColorStop(0, lightenColor(baseColor, 0.3));
          gradient.addColorStop(1, darkenColor(baseColor, 0.15));

          // Selected/focused glow ring
          if (isSelected || (isHoverActive && node.id === hovered)) {
            ctx.save();
            ctx.shadowColor = baseColor;
            ctx.shadowBlur = 15;
            ctx.beginPath();
            ctx.arc(x, y, nodeSize + 3, 0, 2 * Math.PI, false);
            ctx.fillStyle = 'rgba(255,255,255,0.01)';
            ctx.fill();
            ctx.restore();
          }

          // Draw node circle with gradient
          ctx.beginPath();
          ctx.arc(x, y, nodeSize, 0, 2 * Math.PI, false);
          ctx.fillStyle = gradient;
          ctx.fill();
          ctx.strokeStyle = darkenColor(baseColor, 0.2);
          ctx.lineWidth = 1.5;
          ctx.stroke();

          // Zoom-aware labels: only render when zoomed in enough
          if (globalScale > 0.4) {
            const label = node.name || node.id || 'Unknown';
            const fontSize = Math.max(10, 14 / globalScale);
            ctx.font = `600 ${fontSize}px -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif`;
            ctx.textAlign = 'center';
            ctx.textBaseline = 'top';

            const textMetrics = ctx.measureText(label);
            const textWidth = textMetrics.width;
            const textHeight = fontSize * 1.2;
            const padding = 6;
            const bgY = y + nodeSize + 4;
            const radius = 4;

            // Rounded label background
            const bgX = x - (textWidth + padding) / 2;
            const bgW = textWidth + padding;
            const bgH = textHeight + 2;
            ctx.fillStyle = dark ? 'rgba(30, 30, 50, 0.92)' : 'rgba(255, 255, 255, 0.95)';
            ctx.beginPath();
            if (ctx.roundRect) {
              ctx.roundRect(bgX, bgY, bgW, bgH, radius);
            } else {
              // Fallback for older browsers
              ctx.moveTo(bgX + radius, bgY);
              ctx.lineTo(bgX + bgW - radius, bgY);
              ctx.quadraticCurveTo(bgX + bgW, bgY, bgX + bgW, bgY + radius);
              ctx.lineTo(bgX + bgW, bgY + bgH - radius);
              ctx.quadraticCurveTo(bgX + bgW, bgY + bgH, bgX + bgW - radius, bgY + bgH);
              ctx.lineTo(bgX + radius, bgY + bgH);
              ctx.quadraticCurveTo(bgX, bgY + bgH, bgX, bgY + bgH - radius);
              ctx.lineTo(bgX, bgY + radius);
              ctx.quadraticCurveTo(bgX, bgY, bgX + radius, bgY);
            }
            ctx.fill();

            ctx.strokeStyle = dark ? 'rgba(255,255,255,0.1)' : 'rgba(0,0,0,0.08)';
            ctx.lineWidth = 0.5;
            ctx.stroke();

            // Draw text
            ctx.fillStyle = dark ? '#e0e0e0' : '#333';
            ctx.fillText(label, x, bgY + padding / 2);
          }

          // Restore alpha
          ctx.globalAlpha = 1;
        })
        .nodePointerAreaPaint((node: EntityNode, color: string, ctx: CanvasRenderingContext2D) => {
          const { filteredGraphData: fData } = get();
          const degree = computeNodeDegree(node.id, fData.links);
          const nodeSize = Math.max(5, Math.min(20, 4 + degree * 2));
          const x = node.x ?? 0;
          const y = node.y ?? 0;
          ctx.fillStyle = color;
          ctx.beginPath();
          ctx.arc(x, y, nodeSize + 2, 0, 2 * Math.PI, false);
          ctx.fill();
        })
        // Link rendering
        .linkWidth((link: EntityLink) => {
          const { highlightedLinkKeys: hlLinks, hoveredNodeId: hovered } = get();
          if (hovered === null) return 1.5;
          return hlLinks.has(getLinkKey(link)) ? 3 : 0.5;
        })
        .linkColor((link: EntityLink) => {
          const { highlightedLinkKeys: hlLinks, hoveredNodeId: hovered } = get();
          const color = getRelationshipColor(link.relationship);
          if (hovered === null) return color + 'AA';
          return hlLinks.has(getLinkKey(link)) ? color : color + '15';
        })
        .linkDirectionalArrowLength(8)
        .linkDirectionalArrowRelPos(1)
        .linkDirectionalArrowColor((link: EntityLink) => {
          const { highlightedLinkKeys: hlLinks, hoveredNodeId: hovered } = get();
          const color = getRelationshipColor(link.relationship);
          if (hovered === null) return color;
          return hlLinks.has(getLinkKey(link)) ? color : color + '15';
        })
        // Animated directional particles
        .linkDirectionalParticles(2)
        .linkDirectionalParticleSpeed(0.004)
        .linkDirectionalParticleWidth(3)
        .linkDirectionalParticleColor((link: EntityLink) => getRelationshipColor(link.relationship))
        // Relationship labels on links
        .linkCanvasObjectMode(() => 'after')
        .linkCanvasObject((link: EntityLink, ctx: CanvasRenderingContext2D, globalScale: number) => {
          // Only show labels when sufficiently zoomed in
          if (globalScale < 0.8) return;

          const { highlightedLinkKeys: hlLinks, hoveredNodeId: hovered, isDarkMode: dark } = get();
          const isHighlighted = hlLinks.has(getLinkKey(link));

          // During hover, only show labels on highlighted links
          if (hovered !== null && !isHighlighted) return;

          const source = link.source as EntityNode;
          const target = link.target as EntityNode;
          if (!source.x || !target.x) return;

          const midX = (source.x + target.x) / 2;
          const midY = ((source.y ?? 0) + (target.y ?? 0)) / 2;

          const relLabel = link.relationship || 'related_to';
          const fontSize = Math.max(8, 10 / globalScale);
          ctx.font = `${fontSize}px -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif`;
          ctx.textAlign = 'center';
          ctx.textBaseline = 'middle';

          const textWidth = ctx.measureText(relLabel).width;
          const padding = 4;
          const pillW = textWidth + padding * 2;
          const pillH = fontSize + padding;
          const radius = pillH / 2;

          // Rounded pill background
          const bgX = midX - pillW / 2;
          const bgY = midY - pillH / 2;
          ctx.fillStyle = dark ? 'rgba(30, 30, 50, 0.85)' : 'rgba(255, 255, 255, 0.9)';
          ctx.beginPath();
          if (ctx.roundRect) {
            ctx.roundRect(bgX, bgY, pillW, pillH, radius);
          } else {
            ctx.moveTo(bgX + radius, bgY);
            ctx.lineTo(bgX + pillW - radius, bgY);
            ctx.quadraticCurveTo(bgX + pillW, bgY, bgX + pillW, bgY + radius);
            ctx.lineTo(bgX + pillW, bgY + pillH - radius);
            ctx.quadraticCurveTo(bgX + pillW, bgY + pillH, bgX + pillW - radius, bgY + pillH);
            ctx.lineTo(bgX + radius, bgY + pillH);
            ctx.quadraticCurveTo(bgX, bgY + pillH, bgX, bgY + pillH - radius);
            ctx.lineTo(bgX, bgY + radius);
            ctx.quadraticCurveTo(bgX, bgY, bgX + radius, bgY);
          }
          ctx.fill();

          const relColor = getRelationshipColor(relLabel);
          ctx.strokeStyle = relColor + '60';
          ctx.lineWidth = 0.5;
          ctx.stroke();

          ctx.fillStyle = dark ? '#ccc' : '#555';
          ctx.fillText(relLabel, midX, midY);
        })
        .linkCurvature(state.linkCurvature)
        .onNodeHover((node: EntityNode | null) => {
          get().setHoveredNode(node?.id ?? null);
        })
        .onNodeClick((node: EntityNode) => {
          const now = Date.now();
          const nodeId = node.id;

          // Double-click detection
          if (lastClickNodeId === nodeId && now - lastClickTime < 350) {
            // Double-click: focus + camera pan
            logger.debug('Double-click on node, focusing:', nodeId);
            set({ selectedNode: node, focusedNodeId: nodeId });
            const instance = get().graphInstance as ForceGraphInstance | null;
            if (instance && node.x !== undefined && node.y !== undefined) {
              instance.centerAt(node.x, node.y, 500);
              instance.zoom(2, 500);
            }
            lastClickTime = 0;
            lastClickNodeId = null;
          } else {
            // Single click: select node
            logger.debug('Node clicked:', nodeId);
            set({ selectedNode: node });
            lastClickTime = now;
            lastClickNodeId = nodeId;
          }
        })
        .onBackgroundClick(() => {
          logger.debug('Background clicked, clearing selection');
          set({ selectedNode: null, focusedNodeId: null });
        });

      // Configure force simulation
      graph.d3Force('charge')?.strength(state.forceStrength);
      graph.d3Force('link')?.distance?.(state.linkDistance);
      graph.d3Force('center')?.strength(state.centerForce);
      graph.d3Force('collide')?.radius?.(25)?.strength(1.2);

      const width = container.offsetWidth;
      const height = container.offsetHeight;
      const centerX = width / 2;
      const centerY = height / 2;

      if (state.centerForce < 0.3) {
        graph.d3Force('x', null);
        graph.d3Force('y', null);
      } else if (state.centerForce < 0.7) {
        graph.d3Force('x', d3.forceX(centerX).strength(0.1));
        graph.d3Force('y', d3.forceY(centerY).strength(0.1));
      } else {
        graph.d3Force('x', d3.forceX(centerX).strength(0.5));
        graph.d3Force('y', d3.forceY(centerY).strength(0.5));
      }

      set({ graphInstance: graph });

      const { filteredGraphData } = get();
      if (filteredGraphData.nodes.length > 0) {
        logger.debug('Setting initial graph data with', filteredGraphData.nodes.length, 'nodes');
        graph.graphData(filteredGraphData);
        setTimeout(() => {
          graph.zoomToFit(400, 50);
        }, 500);
      } else {
        logger.debug('No initial data to set');
        graph.graphData({ nodes: [], links: [] });
      }
    } catch (err) {
      logger.error('Error initializing graph:', err);
      set({ error: 'Failed to initialize graph visualization' });
    }
  },

  // Cleanup graph instance
  cleanupGraph: () => {
    const { graphInstance } = get();
    if (graphInstance) {
      logger.debug('Cleaning up graph instance');
      try {
        const instance = graphInstance as ForceGraphInstance;
        if (typeof instance._destructor === 'function') {
          instance._destructor();
        }
      } catch (err) {
        logger.error('Error during cleanup:', err);
      }
      set({ graphInstance: null });
    }
  },

  setGraphData: (data: GraphData) => {
    set({ graphData: data });
  },

  setFilteredGraphData: (data: GraphData) => {
    const { graphInstance } = get();
    set({ filteredGraphData: data });

    if (graphInstance && data.nodes.length > 0) {
      logger.debug('Updating graph with', data.nodes.length, 'nodes');
      (graphInstance as ForceGraphInstance).graphData(data);
    }
  },

  setLoading: (loading: boolean) => {
    set({ loading });
  },

  setError: (error: string | null) => {
    set({ error });
  },

  setFocusedNode: (nodeId: string | null) => {
    set({ focusedNodeId: nodeId });
  },

  setSelectedNode: (node: EntityNode | null) => {
    set({ selectedNode: node });
  },

  updateForceParameters: (strength: number, distance: number, centerForce?: number) => {
    const { graphInstance } = get();
    const previousCenterForce = get().centerForce;
    const newCenterForce = centerForce !== undefined ? centerForce : previousCenterForce;

    set({ forceStrength: strength, linkDistance: distance, centerForce: newCenterForce });

    if (graphInstance) {
      const instance = graphInstance as ForceGraphInstance;
      instance.d3Force('charge')?.strength(strength);
      instance.d3Force('link')?.distance?.(distance);

      if (centerForce !== undefined) {
        const width = instance.width();
        const height = instance.height();
        const centerX = width / 2;
        const centerY = height / 2;

        instance.d3Force('center')?.strength(newCenterForce);

        if (newCenterForce < 0.3) {
          instance.d3Force('x', null);
          instance.d3Force('y', null);
          instance.d3Force('center')?.strength(0.01);
        } else if (newCenterForce < 0.7) {
          instance.d3Force('x', d3.forceX(centerX).strength(0.1));
          instance.d3Force('y', d3.forceY(centerY).strength(0.1));
        } else {
          instance.d3Force('x', d3.forceX(centerX).strength(0.5));
          instance.d3Force('y', d3.forceY(centerY).strength(0.5));
          instance.d3Force('charge')?.strength(-100);
        }

        const alphaTarget = newCenterForce > 0.7 ? 1 : 0.8;
        instance.d3ReheatSimulation(alphaTarget);

        if (newCenterForce < 0.7) {
          instance.d3Force('charge')?.strength(strength);
        }
      } else {
        instance.d3ReheatSimulation(0.3);
      }
    }
  },

  setLinkCurvature: (curvature: number) => {
    const { graphInstance } = get();
    set({ linkCurvature: curvature });

    if (graphInstance) {
      (graphInstance as ForceGraphInstance).linkCurvature(curvature);
    }
  },

  resetFilters: () => {
    set({
      focusedNodeId: null,
      selectedNode: null,
      showInferredNodes: true,
      deduplicateNodes: false,
      hoveredNodeId: null,
      highlightedNodeIds: new Set<string>(),
      highlightedLinkKeys: new Set<string>(),
      hiddenEntityTypes: new Set<string>(),
    });
  },

  toggleInferredNodes: () => {
    set((state) => ({ showInferredNodes: !state.showInferredNodes }));
  },

  toggleDeduplication: () => {
    set((state) => ({ deduplicateNodes: !state.deduplicateNodes }));
  },

  toggleOrphanedNodes: () => {
    set((state) => ({ showOrphanedNodes: !state.showOrphanedNodes }));
  },

  zoomToFit: () => {
    const { graphInstance } = get();
    if (graphInstance) {
      (graphInstance as ForceGraphInstance).zoomToFit(400, 50);
    }
  },

  zoomIn: () => {
    const { graphInstance } = get();
    if (graphInstance) {
      const instance = graphInstance as ForceGraphInstance;
      const currentZoom = instance.zoom();
      instance.zoom(currentZoom * 1.2, 300);
    }
  },

  zoomOut: () => {
    const { graphInstance } = get();
    if (graphInstance) {
      const instance = graphInstance as ForceGraphInstance;
      const currentZoom = instance.zoom();
      instance.zoom(currentZoom * 0.8, 300);
    }
  },

  // Hover neighborhood highlight
  setHoveredNode: (nodeId: string | null) => {
    if (nodeId === null) {
      set({
        hoveredNodeId: null,
        highlightedNodeIds: new Set<string>(),
        highlightedLinkKeys: new Set<string>(),
      });
      return;
    }

    const { filteredGraphData } = get();
    const connectedNodes = new Set<string>([nodeId]);
    const connectedLinks = new Set<string>();

    for (const link of filteredGraphData.links) {
      const sourceId = typeof link.source === 'object' ? (link.source as EntityNode).id : link.source;
      const targetId = typeof link.target === 'object' ? (link.target as EntityNode).id : link.target;

      if (sourceId === nodeId || targetId === nodeId) {
        connectedNodes.add(sourceId);
        connectedNodes.add(targetId);
        connectedLinks.add(`${sourceId}--${targetId}`);
      }
    }

    set({
      hoveredNodeId: nodeId,
      highlightedNodeIds: connectedNodes,
      highlightedLinkKeys: connectedLinks,
    });
  },

  toggleEntityTypeVisibility: (type: string) => {
    set((state) => {
      const next = new Set(state.hiddenEntityTypes);
      if (next.has(type)) {
        next.delete(type);
      } else {
        next.add(type);
      }
      return { hiddenEntityTypes: next };
    });
  },

  setControlsPanelCollapsed: (collapsed: boolean) => {
    set({ controlsPanelCollapsed: collapsed });
  },

  setLegendPanelCollapsed: (collapsed: boolean) => {
    set({ legendPanelCollapsed: collapsed });
  },

  setIsDarkMode: (dark: boolean) => {
    const { graphInstance } = get();
    set({ isDarkMode: dark });
    if (graphInstance) {
      (graphInstance as ForceGraphInstance).backgroundColor(dark ? '#1a1a2e' : '#fafafa');
    }
  },

  centerOnNode: (nodeId: string) => {
    const { graphInstance, filteredGraphData } = get();
    if (!graphInstance) return;
    const node = filteredGraphData.nodes.find(n => n.id === nodeId);
    if (node && node.x !== undefined && node.y !== undefined) {
      const instance = graphInstance as ForceGraphInstance;
      instance.centerAt(node.x, node.y, 600);
      instance.zoom(1.5, 600);
    }
  },
}));

export default useEntityGraphStore;
