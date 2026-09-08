import { layoutAgentTaskGroups } from './layoutAgentTaskGroups';
import type { Node } from 'reactflow';
import type { UILayoutState } from '../../../../types/ui/layout';

export interface CanvasArea {
  x: number;
  y: number;
  width: number;
  height: number;
}

export interface NodeDimensions {
  width: number;
  height: number;
}

export interface LayoutOptions {
  margin: number;
  minNodeSpacing: number;
  defaultUIState?: Partial<UILayoutState>;
}

/**
 * Enhanced CanvasLayoutManager - Comprehensive UI-aware node positioning system
 *
 * This class calculates optimal node positions while considering ALL UI elements:
 * - TabBar at top
 * - Left sidebar (activity bar + expandable panel)
 * - Right sidebar
 * - Chat panel (overlay, resizable)
 * - Execution history (bottom overlay, resizable)
 * - Panel splits for dual canvas mode
 *
 * Features:
 * - Real-time UI state tracking
 * - Accurate available space calculation
 * - Intelligent node positioning algorithms
 * - Support for multiple canvas areas (crew vs flow)
 * - Responsive layout adaptation
 */
export class CanvasLayoutManager {
  private uiState: UILayoutState;
  private margin: number;
  private minNodeSpacing: number;

  // Standard node dimensions (can be customized per node type)
  private static readonly NODE_DIMENSIONS: Record<string, NodeDimensions> = {
    agentNode: { width: 200, height: 150 },
    managerNode: { width: 200, height: 150 },
    taskNode: { width: 220, height: 180 },
    crewNode: { width: 240, height: 200 },
    default: { width: 200, height: 150 }
  };

  constructor(options: LayoutOptions = { margin: 20, minNodeSpacing: 50 }) {
    this.margin = options.margin;
    this.minNodeSpacing = options.minNodeSpacing;

    // Initialize with default UI state
    this.uiState = {
      // Screen dimensions (will be updated)
      screenWidth: typeof window !== 'undefined' ? window.innerWidth : 1200,
      screenHeight: typeof window !== 'undefined' ? window.innerHeight : 800,

      // Fixed UI elements
      tabBarHeight: 48,

      // Left sidebar defaults
      leftSidebarVisible: true,
      leftSidebarExpanded: false,
      leftSidebarBaseWidth: 48,
      leftSidebarExpandedWidth: 280,

      // Right sidebar defaults
      rightSidebarVisible: false,
      rightSidebarWidth: 0,

      // Chat panel defaults
      chatPanelVisible: true,
      chatPanelCollapsed: false,
      chatPanelWidth: 450,
      chatPanelCollapsedWidth: 60,
      chatPanelSide: 'right',

      // Execution history defaults
      executionHistoryVisible: false,
      executionHistoryHeight: 60,

      // Panel splits
      panelPosition: 50,
      areFlowsVisible: true,

      // Layout orientation
      layoutOrientation: 'vertical',

      // Override with provided defaults
      ...options.defaultUIState
    };
  }

  /**
   * Update the complete UI state for accurate layout calculations
   */
  updateUIState(newState: Partial<UILayoutState>): void {
    this.uiState = {
      ...this.uiState,
      ...newState
    };
  }

  /**
   * Update screen dimensions (call on window resize)
   */
  updateScreenDimensions(width: number, height: number): void {
    this.uiState.screenWidth = width;
    this.uiState.screenHeight = height;
  }

  /**
   * Calculate the exact available canvas area considering all UI elements
   */
  getAvailableCanvasArea(canvasType: 'crew' | 'flow' | 'full' = 'full'): CanvasArea {
    // Start with full screen
    let availableX = 0;
    let availableY = this.uiState.tabBarHeight; // Account for tab bar
    let availableWidth = this.uiState.screenWidth;
    let availableHeight = this.uiState.screenHeight - this.uiState.tabBarHeight;

    // Subtract left sidebar
    if (this.uiState.leftSidebarVisible) {
      const leftSidebarWidth = this.uiState.leftSidebarExpanded
        ? this.uiState.leftSidebarExpandedWidth
        : this.uiState.leftSidebarBaseWidth;
      availableX += leftSidebarWidth;
      availableWidth -= leftSidebarWidth;
    }

    // Subtract right sidebar
    if (this.uiState.rightSidebarVisible) {
      availableWidth -= this.uiState.rightSidebarWidth;
    }

    const responseFocused = this.uiState.assistantResponseFocused && this.uiState.executionHistoryVisible;
    if (responseFocused) {
      if (this.uiState.screenWidth >= 900) {
        const ratio = this.uiState.assistantPanelRatio ?? 0.6;
        if (this.uiState.assistantPanelSide !== 'right') availableX += availableWidth * ratio;
        availableWidth *= 1 - ratio;
      } else {
        availableY += this.uiState.screenHeight * 0.55;
        availableHeight -= this.uiState.screenHeight * 0.55;
      }
    }
    // The composer moves into the main response area in focus view.
    if (!responseFocused && this.uiState.chatPanelVisible && !this.uiState.areFlowsVisible) {
      availableHeight -= this.uiState.assistantDockHeight ?? 128;
    }

    // Runs and responses share one narrow sidebar; never reserve two columns.
    if (!responseFocused && this.uiState.executionHistoryVisible && this.uiState.screenWidth >= 900) {
      availableWidth -= 268;
      if (this.uiState.assistantPanelSide === 'left') availableX += 268;
    }

    // Handle panel splits for dual canvas mode
    if (canvasType === 'crew' && this.uiState.areFlowsVisible) {
      // Crew canvas takes left portion based on panel position
      availableWidth = availableWidth * (this.uiState.panelPosition / 100);
    } else if (canvasType === 'flow' && this.uiState.areFlowsVisible) {
      // Flow canvas takes right portion
      const crewWidth = availableWidth * (this.uiState.panelPosition / 100);
      availableX += crewWidth;
      availableWidth = availableWidth * ((100 - this.uiState.panelPosition) / 100);
    }
    // For 'full' or when flows are hidden, use the entire available area

    // Apply margins
    const finalArea: CanvasArea = {
      x: availableX + this.margin,
      y: availableY + this.margin,
      width: Math.max(200, availableWidth - (this.margin * 2)), // Minimum usable width
      height: Math.max(150, availableHeight - (this.margin * 2)) // Minimum usable height
    };

    return finalArea;
  }

  /** Append individual nodes below their peers without moving the existing canvas. */
  getAgentNodePosition(existingNodes: Node[], canvasType: 'crew' | 'flow' | 'full' = 'crew'): { x: number; y: number } {
    return this.getAppendedNodePosition(existingNodes, 'agentNode', canvasType);
  }

  getTaskNodePosition(existingNodes: Node[], canvasType: 'crew' | 'flow' | 'full' = 'crew'): { x: number; y: number } {
    return this.getAppendedNodePosition(existingNodes, 'taskNode', canvasType);
  }

  private getAppendedNodePosition(existingNodes: Node[], type: 'agentNode' | 'taskNode', canvasType: 'crew' | 'flow' | 'full'): { x: number; y: number } {
    const area = this.getAvailableCanvasArea(canvasType);
    const gap = Math.max(40, this.minNodeSpacing);
    // Measured dimensions are in canvas coordinates (unaffected by zoom).
    // Conservative fallbacks cover wrapped titles before React Flow measures a node.
    const defaults = { agentNode: { width: 200, height: 210 }, taskNode: { width: 270, height: 230 } };
    const size = (node: Node) => ({
      width: node.width || defaults[node.type as keyof typeof defaults]?.width || CanvasLayoutManager.NODE_DIMENSIONS[node.type || 'default']?.width || 200,
      height: node.height || defaults[node.type as keyof typeof defaults]?.height || CanvasLayoutManager.NODE_DIMENSIONS[node.type || 'default']?.height || 200,
    });
    const obstacles = existingNodes.filter(node => !node.hidden);
    const peers = obstacles.filter(node => node.type === type);
    const position = { x: area.x + gap, y: area.y + gap };
    if (peers.length) {
      const bottommost = peers.reduce((bottom, node) => node.position.y + size(node).height > bottom.position.y + size(bottom).height ? node : bottom);
      position.x = bottommost.position.x;
      position.y = bottommost.position.y + size(bottommost).height + gap;
    } else if (type === 'taskNode') {
      const agent = obstacles.find(node => node.type === 'agentNode');
      if (agent) {
        if (this.uiState.layoutOrientation === 'vertical') {
          position.x = agent.position.x + (size(agent).width - defaults.taskNode.width) / 2;
          position.y = agent.position.y + size(agent).height + gap;
        } else {
          position.x = agent.position.x + size(agent).width + gap;
          position.y = agent.position.y;
        }
      }
    }
    // A manually moved node of another type may occupy the next slot. Step
    // below every intersecting card until the whole new card has clear space.
    const dimensions = defaults[type];
    for (let i = 0; i <= obstacles.length; i++) {
      const collisions = obstacles.filter(node => {
        const bounds = size(node);
        return position.x < node.position.x + bounds.width + gap && position.x + dimensions.width + gap > node.position.x &&
          position.y < node.position.y + bounds.height + gap && position.y + dimensions.height + gap > node.position.y;
      });
      if (!collisions.length) break;
      position.y = Math.max(...collisions.map(node => node.position.y + size(node).height + gap));
    }
    return position;
  }

  /**
   * Get optimal position for manager node in hierarchical mode
   */
  getManagerNodePosition(existingNodes: Node[], canvasType: 'crew' | 'full' = 'crew'): { x: number; y: number } {
    const availableArea = this.getAvailableCanvasArea(canvasType);
    const managerDims = CanvasLayoutManager.NODE_DIMENSIONS.managerNode;
    const agentNodes = existingNodes.filter(node => node.type === 'agentNode');
    const spacing = this.minNodeSpacing;

    const currentLayout = this.uiState?.layoutOrientation || 'horizontal';

    console.log('[CanvasLayoutManager] Getting manager node position:', {
      layout: currentLayout,
      agentCount: agentNodes.length,
      availableArea
    });

    if (agentNodes.length === 0) {
      // No agents yet - place manager at default position
      if (currentLayout === 'vertical') {
        // Vertical: center horizontally at top
        return {
          x: Math.round(availableArea.x + spacing),
          y: Math.round(availableArea.y + spacing)
        };
      } else {
        // Horizontal: place at left
        return {
          x: Math.round(availableArea.x + spacing),
          y: Math.round(availableArea.y + spacing)
        };
      }
    }

    if (currentLayout === 'vertical') {
      // Vertical layout: Manager above all agents (centered horizontally)
      const minAgentY = Math.min(...agentNodes.map(n => n.position.y));
      const avgAgentX = agentNodes.reduce((sum, n) => sum + n.position.x, 0) / agentNodes.length;

      // Position manager above the topmost agent with generous spacing
      // Use spacing * 4 to ensure clear separation
      const managerY = minAgentY - managerDims.height - spacing * 4;

      // Don't clamp the Y position - allow negative values to position manager above agents
      // The manager MUST be above the agents in hierarchical mode

      console.log('[CanvasLayoutManager] Vertical manager position calculation:', {
        minAgentY,
        avgAgentX,
        managerHeight: managerDims.height,
        spacing,
        calculatedY: managerY,
        finalY: Math.round(managerY)
      });

      return {
        x: Math.round(avgAgentX),
        y: Math.round(managerY)
      };
    } else {
      // Horizontal layout: Manager to the left of all agents (centered vertically)
      const minAgentX = Math.min(...agentNodes.map(n => n.position.x));
      const avgAgentY = agentNodes.reduce((sum, n) => sum + n.position.y, 0) / agentNodes.length;

      // Position manager to the left of the leftmost agent with generous spacing
      // Use spacing * 4 to ensure clear separation
      const managerX = minAgentX - managerDims.width - spacing * 4;

      // Don't clamp the X position - allow negative values to position manager to the left

      console.log('[CanvasLayoutManager] Horizontal manager position calculation:', {
        minAgentX,
        avgAgentY,
        managerWidth: managerDims.width,
        spacing,
        calculatedX: managerX,
        finalX: Math.round(managerX)
      });

      return {
        x: Math.round(managerX),
        y: Math.round(avgAgentY)
      };
    }
  }


  /**
   * Get optimal positions for multiple nodes (crew generation)
   * Ensures all nodes fit within available canvas area and provides auto-fit data
   */
  getCrewLayoutPositions(agents: number, tasks: number, canvasType: 'crew' | 'full' = 'crew'): {
    agentPositions: { x: number; y: number }[];
    taskPositions: { x: number; y: number }[];
    layoutBounds: { x: number; y: number; width: number; height: number };
    shouldAutoFit: boolean;
  } {
    const availableArea = this.getAvailableCanvasArea(canvasType);
    const agentDims = CanvasLayoutManager.NODE_DIMENSIONS.agentNode;
    const taskDims = CanvasLayoutManager.NODE_DIMENSIONS.taskNode;

    const agentPositions: { x: number; y: number }[] = [];
    const taskPositions: { x: number; y: number }[] = [];

    // Check if we have a very narrow canvas
    const isNarrowCanvas = availableArea.width < 600;
    const reducedSpacing = isNarrowCanvas ? Math.max(20, this.minNodeSpacing / 2) : this.minNodeSpacing;

    console.log(`[CanvasLayout] Available area: ${availableArea.width}x${availableArea.height}, isNarrow: ${isNarrowCanvas}, spacing: ${reducedSpacing}`);

    // For narrow canvases, use a more compact layout strategy
    if (isNarrowCanvas) {
      return this.getCompactCrewLayout(agents, tasks, availableArea, reducedSpacing);
    }

    // Calculate how many nodes can fit vertically with normal spacing
    const maxAgentsPerColumn = Math.max(1, Math.floor(availableArea.height / (agentDims.height + reducedSpacing)));

    // Calculate number of columns needed
    const agentColumns = Math.ceil(agents / maxAgentsPerColumn);
    const taskColumns = tasks > 0 ? 1 : 0; // Tasks always in single column

    // Calculate total layout width needed
    const agentAreaWidth = agentColumns * (agentDims.width + reducedSpacing);
    const taskAreaWidth = taskColumns * (taskDims.width + reducedSpacing);
    const totalLayoutWidth = agentAreaWidth + taskAreaWidth;

    // Start positioning from left side of available area
    const startX = availableArea.x;

    // Position agents in columns (left side)
    for (let i = 0; i < agents; i++) {
      const col = Math.floor(i / maxAgentsPerColumn);
      const row = i % maxAgentsPerColumn;

      const x = startX + col * (agentDims.width + reducedSpacing);
      const y = availableArea.y + row * (agentDims.height + reducedSpacing);

      agentPositions.push({ x, y });
    }

    // Position tasks in single column to the right of agents (always stacked vertically)
    const taskStartX = startX + agentAreaWidth;
    for (let i = 0; i < tasks; i++) {
      const x = taskStartX; // All tasks in same column
      const y = availableArea.y + i * (taskDims.height + reducedSpacing);

      taskPositions.push({ x, y });
    }

    // Calculate actual layout bounds
    const allPositions = [...agentPositions, ...taskPositions];
    if (allPositions.length === 0) {
      return {
        agentPositions: [],
        taskPositions: [],
        layoutBounds: { x: availableArea.x, y: availableArea.y, width: 0, height: 0 },
        shouldAutoFit: false
      };
    }

    const minX = Math.min(...allPositions.map(p => p.x));
    const maxX = Math.max(...allPositions.map(p => p.x),
                         ...agentPositions.map(p => p.x + agentDims.width),
                         ...taskPositions.map(p => p.x + taskDims.width));
    const minY = Math.min(...allPositions.map(p => p.y));
    const maxY = Math.max(...allPositions.map(p => p.y),
                         ...agentPositions.map(p => p.y + agentDims.height),
                         ...taskPositions.map(p => p.y + taskDims.height));

    const layoutBounds = {
      x: minX,
      y: minY,
      width: maxX - minX,
      height: maxY - minY
    };

    // Determine if auto-fit is needed (layout extends beyond available area)
    const shouldAutoFit = totalLayoutWidth > availableArea.width ||
                         layoutBounds.height > availableArea.height;

    console.log(`[CanvasLayout] Layout bounds: ${layoutBounds.width}x${layoutBounds.height}, shouldAutoFit: ${shouldAutoFit}`);

    return {
      agentPositions,
      taskPositions,
      layoutBounds,
      shouldAutoFit
    };
  }

  /**
   * Compact layout strategy for narrow canvases
   * Agents in left column, tasks in right column, both stacked vertically
   */
  private getCompactCrewLayout(
    agents: number,
    tasks: number,
    availableArea: CanvasArea,
    spacing: number
  ): {
    agentPositions: { x: number; y: number }[];
    taskPositions: { x: number; y: number }[];
    layoutBounds: { x: number; y: number; width: number; height: number };
    shouldAutoFit: boolean;
  } {
    const agentDims = CanvasLayoutManager.NODE_DIMENSIONS.agentNode;
    const taskDims = CanvasLayoutManager.NODE_DIMENSIONS.taskNode;
    const agentPositions: { x: number; y: number }[] = [];
    const taskPositions: { x: number; y: number }[] = [];

    // For narrow screens: agents in left column, tasks in right column
    // Calculate how much width we can allocate to each column
    const totalColumns = (agents > 0 ? 1 : 0) + (tasks > 0 ? 1 : 0);
    const availableWidth = availableArea.width - (spacing * (totalColumns + 1));
    const columnWidth = totalColumns > 0 ? availableWidth / totalColumns : availableArea.width;

    // Ensure minimum viable width
    const nodeWidth = Math.max(140, Math.min(200, columnWidth));

    // Position agents in left column (vertically stacked)
    if (agents > 0) {
      const agentX = availableArea.x + spacing;
      for (let i = 0; i < agents; i++) {
        const y = availableArea.y + i * (agentDims.height + spacing);
        agentPositions.push({ x: agentX, y });
      }
    }

    // Position tasks in right column (vertically stacked)
    if (tasks > 0) {
      const taskX = agents > 0
        ? availableArea.x + spacing + nodeWidth + spacing  // After agents column
        : availableArea.x + spacing;  // First column if no agents

      for (let i = 0; i < tasks; i++) {
        const y = availableArea.y + i * (taskDims.height + spacing);
        taskPositions.push({ x: taskX, y });
      }
    }

    // Calculate layout bounds
    const allPositions = [...agentPositions, ...taskPositions];
    if (allPositions.length === 0) {
      return {
        agentPositions: [],
        taskPositions: [],
        layoutBounds: { x: availableArea.x, y: availableArea.y, width: 0, height: 0 },
        shouldAutoFit: false
      };
    }

    const minX = Math.min(...allPositions.map(p => p.x));
    const maxX = Math.max(...allPositions.map(p => p.x + nodeWidth));
    const minY = Math.min(...allPositions.map(p => p.y));
    const maxY = Math.max(
      ...agentPositions.map(p => p.y + agentDims.height),
      ...taskPositions.map(p => p.y + taskDims.height)
    );

    const layoutBounds = {
      x: minX,
      y: minY,
      width: maxX - minX,
      height: maxY - minY
    };

    // Auto-fit if layout still doesn't fit
    const shouldAutoFit = layoutBounds.width > availableArea.width ||
                         layoutBounds.height > availableArea.height;

    console.log(`[CompactLayout] Agents column, tasks column vertically stacked: ${layoutBounds.width}x${layoutBounds.height}, shouldAutoFit: ${shouldAutoFit}`);

    return {
      agentPositions,
      taskPositions,
      layoutBounds,
      shouldAutoFit
    };
  }


  /**
   * Get node dimensions for a specific node type
   */
  static getNodeDimensions(nodeType: string): NodeDimensions {
    return CanvasLayoutManager.NODE_DIMENSIONS[nodeType] ||
           CanvasLayoutManager.NODE_DIMENSIONS.default;
  }

  /**
   * Utility method to organize existing nodes to prevent overlap
   */
  reorganizeNodes(nodes: Node[], canvasType: 'crew' | 'flow' | 'full' = 'full', edges: Array<{ id: string; source: string; target: string }> = []): Node[] {
    const availableArea = this.getAvailableCanvasArea(canvasType);
    const orientation = this.uiState.layoutOrientation || 'horizontal';
    const crewNodes = nodes.filter(node => node.type !== 'crewNode');
    const flowNodes = nodes.filter(node => node.type === 'crewNode');
    const reorganizedNodes = layoutAgentTaskGroups(crewNodes, edges, orientation, availableArea, Math.max(this.minNodeSpacing, 80));
    reorganizedNodes.push(...this.layoutFlowNodesByDependency(
      flowNodes, edges, orientation, availableArea, CanvasLayoutManager.NODE_DIMENSIONS.crewNode,
    ));

    return reorganizedNodes;
  }

  /**
   * Lay out flow (crew) nodes as a layered DAG based on their connection graph.
   *
   * Nodes are assigned a depth = longest path from any root (a node with no
   * incoming edge). Each depth becomes a layer; siblings in a layer are spread
   * along the cross-axis and every layer is centered, so a start node sits above
   * (or left of) its branches and a merge node lands centered after them.
   *
   * - vertical:   layers run top → bottom; siblings spread along X.
   * - horizontal: layers run left → right; siblings spread along Y.
   *
   * Falls back gracefully when there are no edges (everything lands in one layer).
   */
  private layoutFlowNodesByDependency(
    flowNodes: Node[],
    edges: Array<{ id: string; source: string; target: string }>,
    orientation: 'vertical' | 'horizontal',
    availableArea: CanvasArea,
    flowDims: NodeDimensions
  ): Node[] {
    if (flowNodes.length === 0) return [];

    const ids = new Set(flowNodes.map(n => n.id));
    const children = new Map<string, string[]>();
    const indegree = new Map<string, number>();
    ids.forEach(id => {
      children.set(id, []);
      indegree.set(id, 0);
    });

    edges.forEach(e => {
      if (e.source !== e.target && ids.has(e.source) && ids.has(e.target)) {
        children.get(e.source)!.push(e.target);
        indegree.set(e.target, (indegree.get(e.target) || 0) + 1);
      }
    });

    // Longest-path layering via Kahn topological traversal: a node's depth is one
    // more than the deepest parent, so merge points sit below all their inputs.
    const depth = new Map<string, number>();
    const indegreeWork = new Map(indegree);
    const queue: string[] = [];
    ids.forEach(id => {
      if ((indegreeWork.get(id) || 0) === 0) {
        depth.set(id, 0);
        queue.push(id);
      }
    });
    for (let head = 0; head < queue.length; head++) {
      const n = queue[head];
      const nd = depth.get(n) || 0;
      for (const c of children.get(n) || []) {
        depth.set(c, Math.max(depth.get(c) ?? 0, nd + 1));
        indegreeWork.set(c, (indegreeWork.get(c) || 0) - 1);
        if ((indegreeWork.get(c) || 0) === 0) queue.push(c);
      }
    }
    // Any node still unleveled (part of a cycle) lands in the first layer.
    flowNodes.forEach(n => {
      if (!depth.has(n.id)) depth.set(n.id, 0);
    });

    // Group nodes by depth (layer) and order siblings deterministically.
    const byLayer = new Map<number, Node[]>();
    flowNodes.forEach(n => {
      const d = depth.get(n.id) ?? 0;
      const layer = byLayer.get(d) || [];
      layer.push(n);
      byLayer.set(d, layer);
    });
    const orderOf = (n: Node) => (n.data?.order as number | undefined) ?? Number.MAX_SAFE_INTEGER;
    byLayer.forEach(layer =>
      layer.sort((a, b) => {
        const oa = orderOf(a);
        const ob = orderOf(b);
        if (oa !== ob) return oa - ob;
        return orientation === 'vertical'
          ? a.position.x - b.position.x
          : a.position.y - b.position.y;
      })
    );

    const layerDepths = [...byLayer.keys()].sort((a, b) => a - b);
    const maxSiblings = Math.max(...[...byLayer.values()].map(l => l.length));

    const layerGap = Math.max(this.minNodeSpacing * 0.5, 40); // gap between layers
    const siblingGap = Math.max(this.minNodeSpacing * 0.4, 28); // gap within a layer

    const positioned: Node[] = [];

    if (orientation === 'vertical') {
      const crossStep = flowDims.width + siblingGap;
      const globalWidth = maxSiblings * flowDims.width + (maxSiblings - 1) * siblingGap;
      const centerX = availableArea.x + globalWidth / 2;
      layerDepths.forEach(d => {
        const layer = byLayer.get(d)!;
        const rowWidth = layer.length * flowDims.width + (layer.length - 1) * siblingGap;
        const startX = centerX - rowWidth / 2;
        const y = availableArea.y + d * (flowDims.height + layerGap);
        layer.forEach((node, i) => {
          positioned.push({ ...node, position: { x: startX + i * crossStep, y } });
        });
      });
    } else {
      const crossStep = flowDims.height + siblingGap;
      const globalHeight = maxSiblings * flowDims.height + (maxSiblings - 1) * siblingGap;
      const centerY = availableArea.y + globalHeight / 2;
      layerDepths.forEach(d => {
        const layer = byLayer.get(d)!;
        const colHeight = layer.length * flowDims.height + (layer.length - 1) * siblingGap;
        const startY = centerY - colHeight / 2;
        const x = availableArea.x + d * (flowDims.width + layerGap);
        layer.forEach((node, i) => {
          positioned.push({ ...node, position: { x, y: startY + i * crossStep } });
        });
      });
    }

    return positioned;
  }


  /**
   * Get debug information about current layout state
   */
  getLayoutDebugInfo(): {
    uiState: UILayoutState;
    availableAreas: Record<string, CanvasArea>;
    recommendations: string[];
  } {
    const availableAreas = {
      full: this.getAvailableCanvasArea('full'),
      crew: this.getAvailableCanvasArea('crew'),
      flow: this.getAvailableCanvasArea('flow')
    };

    const recommendations: string[] = [];

    // Check for potential issues and provide specific recommendations
    if (availableAreas.crew.width < 400) {
      recommendations.push('❌ CRITICAL: Canvas is extremely narrow. Collapse chat panel immediately!');
    } else if (availableAreas.crew.width < 600) {
      recommendations.push('⚠️ Canvas width is narrow - collapse chat panel or reduce window elements');
    }

    if (availableAreas.crew.height < 300) {
      recommendations.push('❌ CRITICAL: Canvas height is too small. Close a side panel to free canvas space!');
    } else if (availableAreas.crew.height < 400) {
      recommendations.push('⚠️ Canvas height is limited - consider closing a side panel');
    }

    if (this.uiState.chatPanelVisible && !this.uiState.chatPanelCollapsed && this.uiState.chatPanelWidth > 350) {
      recommendations.push('💡 TIP: Reduce chat panel width or collapse it temporarily for better node visibility');
    }

    if (this.uiState.executionHistoryVisible) {
      recommendations.push('💡 TIP: Close job history to give more space for nodes');
    }

    // Add specific action suggestions
    const totalUIOverhead = this.uiState.leftSidebarBaseWidth + this.uiState.rightSidebarWidth +
                           (this.uiState.chatPanelVisible ? this.uiState.chatPanelWidth : 0);
    const uiOverheadPercentage = (totalUIOverhead / this.uiState.screenWidth) * 100;

    if (uiOverheadPercentage > 60) {
      recommendations.push(`🔧 ACTION: UI elements take ${Math.round(uiOverheadPercentage)}% of screen width. Consider larger screen or hide panels.`);
    }

    return {
      uiState: this.uiState,
      availableAreas,
      recommendations
    };
  }
}
