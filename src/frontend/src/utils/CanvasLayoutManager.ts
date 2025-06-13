import { Node } from 'reactflow';

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

export interface UILayoutState {
  // Screen dimensions
  screenWidth: number;
  screenHeight: number;
  
  // Fixed UI elements
  tabBarHeight: number;
  
  // Left sidebar
  leftSidebarVisible: boolean;
  leftSidebarExpanded: boolean;
  leftSidebarBaseWidth: number;    // Activity bar width (48px)
  leftSidebarExpandedWidth: number; // Full expanded width (280px)
  
  // Right sidebar
  rightSidebarVisible: boolean;
  rightSidebarWidth: number;       // Fixed width (48px)
  
  // Chat panel
  chatPanelVisible: boolean;
  chatPanelCollapsed: boolean;
  chatPanelWidth: number;          // Dynamic width when expanded
  chatPanelCollapsedWidth: number; // Width when collapsed (60px)
  
  // Execution history
  executionHistoryVisible: boolean;
  executionHistoryHeight: number;  // Dynamic height
  
  // Panel splits (for dual canvas mode)
  panelPosition: number;           // 0-100% split between crew/flow panels
  areFlowsVisible: boolean;        // Whether flows panel is shown
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
    taskNode: { width: 220, height: 180 },
    flowNode: { width: 180, height: 120 },
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
      rightSidebarVisible: true,
      rightSidebarWidth: 48,
      
      // Chat panel defaults
      chatPanelVisible: true,
      chatPanelCollapsed: false,
      chatPanelWidth: 450,
      chatPanelCollapsedWidth: 60,
      
      // Execution history defaults
      executionHistoryVisible: false,
      executionHistoryHeight: 60,
      
      // Panel splits
      panelPosition: 50,
      areFlowsVisible: true,
      
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
    const availableY = this.uiState.tabBarHeight; // Account for tab bar
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

    // Subtract chat panel (overlay from the right)
    if (this.uiState.chatPanelVisible) {
      const chatWidth = this.uiState.chatPanelCollapsed 
        ? this.uiState.chatPanelCollapsedWidth 
        : this.uiState.chatPanelWidth;
      availableWidth -= chatWidth;
    }

    // Subtract execution history (overlay from the bottom)
    if (this.uiState.executionHistoryVisible) {
      availableHeight -= this.uiState.executionHistoryHeight;
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

  /**
   * Get optimal position for a new agent node
   */
  getAgentNodePosition(existingNodes: Node[], canvasType: 'crew' | 'flow' | 'full' = 'crew'): { x: number; y: number } {
    const availableArea = this.getAvailableCanvasArea(canvasType);
    const agentNodes = existingNodes.filter(node => node.type === 'agentNode');
    const nodeDims = CanvasLayoutManager.NODE_DIMENSIONS.agentNode;
    const isNarrow = availableArea.width < 600;
    const spacing = isNarrow ? Math.max(20, this.minNodeSpacing / 2) : this.minNodeSpacing;

    if (agentNodes.length === 0) {
      // First agent - position in top-left of available area with proper margin
      return {
        x: availableArea.x + spacing,
        y: availableArea.y + spacing
      };
    }

    // For narrow screens, use smarter positioning
    if (isNarrow) {
      return this.findSmartAgentPosition(agentNodes, availableArea, spacing);
    }

    // Find the best position for the new agent using standard layout
    return this.findOptimalPosition(agentNodes, nodeDims, availableArea, 'vertical');
  }

  /**
   * Get optimal position for a new task node
   */
  getTaskNodePosition(existingNodes: Node[], canvasType: 'crew' | 'flow' | 'full' = 'crew'): { x: number; y: number } {
    const availableArea = this.getAvailableCanvasArea(canvasType);
    const taskNodes = existingNodes.filter(node => node.type === 'taskNode');
    const agentNodes = existingNodes.filter(node => node.type === 'agentNode');
    const isNarrow = availableArea.width < 600;
    const spacing = isNarrow ? Math.max(20, this.minNodeSpacing / 2) : this.minNodeSpacing;

    if (taskNodes.length === 0) {
      if (agentNodes.length > 0) {
        // Position first task in the tasks column (to the right of agents)
        return this.getFirstTaskPosition(agentNodes, availableArea, spacing, isNarrow);
      }
      
      // No agents, position task in left area
      return {
        x: availableArea.x + spacing,
        y: availableArea.y + spacing
      };
    }

    // Always stack tasks vertically in the same column
    return this.findVerticalTaskPosition(taskNodes, availableArea, spacing);
  }

  /**
   * Get optimal position for a new flow node
   */
  getFlowNodePosition(existingNodes: Node[], canvasType: 'flow' | 'full' = 'flow'): { x: number; y: number } {
    const availableArea = this.getAvailableCanvasArea(canvasType);
    const flowNodes = existingNodes.filter(node => node.type === 'flowNode');
    const nodeDims = CanvasLayoutManager.NODE_DIMENSIONS.flowNode;

    if (flowNodes.length === 0) {
      // First flow node - center in available area
      return {
        x: availableArea.x + (availableArea.width - nodeDims.width) / 2,
        y: availableArea.y + 50
      };
    }

    // Find the best position for the new flow node
    return this.findOptimalPosition(flowNodes, nodeDims, availableArea, 'horizontal');
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
   * Find smart agent position for narrow screens
   */
  private findSmartAgentPosition(
    agentNodes: Node[], 
    availableArea: CanvasArea, 
    spacing: number
  ): { x: number; y: number } {
    const agentDims = CanvasLayoutManager.NODE_DIMENSIONS.agentNode;
    
    // Find the lowest agent to stack below it
    const lowestAgent = agentNodes.reduce((lowest, current) => 
      current.position.y > lowest.position.y ? current : lowest
    );
    
    const newY = lowestAgent.position.y + agentDims.height + spacing;
    
    // Check if we can fit another agent vertically
    if (newY + agentDims.height <= availableArea.y + availableArea.height) {
      return {
        x: lowestAgent.position.x,
        y: newY
      };
    }
    
    // Need to start a new column
    const rightmostAgent = agentNodes.reduce((rightmost, current) => 
      current.position.x > rightmost.position.x ? current : rightmost
    );
    
    return {
      x: rightmostAgent.position.x + agentDims.width + spacing,
      y: availableArea.y + spacing
    };
  }

  /**
   * Get position for first task relative to agents
   */
  private getFirstTaskPosition(
    agentNodes: Node[], 
    availableArea: CanvasArea, 
    spacing: number, 
    isNarrow: boolean
  ): { x: number; y: number } {
    const agentDims = CanvasLayoutManager.NODE_DIMENSIONS.agentNode;
    
    if (isNarrow) {
      // For narrow screens, find the rightmost agent and place task next to it
      const rightmostAgent = agentNodes.reduce((rightmost, current) => 
        current.position.x > rightmost.position.x ? current : rightmost
      );
      
      return {
        x: rightmostAgent.position.x + agentDims.width + spacing,
        y: availableArea.y + spacing
      };
    }
    
    // For wider screens, use standard logic
    const rightmostAgent = agentNodes.reduce((rightmost, current) => 
      current.position.x > rightmost.position.x ? current : rightmost
    );
    
    const newX = rightmostAgent.position.x + agentDims.width + spacing;
    
    return {
      x: newX,
      y: rightmostAgent.position.y
    };
  }

  /**
   * Find vertical position for new task (always stack vertically)
   */
  private findVerticalTaskPosition(
    taskNodes: Node[], 
    availableArea: CanvasArea, 
    spacing: number
  ): { x: number; y: number } {
    const taskDims = CanvasLayoutManager.NODE_DIMENSIONS.taskNode;
    
    // Find the lowest task
    const lowestTask = taskNodes.reduce((lowest, current) => 
      current.position.y > lowest.position.y ? current : lowest
    );
    
    // Always use the same X position as existing tasks (same column)
    const taskX = taskNodes[0].position.x;
    const newY = lowestTask.position.y + taskDims.height + spacing;
    
    // Check if we can fit vertically
    if (newY + taskDims.height <= availableArea.y + availableArea.height) {
      return {
        x: taskX,
        y: newY
      };
    }
    
    // If we can't fit vertically, still stack in same column but let auto-fit handle it
    return {
      x: taskX,
      y: newY
    };
  }

  /**
   * Find optimal position for a new node among existing nodes of the same type
   */
  private findOptimalPosition(
    existingNodes: Node[], 
    nodeDims: NodeDimensions, 
    availableArea: CanvasArea,
    layout: 'vertical' | 'horizontal' | 'grid' = 'vertical'
  ): { x: number; y: number } {
    
    if (layout === 'vertical') {
      // Stack nodes vertically
      const lowestNode = existingNodes.reduce((lowest, current) => 
        current.position.y > lowest.position.y ? current : lowest
      );
      
      const newY = lowestNode.position.y + nodeDims.height + this.minNodeSpacing;
      
      // Check if we need to wrap to a new column
      if (newY + nodeDims.height > availableArea.y + availableArea.height) {
        // Start a new column
        const rightmostNode = existingNodes.reduce((rightmost, current) => 
          current.position.x > rightmost.position.x ? current : rightmost
        );
        
        const newX = rightmostNode.position.x + nodeDims.width + this.minNodeSpacing;
        
        // Ensure we don't exceed available width
        if (newX + nodeDims.width <= availableArea.x + availableArea.width) {
          return { x: newX, y: availableArea.y };
        } else {
          // If no room for new column, stack at bottom
          return { x: lowestNode.position.x, y: newY };
        }
      }
      
      return { x: lowestNode.position.x, y: newY };
    }
    
    if (layout === 'horizontal') {
      // Stack nodes horizontally
      const rightmostNode = existingNodes.reduce((rightmost, current) => 
        current.position.x > rightmost.position.x ? current : rightmost
      );
      
      const newX = rightmostNode.position.x + nodeDims.width + this.minNodeSpacing;
      
      // Check if we need to wrap to a new row
      if (newX + nodeDims.width > availableArea.x + availableArea.width) {
        // Start a new row
        const lowestNode = existingNodes.reduce((lowest, current) => 
          current.position.y > lowest.position.y ? current : lowest
        );
        
        return {
          x: availableArea.x,
          y: lowestNode.position.y + nodeDims.height + this.minNodeSpacing
        };
      }
      
      return { x: newX, y: rightmostNode.position.y };
    }
    
    // Fallback to simple positioning
    return { x: availableArea.x, y: availableArea.y };
  }

  /**
   * Check if a position would cause overlap with existing nodes
   */
  private wouldOverlap(
    position: { x: number; y: number }, 
    nodeDims: NodeDimensions, 
    existingNodes: Node[]
  ): boolean {
    const newNodeArea = {
      left: position.x,
      right: position.x + nodeDims.width,
      top: position.y,
      bottom: position.y + nodeDims.height
    };

    return existingNodes.some(node => {
      const existingNodeDims = CanvasLayoutManager.NODE_DIMENSIONS[node.type || 'default'] || 
                              CanvasLayoutManager.NODE_DIMENSIONS.default;
      
      const existingNodeArea = {
        left: node.position.x,
        right: node.position.x + existingNodeDims.width,
        top: node.position.y,
        bottom: node.position.y + existingNodeDims.height
      };

      // Check for overlap with margin
      return !(
        newNodeArea.right + this.minNodeSpacing < existingNodeArea.left ||
        newNodeArea.left > existingNodeArea.right + this.minNodeSpacing ||
        newNodeArea.bottom + this.minNodeSpacing < existingNodeArea.top ||
        newNodeArea.top > existingNodeArea.bottom + this.minNodeSpacing
      );
    });
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
  reorganizeNodes(nodes: Node[], canvasType: 'crew' | 'flow' | 'full' = 'full'): Node[] {
    const availableArea = this.getAvailableCanvasArea(canvasType);
    const agentNodes = nodes.filter(n => n.type === 'agentNode');
    const taskNodes = nodes.filter(n => n.type === 'taskNode');
    const flowNodes = nodes.filter(n => n.type === 'flowNode');
    const otherNodes = nodes.filter(n => !['agentNode', 'taskNode', 'flowNode'].includes(n.type || ''));

    const reorganizedNodes: Node[] = [...otherNodes]; // Keep other nodes as-is

    // Reorganize agents
    const agentDims = CanvasLayoutManager.NODE_DIMENSIONS.agentNode;
    agentNodes.forEach((node, index) => {
      reorganizedNodes.push({
        ...node,
        position: {
          x: availableArea.x,
          y: availableArea.y + index * (agentDims.height + this.minNodeSpacing)
        }
      });
    });

    // Reorganize tasks
    const taskDims = CanvasLayoutManager.NODE_DIMENSIONS.taskNode;
    const taskStartX = availableArea.x + agentDims.width + this.minNodeSpacing;
    taskNodes.forEach((node, index) => {
      reorganizedNodes.push({
        ...node,
        position: {
          x: taskStartX,
          y: availableArea.y + index * (taskDims.height + this.minNodeSpacing)
        }
      });
    });

    // Reorganize flow nodes
    const flowDims = CanvasLayoutManager.NODE_DIMENSIONS.flowNode;
    flowNodes.forEach((node, index) => {
      reorganizedNodes.push({
        ...node,
        position: {
          x: availableArea.x + index * (flowDims.width + this.minNodeSpacing),
          y: availableArea.y + 50
        }
      });
    });

    return reorganizedNodes;
  }

  /**
   * Scale positions to fit within available area if needed
   */
  scalePositionsToFit(
    positions: { x: number; y: number }[], 
    nodeDimensions: NodeDimensions,
    canvasType: 'crew' | 'flow' | 'full' = 'full'
  ): { x: number; y: number }[] {
    if (positions.length === 0) return positions;
    
    const availableArea = this.getAvailableCanvasArea(canvasType);
    
    // Find bounds of current positions
    const minX = Math.min(...positions.map(p => p.x));
    const maxX = Math.max(...positions.map(p => p.x + nodeDimensions.width));
    const minY = Math.min(...positions.map(p => p.y));
    const maxY = Math.max(...positions.map(p => p.y + nodeDimensions.height));
    
    const currentWidth = maxX - minX;
    const currentHeight = maxY - minY;
    
    // Calculate scale factors
    const scaleX = currentWidth > availableArea.width ? availableArea.width / currentWidth : 1;
    const scaleY = currentHeight > availableArea.height ? availableArea.height / currentHeight : 1;
    const scale = Math.min(scaleX, scaleY, 1); // Never scale up, only down
    
    // If no scaling needed, just return original positions
    if (scale >= 1) return positions;
    
    // Scale and reposition
    return positions.map(pos => ({
      x: availableArea.x + (pos.x - minX) * scale,
      y: availableArea.y + (pos.y - minY) * scale
    }));
  }

  /**
   * Get auto-fit zoom level for a given layout bounds
   */
  getAutoFitZoom(layoutBounds: { x: number; y: number; width: number; height: number }, canvasType: 'crew' | 'flow' | 'full' = 'full'): number {
    const availableArea = this.getAvailableCanvasArea(canvasType);
    
    // Use smaller padding for narrow screens
    const isNarrow = availableArea.width < 600;
    const padding = isNarrow ? 20 : 50;
    const usableWidth = Math.max(100, availableArea.width - (padding * 2));
    const usableHeight = Math.max(100, availableArea.height - (padding * 2));
    
    // Calculate zoom to fit both width and height
    const zoomX = layoutBounds.width > 0 ? usableWidth / layoutBounds.width : 1;
    const zoomY = layoutBounds.height > 0 ? usableHeight / layoutBounds.height : 1;
    
    // Use the smaller zoom to ensure everything fits, but allow more aggressive zoom for narrow screens
    const minZoom = isNarrow ? 0.3 : 0.5; // Allow smaller zoom for narrow screens
    const maxZoom = 1.0; // Never zoom in beyond 100%
    const calculatedZoom = Math.min(zoomX, zoomY);
    
    const finalZoom = Math.max(minZoom, Math.min(maxZoom, calculatedZoom));
    
    console.log(`[AutoFit] Available: ${availableArea.width}x${availableArea.height}, Layout: ${layoutBounds.width}x${layoutBounds.height}, Zoom: ${finalZoom}`);
    
    return finalZoom;
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
      recommendations.push('❌ CRITICAL: Canvas height is too small. Reduce execution history height!');
    } else if (availableAreas.crew.height < 400) {
      recommendations.push('⚠️ Canvas height is limited - consider reducing execution history height');
    }
    
    if (this.uiState.chatPanelVisible && !this.uiState.chatPanelCollapsed && this.uiState.chatPanelWidth > 350) {
      recommendations.push('💡 TIP: Reduce chat panel width or collapse it temporarily for better node visibility');
    }
    
    if (this.uiState.executionHistoryVisible && this.uiState.executionHistoryHeight > 200) {
      recommendations.push('💡 TIP: Reduce execution history height to give more space for nodes');
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