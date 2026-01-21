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
  chatPanelSide: 'left' | 'right'; // Which side the chat panel is docked to

  // Execution history
  executionHistoryVisible: boolean;
  executionHistoryHeight: number;  // Dynamic height

  // Panel splits (for dual canvas mode)
  panelPosition: number;           // 0-100% split between crew/flow panels
  areFlowsVisible: boolean;        // Whether flows panel is shown

  // Layout orientation for crew canvas
  layoutOrientation?: 'vertical' | 'horizontal';
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
      rightSidebarVisible: true,
      rightSidebarWidth: 48,

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

    // Subtract chat panel (overlay from the configured side)
    if (this.uiState.chatPanelVisible) {
      const chatWidth = this.uiState.chatPanelCollapsed
        ? this.uiState.chatPanelCollapsedWidth
        : this.uiState.chatPanelWidth;
      // Reduce available width regardless of side
      availableWidth -= chatWidth;
      // If docked on the left, also shift the available X start position
      if (this.uiState.chatPanelSide === 'left') {
        availableX += chatWidth;
      }
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
    const taskNodes = existingNodes.filter(node => node.type === 'taskNode');
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

    // Get current layout orientation from UI state
    // ALWAYS respect the layout orientation, even on narrow screens
    const currentLayout = this.uiState?.layoutOrientation || 'horizontal';

    // Only perform layout detection if there are 2+ agents
    // With just 1 agent, we can't determine the actual layout, so use the configured layout
    let effectiveLayout = currentLayout;

    if (agentNodes.length >= 2) {
      // Check if existing agents are properly aligned for the current layout
      const agentYPositions = agentNodes.map(n => n.position.y);
      const agentXPositions = agentNodes.map(n => n.position.x);
      const yVariance = Math.max(...agentYPositions) - Math.min(...agentYPositions);
      const xVariance = Math.max(...agentXPositions) - Math.min(...agentXPositions);

      // In vertical layout, agents should have similar Y (low variance) and different X (high variance)
      // In horizontal layout, agents should have similar X (low variance) and different Y (high variance)
      // Use stricter thresholds: X variance < 10 for horizontal (agents in same column)
      // Y variance < 10 for vertical (agents in same row)
      const isProperlyAlignedForVertical = yVariance < 10 && xVariance > 150;
      const isProperlyAlignedForHorizontal = xVariance < 10 && yVariance > 150;

      console.log('[CanvasLayoutManager] getAgentNodePosition - Layout Analysis:', {
        currentLayout,
        existingAgents: agentNodes.length,
        existingTasks: taskNodes.length,
        yVariance,
        xVariance,
        isProperlyAlignedForVertical,
        isProperlyAlignedForHorizontal,
        agentPositions: agentNodes.map(n => ({ id: n.id.substring(0, 20), x: Math.round(n.position.x), y: Math.round(n.position.y) })),
        taskPositions: taskNodes.map(n => ({ id: n.id.substring(0, 20), x: Math.round(n.position.x), y: Math.round(n.position.y) }))
      });

      // Detect if we need to adapt to the actual layout vs the configured layout
      // If agents are misaligned for the current layout, position based on actual layout
      const useActualLayout = currentLayout === 'vertical' ? !isProperlyAlignedForVertical : !isProperlyAlignedForHorizontal;

      // Determine effective layout: use actual layout if misaligned, otherwise use configured layout
      effectiveLayout = useActualLayout
        ? (isProperlyAlignedForHorizontal ? 'horizontal' : 'vertical')
        : currentLayout;

      console.log('[CanvasLayoutManager] Layout Decision:', {
        configuredLayout: currentLayout,
        effectiveLayout,
        useActualLayout,
        reason: useActualLayout
          ? `Agents are misaligned for ${currentLayout} layout (yVar=${yVariance}, xVar=${xVariance}). Using ${effectiveLayout} layout instead.`
          : `Agents are properly aligned for ${currentLayout} layout.`
      });
    } else {
      console.log('[CanvasLayoutManager] Layout Decision:', {
        configuredLayout: currentLayout,
        effectiveLayout,
        reason: 'Only 1 agent exists, using configured layout.'
      });
    }

    if (effectiveLayout === 'vertical') {
      // Vertical layout: agents in a row at the top, tasks below
      // Place new agent to the right of existing agents (same Y, increasing X)

      // Sort agents by X position to find the rightmost one
      const sortedAgents = [...agentNodes].sort((a, b) => a.position.x - b.position.x);
      const rightmostAgent = sortedAgents[sortedAgents.length - 1];

      // Get the Y position - use the minimum Y among all agents to ensure alignment at the top row
      // Round to nearest integer to avoid floating point precision issues
      const minAgentY = Math.min(...agentNodes.map(n => n.position.y));
      const agentRowY = Math.round(minAgentY);

      // Calculate X position to the right of the rightmost agent
      // Use larger spacing in vertical layout to accommodate wider tasks (220px) under agents (200px)
      // Need at least taskWidth + gap between task columns
      const taskDims = CanvasLayoutManager.NODE_DIMENSIONS.taskNode;
      const verticalAgentSpacing = Math.max(spacing, taskDims.width + 80); // Task width + 80px gap
      const newX = rightmostAgent.position.x + nodeDims.width + verticalAgentSpacing;

      const newPosition = {
        x: Math.round(newX),
        y: agentRowY
      };

      console.log('[CanvasLayoutManager] ✅ Vertical Layout - New Agent Position:', {
        rightmostAgent: { x: rightmostAgent.position.x, y: rightmostAgent.position.y },
        agentWidth: nodeDims.width,
        taskWidth: taskDims.width,
        verticalAgentSpacing,
        minAgentY,
        agentRowY,
        newPosition,
        allAgents: agentNodes.map(n => ({ id: n.id.substring(0, 20), x: Math.round(n.position.x), y: Math.round(n.position.y) }))
      });
      return newPosition;
    } else {
      // Horizontal layout: agents in left column, tasks in right column
      // ALWAYS place new agent underneath existing agents (same X, increasing Y)
      // Simple rule: stack agents vertically, no exceptions

      // Find the bottommost agent
      const maxAgentY = Math.max(...agentNodes.map(n => n.position.y));
      const bottommostAgent = agentNodes.find(n => n.position.y === maxAgentY);

      // Get the X position from existing agents (they should all be at the same X in horizontal mode)
      const agentColumnX = bottommostAgent?.position.x || availableArea.x + spacing;

      // Calculate new Y position below the bottommost agent
      // Simple: just add height + spacing, no complex logic
      const newY = maxAgentY + nodeDims.height + spacing;

      const newPosition = {
        x: Math.round(agentColumnX),
        y: Math.round(newY)
      };

      console.log('[CanvasLayoutManager] ✅ Horizontal Layout - New Agent Position:', {
        bottommostAgent: { x: Math.round(bottommostAgent?.position.x || 0), y: Math.round(maxAgentY) },
        agentColumnX: Math.round(agentColumnX),
        newY: Math.round(newY),
        newPosition,
        allAgents: agentNodes.map(n => ({ id: n.id.substring(0, 20), x: Math.round(n.position.x), y: Math.round(n.position.y) }))
      });
      return newPosition;
    }
  }

  /**
   * Get optimal position for a new task node
   * Tasks are distributed under agents in a round-robin fashion:
   * - 1st task goes under 1st agent
   * - 2nd task goes under 2nd agent
   * - 3rd task goes under 3rd agent
   * - 4th task goes under 1st agent (below existing tasks)
   * etc.
   */
  getTaskNodePosition(existingNodes: Node[], canvasType: 'crew' | 'flow' | 'full' = 'crew'): { x: number; y: number } {
    const availableArea = this.getAvailableCanvasArea(canvasType);
    const taskNodes = existingNodes.filter(node => node.type === 'taskNode');
    const agentNodes = existingNodes.filter(node => node.type === 'agentNode');
    const agentDims = CanvasLayoutManager.NODE_DIMENSIONS.agentNode;
    const taskDims = CanvasLayoutManager.NODE_DIMENSIONS.taskNode;
    const isNarrow = availableArea.width < 600;
    const spacing = isNarrow ? Math.max(20, this.minNodeSpacing / 2) : this.minNodeSpacing;

    // Get current layout orientation from UI state
    const currentLayout = this.uiState?.layoutOrientation || 'horizontal';

    // Only perform layout detection if there are 2+ agents
    let effectiveLayout = currentLayout;

    if (agentNodes.length >= 2) {
      // Check if existing agents are properly aligned for the current layout
      const agentYPositions = agentNodes.map(n => n.position.y);
      const agentXPositions = agentNodes.map(n => n.position.x);
      const yVariance = Math.max(...agentYPositions) - Math.min(...agentYPositions);
      const xVariance = Math.max(...agentXPositions) - Math.min(...agentXPositions);

      const isProperlyAlignedForVertical = yVariance < 10 && xVariance > 150;
      const isProperlyAlignedForHorizontal = xVariance < 10 && yVariance > 150;

      // Detect if we need to adapt to the actual layout vs the configured layout
      const useActualLayout = currentLayout === 'vertical' ? !isProperlyAlignedForVertical : !isProperlyAlignedForHorizontal;

      // Determine effective layout
      effectiveLayout = useActualLayout
        ? (isProperlyAlignedForHorizontal ? 'horizontal' : 'vertical')
        : currentLayout;

      console.log('[CanvasLayoutManager] getTaskNodePosition:', {
        currentLayout,
        effectiveLayout,
        useActualLayout,
        existingAgents: agentNodes.length,
        existingTasks: taskNodes.length,
        yVariance,
        xVariance,
        agentPositions: agentNodes.map(n => ({ id: n.id.substring(0, 20), x: Math.round(n.position.x), y: Math.round(n.position.y) })),
        taskPositions: taskNodes.map(n => ({ id: n.id.substring(0, 20), x: Math.round(n.position.x), y: Math.round(n.position.y) }))
      });
    } else {
      console.log('[CanvasLayoutManager] getTaskNodePosition:', {
        currentLayout,
        effectiveLayout,
        existingAgents: agentNodes.length,
        existingTasks: taskNodes.length,
        reason: agentNodes.length === 0 ? 'No agents' : 'Only 1 agent, using configured layout',
        agentPositions: agentNodes.map(n => ({ id: n.id.substring(0, 20), x: Math.round(n.position.x), y: Math.round(n.position.y) })),
        taskPositions: taskNodes.map(n => ({ id: n.id.substring(0, 20), x: Math.round(n.position.x), y: Math.round(n.position.y) }))
      });
    }

    if (agentNodes.length === 0) {
      // No agents, position task in default location
      return {
        x: availableArea.x + spacing,
        y: availableArea.y + spacing
      };
    }

    // Sort agents by position (left to right in vertical, top to bottom in horizontal)
    const sortedAgents = [...agentNodes].sort((a, b) => {
      if (effectiveLayout === 'vertical') {
        // In vertical layout, agents are in a row (sort by X)
        return a.position.x - b.position.x;
      } else {
        // In horizontal layout, agents are in a column (sort by Y)
        return a.position.y - b.position.y;
      }
    });

    // Determine which agent this task should go under (round-robin)
    const agentIndex = taskNodes.length % sortedAgents.length;
    const targetAgent = sortedAgents[agentIndex];

    console.log('[CanvasLayoutManager] Task assignment:', {
      taskNumber: taskNodes.length + 1,
      totalTasks: taskNodes.length,
      totalAgents: sortedAgents.length,
      agentIndex,
      targetAgentId: targetAgent.id.substring(0, 30),
      targetAgentPos: { x: Math.round(targetAgent.position.x), y: Math.round(targetAgent.position.y) },
      allAgents: sortedAgents.map(a => ({
        id: a.id.substring(0, 30),
        x: Math.round(a.position.x),
        y: Math.round(a.position.y)
      })),
      existingTasks: taskNodes.map(t => ({
        id: t.id.substring(0, 30),
        x: Math.round(t.position.x),
        y: Math.round(t.position.y)
      }))
    });

    if (effectiveLayout === 'vertical') {
      // Vertical layout: tasks go below their assigned agent
      // Use larger spacing for vertical task stacking to match reorganizeNodes behavior
      const verticalTaskSpacing = Math.max(spacing, 100);

      // Center task under agent (task is wider than agent by 20px)
      // Agent width: 200px, Task width: 220px
      // Offset task X by -10px to center it under the agent
      const taskXOffset = (taskDims.width - agentDims.width) / 2;
      const taskX = targetAgent.position.x - taskXOffset;

      // Find how many tasks are already under this agent (within same column)
      const columnTolerance = 50; // Allow some tolerance for column detection
      const tasksUnderThisAgent = taskNodes.filter(t =>
        Math.abs(t.position.x - taskX) < columnTolerance
      ).sort((a, b) => a.position.y - b.position.y); // Sort by Y position

      // Calculate Y position:
      // If no tasks under this agent: agent Y + agent height + spacing
      // If tasks exist: position below the bottommost task
      let taskY: number;

      if (tasksUnderThisAgent.length === 0) {
        // First task under this agent
        const agentBottomY = targetAgent.position.y + agentDims.height;
        taskY = agentBottomY + verticalTaskSpacing;
      } else {
        // Position below the last task in this column
        const lastTask = tasksUnderThisAgent[tasksUnderThisAgent.length - 1];
        taskY = lastTask.position.y + taskDims.height + verticalTaskSpacing;
      }

      const newPosition = {
        x: Math.round(taskX),
        y: Math.round(taskY)
      };

      console.log('[CanvasLayoutManager] Vertical layout - new task position:', {
        agentX: Math.round(targetAgent.position.x),
        agentWidth: agentDims.width,
        taskWidth: taskDims.width,
        taskXOffset,
        taskX: Math.round(taskX),
        agentY: Math.round(targetAgent.position.y),
        agentHeight: agentDims.height,
        verticalTaskSpacing,
        tasksUnderThisAgent: tasksUnderThisAgent.length,
        lastTaskY: tasksUnderThisAgent.length > 0 ? Math.round(tasksUnderThisAgent[tasksUnderThisAgent.length - 1].position.y) : 'none',
        newPosition
      });

      return newPosition;
    } else {
      // Horizontal layout: tasks go to the right of their assigned agent
      // Find how many tasks are already in the same row as this agent
      const agentY = targetAgent.position.y;
      const rowTolerance = 50; // Allow some tolerance for row detection
      const tasksInThisRow = taskNodes.filter(t =>
        Math.abs(t.position.y - agentY) < rowTolerance
      ).sort((a, b) => a.position.x - b.position.x); // Sort by X position

      // Calculate X position:
      // If no tasks in this row: agent X + agent width + spacing
      // If tasks exist: position to the right of the rightmost task
      let taskX: number;

      if (tasksInThisRow.length === 0) {
        // First task in this row
        taskX = targetAgent.position.x + agentDims.width + spacing;
      } else {
        // Position to the right of the last task in this row
        const lastTask = tasksInThisRow[tasksInThisRow.length - 1];
        taskX = lastTask.position.x + taskDims.width + spacing;
      }

      const newPosition = {
        x: taskX,
        y: agentY
      };

      console.log('[CanvasLayoutManager] Horizontal layout - new task position:', {
        agentY,
        agentX: targetAgent.position.x,
        agentWidth: agentDims.width,
        tasksInThisRow: tasksInThisRow.length,
        lastTaskX: tasksInThisRow.length > 0 ? tasksInThisRow[tasksInThisRow.length - 1].position.x : 'none',
        newPosition
      });

      return newPosition;
    }
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
   * Get optimal position for a new flow node
   */
  getFlowNodePosition(existingNodes: Node[], canvasType: 'flow' | 'full' = 'flow'): { x: number; y: number } {
    const availableArea = this.getAvailableCanvasArea(canvasType);
    // Flow canvas uses 'crewNode' type
    const flowNodes = existingNodes.filter(node => node.type === 'crewNode');
    const nodeDims = CanvasLayoutManager.NODE_DIMENSIONS.crewNode;

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
  reorganizeNodes(nodes: Node[], canvasType: 'crew' | 'flow' | 'full' = 'full', edges: any[] = []): Node[] {
    const availableArea = this.getAvailableCanvasArea(canvasType);
    const agentNodes = nodes.filter(n => n.type === 'agentNode');
    const managerNodes = nodes.filter(n => n.type === 'managerNode');
    const taskNodes = nodes.filter(n => n.type === 'taskNode');
    // Flow canvas uses 'crewNode' type, not 'flowNode'
    const flowNodes = nodes.filter(n => n.type === 'crewNode');
    const otherNodes = nodes.filter(n => !['agentNode', 'managerNode', 'taskNode', 'crewNode'].includes(n.type || ''));

    // Keep manager nodes as-is (their position is managed by useManagerNode hook)
    const reorganizedNodes: Node[] = [...otherNodes, ...managerNodes];

    const agentDims = CanvasLayoutManager.NODE_DIMENSIONS.agentNode;
    const taskDims = CanvasLayoutManager.NODE_DIMENSIONS.taskNode;
    // Use crewNode dimensions for flow canvas nodes
    const flowDims = CanvasLayoutManager.NODE_DIMENSIONS.crewNode;

    const orientation = this.uiState.layoutOrientation || 'horizontal';

    if (orientation === 'horizontal') {
      // Horizontal layout: agents left column, tasks right column (side by side)
      // Each agent aligns with its first connected task

      // Build map of agent -> tasks from edges
      const agentTaskMap = new Map<string, string[]>();
      edges.forEach(edge => {
        const isAgentToTask = edge.source?.startsWith('agent-') && edge.target?.startsWith('task-');
        if (isAgentToTask) {
          const tasks = agentTaskMap.get(edge.source) || [];
          tasks.push(edge.target);
          agentTaskMap.set(edge.source, tasks);
        }
      });

      const taskSpacing = Math.max(this.minNodeSpacing, 80);
      const taskStartX = availableArea.x + agentDims.width + this.minNodeSpacing * 2;

      let currentTaskY = availableArea.y;
      const taskPositions = new Map<string, { x: number; y: number }>();

      // Process each agent and its tasks
      agentNodes.forEach((agentNode) => {
        const connectedTaskIds = agentTaskMap.get(agentNode.id) || [];
        const connectedTasks = taskNodes.filter(t => connectedTaskIds.includes(t.id));

        if (connectedTasks.length > 0) {
          // Calculate the starting Y position for tasks
          const taskStartY = currentTaskY;

          // Position this agent's tasks (top to bottom)
          connectedTasks.forEach((taskNode) => {
            taskPositions.set(taskNode.id, { x: taskStartX, y: currentTaskY });
            reorganizedNodes.push({
              ...taskNode,
              position: { x: taskStartX, y: currentTaskY }
            });
            currentTaskY += taskDims.height + taskSpacing;
          });

          // Calculate agent Y position to center it between task handles
          // Task handles are at the vertical center of each task
          const taskCenters = connectedTasks.map((_, idx) =>
            taskStartY + (idx * (taskDims.height + taskSpacing)) + (taskDims.height / 2)
          );

          // Find the midpoint between first and last task centers
          const firstTaskCenter = taskCenters[0];
          const lastTaskCenter = taskCenters[taskCenters.length - 1];
          const midpointOfTaskCenters = (firstTaskCenter + lastTaskCenter) / 2;

          // Position agent so its center (handle) aligns with the midpoint
          // Adjust slightly higher in horizontal view for better visual balance
          const verticalOffset = -10; // Move agent 10px higher
          const agentY = midpointOfTaskCenters - (agentDims.height / 2) + verticalOffset;

          console.log(`[Layout] Agent ${agentNode.id} centering:`, {
            taskCount: connectedTasks.length,
            taskStartY,
            taskDims,
            agentDims,
            taskSpacing,
            taskCenters,
            firstTaskCenter,
            lastTaskCenter,
            midpointOfTaskCenters,
            agentY,
            agentCenter: agentY + (agentDims.height / 2),
            taskPositions: connectedTasks.map(t => taskPositions.get(t.id))
          });

          reorganizedNodes.push({
            ...agentNode,
            position: { x: availableArea.x, y: agentY }
          });
        } else {
          // Agent with no tasks - position at current Y
          reorganizedNodes.push({
            ...agentNode,
            position: { x: availableArea.x, y: currentTaskY }
          });
          currentTaskY += agentDims.height + taskSpacing;
        }
      });

      // Add any unconnected tasks at the end
      const connectedTaskIds = new Set(Array.from(agentTaskMap.values()).flat());
      const unconnectedTasks = taskNodes.filter(t => !connectedTaskIds.has(t.id));
      unconnectedTasks.forEach((taskNode) => {
        reorganizedNodes.push({
          ...taskNode,
          position: { x: taskStartX, y: currentTaskY }
        });
        currentTaskY += taskDims.height + taskSpacing;
      });

      // Manager node position is handled by useManagerNode hook, not here

      // Flow nodes (if any) - horizontal layout: left to right
      // Sort by explicit order field first, then fallback to Y position for legacy nodes
      const sortedFlowNodes = [...flowNodes].sort((a, b) => {
        const orderA = a.data?.order ?? Number.MAX_SAFE_INTEGER;
        const orderB = b.data?.order ?? Number.MAX_SAFE_INTEGER;
        if (orderA !== orderB) return orderA - orderB;
        // Fallback: sort by current Y position to preserve top-to-bottom order
        return a.position.y - b.position.y;
      });
      sortedFlowNodes.forEach((node, index) => {
        reorganizedNodes.push({
          ...node,
          position: {
            x: availableArea.x + index * (flowDims.width + this.minNodeSpacing),
            y: availableArea.y + 50
          }
        });
      });
    } else {
      // Vertical layout: agents above their connected tasks, centered
      // Build map of agent -> tasks from edges
      const agentTaskMap = new Map<string, string[]>();

      edges.forEach(edge => {
        const isAgentToTask = edge.source?.startsWith('agent-') && edge.target?.startsWith('task-');
        if (isAgentToTask) {
          const tasks = agentTaskMap.get(edge.source) || [];
          tasks.push(edge.target);
          agentTaskMap.set(edge.source, tasks);
        }
      });

      const taskSpacing = Math.max(this.minNodeSpacing, 100);
      const agentRowY = availableArea.y;
      const taskRowY = agentRowY + agentDims.height + this.minNodeSpacing * 2;

      let currentTaskX = availableArea.x;
      const taskPositions = new Map<string, { x: number; y: number }>();

      // Process each agent and its tasks
      agentNodes.forEach((agentNode) => {
        const connectedTaskIds = agentTaskMap.get(agentNode.id) || [];
        const connectedTasks = taskNodes.filter(t => connectedTaskIds.includes(t.id));

        // Position this agent's tasks (left to right)
        const agentTaskStartX = currentTaskX;
        connectedTasks.forEach((taskNode) => {
          const taskX = currentTaskX;
          taskPositions.set(taskNode.id, { x: taskX, y: taskRowY });
          reorganizedNodes.push({
            ...taskNode,
            position: { x: taskX, y: taskRowY }
          });
          currentTaskX += taskDims.width + taskSpacing;
        });

        // Position agent centered above its tasks
        if (connectedTasks.length > 0) {
          const agentTaskEndX = currentTaskX - taskSpacing;
          // Adjust slightly to the right in vertical view for better visual balance
          const horizontalOffset = 35; // Move agent 30px to the right
          const agentX = agentTaskStartX + (agentTaskEndX - agentTaskStartX) / 2 - agentDims.width / 2 + horizontalOffset;
          reorganizedNodes.push({
            ...agentNode,
            position: { x: agentX, y: agentRowY }
          });
        } else {
          // Agent with no tasks - position at current X
          reorganizedNodes.push({
            ...agentNode,
            position: { x: currentTaskX, y: agentRowY }
          });
          currentTaskX += agentDims.width + taskSpacing;
        }
      });

      // Add any unconnected tasks at the end
      const connectedTaskIds = new Set(Array.from(agentTaskMap.values()).flat());
      const unconnectedTasks = taskNodes.filter(t => !connectedTaskIds.has(t.id));
      unconnectedTasks.forEach((taskNode) => {
        reorganizedNodes.push({
          ...taskNode,
          position: { x: currentTaskX, y: taskRowY }
        });
        currentTaskX += taskDims.width + taskSpacing;
      });

      // Manager node position is handled by useManagerNode hook, not here

      // Flow nodes (if any) - vertical layout: top to bottom
      // Sort by explicit order field first, then fallback to X position for legacy nodes
      const sortedFlowNodes = [...flowNodes].sort((a, b) => {
        const orderA = a.data?.order ?? Number.MAX_SAFE_INTEGER;
        const orderB = b.data?.order ?? Number.MAX_SAFE_INTEGER;
        if (orderA !== orderB) return orderA - orderB;
        // Fallback: sort by current X position to preserve left-to-right order
        return a.position.x - b.position.x;
      });
      sortedFlowNodes.forEach((node, index) => {
        reorganizedNodes.push({
          ...node,
          position: {
            x: availableArea.x + 50,
            y: availableArea.y + index * (flowDims.height + this.minNodeSpacing)
          }
        });
      });
    }

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