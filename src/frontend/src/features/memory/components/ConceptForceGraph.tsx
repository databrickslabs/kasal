/**
 * Force-directed concept graph for memory categories.
 *
 * Built on `force-graph` (vasturiano) — canvas rendering, d3-force physics,
 * native pan/zoom/drag. We keep MUI overlays for the legend, zoom controls,
 * fullscreen toggle and the hover tooltip so the component blends with the
 * rest of the Memory Browser.
 *
 * Visuals follow the same warm and sage surfaces as builder nodes:
 *  - Subtle concept colours with theme-aware labels
 *  - Sparse edge set (top weights only) so dense graphs stay readable
 *  - Dashed selection ring for pinned/active concepts
 *  - Hover dim with neighbour-highlight
 */

import React, {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import { Box, IconButton, Tooltip, Typography } from '@mui/material';
import { alpha, useTheme } from '@mui/material/styles';
import { kasalNodePalette } from '../../../theme/kasalSurfaces';
import AddIcon from '@mui/icons-material/Add';
import RemoveIcon from '@mui/icons-material/Remove';
import CenterFocusStrongIcon from '@mui/icons-material/CenterFocusStrong';
import FullscreenIcon from '@mui/icons-material/Fullscreen';
import FullscreenExitIcon from '@mui/icons-material/FullscreenExit';
import ForceGraph2D from 'force-graph';

export interface ConceptGraphNode {
  id: string;
  label: string;
  count: number;
  avgImportance: number;
}

export interface ConceptGraphEdge {
  source: string;
  target: string;
  weight: number;
}

interface PhysicsNode extends ConceptGraphNode {
  /** Pre-split label lines so we don't recompute every frame. */
  lines: string[];
  radius: number;
  // Mutated by force-graph
  x?: number; y?: number; vx?: number; vy?: number; fx?: number; fy?: number;
}

interface PhysicsLink {
  source: string | PhysicsNode;
  target: string | PhysicsNode;
  weight: number;
}

interface Props {
  nodes: ConceptGraphNode[];
  edges: ConceptGraphEdge[];
  activeIds: Set<string>;
  onToggleNode: (id: string) => void;
  importanceColor: (value: number) => string;
  height?: number;
}

// Only render edges whose weight ≥ this fraction of the maximum.
// Hides the long tail of weak co-occurrences that turns dense graphs into
// a tangled web without losing the meaningful connections.
const EDGE_DISPLAY_RATIO = 0.07;

const LABEL_FONT   = 11;
const LABEL_LINE_H = 13;
const LABEL_PAD    = 6;
const LABEL_CHAR_W = LABEL_FONT * 0.6;

const ZOOM_STEP = 1.3;

// djb2 — fast, decent distribution, deterministic per id.
function hashId(id: string): number {
  let h = 5381;
  for (let i = 0; i < id.length; i++) h = ((h << 5) + h + id.charCodeAt(i)) | 0;
  return Math.abs(h);
}

function splitLabel(label: string): string[] {
  const parts = label.split(/[_\-\s/]+/).map((p) => p.trim()).filter(Boolean);
  return parts.length ? parts : [label];
}

function requiredRadiusForLabel(lines: string[]): number {
  const longest = lines.reduce((m, l) => Math.max(m, l.length), 0);
  const w = longest * LABEL_CHAR_W;
  const h = lines.length * LABEL_LINE_H;
  return Math.sqrt(w * w + h * h) / 2 + LABEL_PAD;
}

// Minimal subset of the force-graph instance API we touch — keeps the file
// strongly typed without depending on the library's full generic surface.
interface ForceGraphInstance {
  width(n: number): ForceGraphInstance;
  height(n: number): ForceGraphInstance;
  backgroundColor(c: string): ForceGraphInstance;
  nodeId(id: string): ForceGraphInstance;
  nodeVal(fn: (n: PhysicsNode) => number): ForceGraphInstance;
  nodeRelSize(n: number): ForceGraphInstance;
  nodeCanvasObject(fn: (n: PhysicsNode, ctx: CanvasRenderingContext2D, scale: number) => void): ForceGraphInstance;
  nodePointerAreaPaint(fn: (n: PhysicsNode, color: string, ctx: CanvasRenderingContext2D) => void): ForceGraphInstance;
  linkCanvasObject(fn: (l: PhysicsLink, ctx: CanvasRenderingContext2D, scale: number) => void): ForceGraphInstance;
  linkCanvasObjectMode(fn: () => 'replace' | 'before' | 'after'): ForceGraphInstance;
  onNodeClick(fn: (n: PhysicsNode) => void): ForceGraphInstance;
  onNodeHover(fn: (n: PhysicsNode | null) => void): ForceGraphInstance;
  onEngineTick(fn: () => void): ForceGraphInstance;
  onEngineStop(fn: () => void): ForceGraphInstance;
  graphData(): { nodes: PhysicsNode[]; links: PhysicsLink[] };
  graphData(data: { nodes: PhysicsNode[]; links: PhysicsLink[] }): ForceGraphInstance;
  zoom(): number;
  zoom(level: number, ms?: number): ForceGraphInstance;
  zoomToFit(ms?: number, padding?: number): ForceGraphInstance;
  d3Force(name: string): { strength?: (s: number) => unknown; distance?: (d: number) => unknown } | undefined;
  d3AlphaDecay(d: number): ForceGraphInstance;
  d3VelocityDecay(d: number): ForceGraphInstance;
  d3ReheatSimulation(): ForceGraphInstance;
  cooldownTicks(n: number): ForceGraphInstance;
  warmupTicks(n: number): ForceGraphInstance;
  minZoom(n: number): ForceGraphInstance;
  maxZoom(n: number): ForceGraphInstance;
  enableNodeDrag(b: boolean): ForceGraphInstance;
  _destructor?: () => void;
}

type ForceGraphFactory = () => (el: HTMLElement) => ForceGraphInstance;

export const ConceptForceGraph: React.FC<Props> = ({
  nodes,
  edges,
  activeIds,
  onToggleNode,
  importanceColor,
  height = 520,
}) => {
  const theme = useTheme();
  const dark = theme.palette.mode === 'dark';
  const agentPalette = kasalNodePalette(dark, 'agent');
  const taskPalette = kasalNodePalette(dark, 'task');
  // Read current colours inside the persistent canvas callbacks. Changing the
  // theme should repaint the graph without resetting node positions or pins.
  const colorsRef = useRef({ fills: [] as string[], text: '', edge: '', focus: '', shadow: '' });
  useEffect(() => {
    colorsRef.current = {
      fills: [agentPalette.surface, taskPalette.surface, agentPalette.badge, taskPalette.badge],
      text: theme.palette.text.primary, edge: theme.palette.text.secondary,
      focus: taskPalette.accent, shadow: alpha(theme.palette.common.black, dark ? 0.22 : 0.08),
    };
  }, [theme, agentPalette.surface, agentPalette.badge, taskPalette.surface, taskPalette.badge, taskPalette.accent, dark]);
  const containerRef = useRef<HTMLDivElement | null>(null);
  const graphRef     = useRef<ForceGraphInstance | null>(null);
  const initialFitRef = useRef(true);
  const settleFitRef = useRef(true);
  const fitGraph = useCallback((duration = 0) => {
    const graph = graphRef.current;
    const bounds = containerRef.current?.getBoundingClientRect();
    if (!graph || !bounds || bounds.width < 1 || bounds.height < 1) return;
    const positioned = graph.graphData().nodes;
    if (!positioned.length || positioned.some(node => !Number.isFinite(node.x) || !Number.isFinite(node.y))) return;
    graph.zoomToFit(duration, Math.min(56, bounds.width / 5, bounds.height / 5));
  }, []);

  const [hoveredId,    setHoveredId]    = useState<string | null>(null);
  const [isFullscreen, setIsFullscreen] = useState(false);

  // Refs accessed inside canvas callbacks — kept current via effects so the
  // render closure never needs rebinding.
  const activeIdsRef       = useRef(activeIds);
  const hoveredIdRef       = useRef<string | null>(null);
  const neighboursRef      = useRef<Map<string, Set<string>>>(new Map());
  const maxEdgeWeightRef   = useRef(1);
  const importanceColorRef = useRef(importanceColor);

  useEffect(() => { activeIdsRef.current       = activeIds;       }, [activeIds]);
  useEffect(() => { hoveredIdRef.current       = hoveredId;       }, [hoveredId]);
  useEffect(() => { importanceColorRef.current = importanceColor; }, [importanceColor]);

  // Sparse edges + adjacency built from the full edge set.
  const { displayEdges, neighbours, maxEdgeWeight } = useMemo(() => {
    const maxW   = Math.max(1, ...edges.map((e) => e.weight));
    const cutoff = maxW * EDGE_DISPLAY_RATIO;
    const display = edges.filter((e) => e.weight >= cutoff);
    const nbrs    = new Map<string, Set<string>>();
    for (const e of edges) {
      if (!nbrs.has(e.source)) nbrs.set(e.source, new Set());
      if (!nbrs.has(e.target)) nbrs.set(e.target, new Set());
      nbrs.get(e.source)!.add(e.target);
      nbrs.get(e.target)!.add(e.source);
    }
    return { displayEdges: display, neighbours: nbrs, maxEdgeWeight: maxW };
  }, [edges]);

  useEffect(() => { neighboursRef.current    = neighbours;    }, [neighbours]);
  useEffect(() => { maxEdgeWeightRef.current = maxEdgeWeight; }, [maxEdgeWeight]);

  // Build the physics node payload. We re-derive radius here so
  // canvas callbacks can read them straight off the node object.
  const physicsNodes = useMemo<PhysicsNode[]>(() => {
    const maxCount = nodes.reduce((m, n) => Math.max(m, n.count), 1);
    return nodes.map((n) => {
      const lines  = splitLabel(n.label);
      const baseR  = 14 + Math.sqrt(n.count / maxCount) * 22;
      const radius = Math.max(baseR, requiredRadiusForLabel(lines));
      return { ...n, lines, radius };
    });
  }, [nodes]);

  // ---- Initialise force-graph once ----
  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    const Factory = ForceGraph2D as unknown as ForceGraphFactory;
    const g       = Factory()(container);
    graphRef.current = g;

    g.backgroundColor('rgba(0,0,0,0)')
      .nodeId('id')
      .nodeRelSize(1)
      .nodeVal((n: PhysicsNode) => n.radius * n.radius)
      .nodeCanvasObject(drawNode)
      .nodePointerAreaPaint(paintNodeHit)
      .linkCanvasObject(drawLink)
      .linkCanvasObjectMode(() => 'replace')
      .onNodeClick((n: PhysicsNode) => onToggleNodeRef.current(n.id))
      .onNodeHover((n) => setHoveredId(n?.id ?? null))
      .onEngineTick(() => {
        if (!initialFitRef.current) return;
        fitGraph();
        initialFitRef.current = false;
      })
      .onEngineStop(() => {
        if (settleFitRef.current) fitGraph(250);
        settleFitRef.current = false;
      })
      .minZoom(0.001)
      .maxZoom(6)
      .enableNodeDrag(true)
      .d3AlphaDecay(0.035)
      .d3VelocityDecay(0.55)
      .warmupTicks(60)
      .cooldownTicks(180);

    // Physics tuning to match the previous look: medium repulsion, springy
    // edges with a comfortable rest length, gentle centring.
    const charge = g.d3Force('charge');
    if (charge?.strength) charge.strength(-380);
    const link = g.d3Force('link');
    if (link?.distance) link.distance(140);
    const centre = g.d3Force('center');
    if (centre?.strength) centre.strength(0.04);

    // Keep the canvas sized to its container.
    let resizeFrame = 0;
    let previousWidth = 0;
    let previousHeight = 0;
    const sync = () => {
      const r = container.getBoundingClientRect();
      if (r.width < 1 || r.height < 1 || (r.width === previousWidth && r.height === previousHeight)) return;
      previousWidth = r.width; previousHeight = r.height;
      g.width(r.width).height(r.height);
      cancelAnimationFrame(resizeFrame);
      resizeFrame = requestAnimationFrame(() => fitGraph());
    };
    sync();
    const ro = new ResizeObserver(sync);
    ro.observe(container);
    // Native pan/zoom/drag takes ownership of the viewport. A delayed layout
    // completion must not undo a user's adjustment.
    const takeControl = () => { initialFitRef.current = false; settleFitRef.current = false; };
    container.addEventListener('pointerdown', takeControl);
    container.addEventListener('wheel', takeControl, { passive: true });

    return () => {
      ro.disconnect();
      cancelAnimationFrame(resizeFrame);
      container.removeEventListener('pointerdown', takeControl);
      container.removeEventListener('wheel', takeControl);
      g._destructor?.();
      graphRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Click handler is mutable — keep it behind a ref so the graph init effect
  // can stay one-shot.
  const onToggleNodeRef = useRef(onToggleNode);
  useEffect(() => { onToggleNodeRef.current = onToggleNode; }, [onToggleNode]);

  // ---- Sync data ----
  useEffect(() => {
    const g = graphRef.current;
    if (!g) return;

    // Preserve positions for nodes that already exist by copying x/y/vx/vy
    // off the previous force-graph data array (force-graph mutates these).
    const prev    = g.graphData();
    const prevMap = new Map(prev.nodes.map((n) => [n.id, n]));
    if (prev.nodes.length !== physicsNodes.length || physicsNodes.some(node => !prevMap.has(node.id))) {
      initialFitRef.current = true;
      settleFitRef.current = true;
    }
    for (const n of physicsNodes) {
      const p = prevMap.get(n.id);
      if (p) { n.x = p.x; n.y = p.y; n.vx = p.vx; n.vy = p.vy; }
    }
    // Feed FRESH link objects with string-id endpoints. force-graph MUTATES a
    // link's source/target from id to the resolved node object — so a reused
    // link array on a later feed still points at the PREVIOUS feed's node
    // objects, whose positions never advance again: the nodes sail on while
    // their edges stay frozen mid-air (the "disconnected graph" bug when a
    // host re-renders with stable edges but fresh nodes).
    const links = (displayEdges as PhysicsLink[]).map((e) => ({
      source: typeof e.source === 'object' ? (e.source as PhysicsNode).id : e.source,
      target: typeof e.target === 'object' ? (e.target as PhysicsNode).id : e.target,
      weight: e.weight,
    }));
    g.graphData({ nodes: physicsNodes, links: links as PhysicsLink[] });
  }, [physicsNodes, displayEdges]);

  // ---- Canvas drawing ----

  const drawLink = useCallback(
    (link: PhysicsLink, ctx: CanvasRenderingContext2D) => {
      const a = link.source as PhysicsNode;
      const b = link.target as PhysicsNode;
      if (typeof a !== 'object' || typeof b !== 'object') return;
      if (a.x == null || a.y == null || b.x == null || b.y == null) return;

      const focus     = hoveredIdRef.current;
      const isFocused = focus != null && (a.id === focus || b.id === focus);
      const dimmed    = focus != null && !isFocused;
      const wRatio    = link.weight / maxEdgeWeightRef.current;

      ctx.beginPath();
      ctx.moveTo(a.x, a.y);
      ctx.lineTo(b.x, b.y);
      ctx.strokeStyle = isFocused
        ? alpha(colorsRef.current.focus, 0.7)
        : dimmed
          ? alpha(colorsRef.current.edge, 0.05)
          : alpha(colorsRef.current.edge, 0.22);
      ctx.lineWidth   = 0.5 + wRatio * 1.6;
      ctx.lineCap     = 'round';
      ctx.stroke();
    },
    [],
  );

  const drawNode = useCallback(
    (node: PhysicsNode, ctx: CanvasRenderingContext2D, scale: number) => {
      if (node.x == null || node.y == null) return;
      const r           = node.radius;
      const focus       = hoveredIdRef.current;
      const isFocused   = node.id === focus;
      const focusNbrs   = focus ? neighboursRef.current.get(focus) : null;
      const highlighted = !focus || isFocused || (focusNbrs?.has(node.id) ?? false);
      const dimmed      = !!focus && !highlighted;
      const isPinned    = activeIdsRef.current.has(node.id);

      ctx.save();
      ctx.globalAlpha = dimmed ? 0.18 : 1;

      // Drop shadow + focus glow.
      if (isFocused) {
        ctx.shadowColor = alpha(colorsRef.current.focus, 0.2);
        ctx.shadowBlur  = 10;
      } else {
        ctx.shadowColor = colorsRef.current.shadow;
        ctx.shadowBlur  = 4;
        ctx.shadowOffsetY = 1.5;
      }

      // Main fill.
      ctx.beginPath();
      ctx.arc(node.x, node.y, r, 0, Math.PI * 2);
      ctx.fillStyle = colorsRef.current.fills[hashId(node.id) % colorsRef.current.fills.length];
      ctx.fill();
      ctx.globalAlpha = dimmed ? 0.18 : 1;
      ctx.shadowBlur  = 0;
      ctx.shadowOffsetY = 0;

      // Border.
      ctx.lineWidth   = isFocused ? 2.5 : isPinned ? 2 : 1.2;
      ctx.strokeStyle = isFocused || isPinned
        ? colorsRef.current.focus
        : alpha(colorsRef.current.edge, 0.15);
      ctx.stroke();

      // Pinned-selection dashed ring — coloured by importance so the rim
      // doubles as the importance channel for pinned concepts.
      if (isPinned) {
        ctx.beginPath();
        ctx.arc(node.x, node.y, r + 6, 0, Math.PI * 2);
        ctx.setLineDash([4, 3]);
        ctx.strokeStyle = importanceColorRef.current(node.avgImportance);
        ctx.lineWidth   = 2;
        ctx.globalAlpha = dimmed ? 0.18 : 0.85;
        ctx.stroke();
        ctx.setLineDash([]);
        ctx.globalAlpha = dimmed ? 0.18 : 1;
      }

      // Label — only when the node is large enough to read at the current zoom.
      const fontSize = Math.max(9, Math.min(13, r * 0.52));
      if (r * scale >= 9) {
        ctx.font         = `${isFocused || isPinned ? 700 : 500} ${fontSize}px system-ui, -apple-system, sans-serif`;
        ctx.textAlign    = 'center';
        ctx.textBaseline = 'middle';
        ctx.fillStyle    = colorsRef.current.text;

        const lines  = node.lines;
        const startY = node.y - ((lines.length - 1) / 2) * LABEL_LINE_H;
        for (let i = 0; i < lines.length; i++) {
          const y = startY + i * LABEL_LINE_H;
          ctx.fillText(lines[i], node.x, y);
        }
      }

      ctx.restore();
    },
    [],
  );

  // Force-graph pauses redraws once the simulation settles. Rebind the painter
  // to invalidate that cached frame on theme changes without reheating physics.
  useEffect(() => {
    graphRef.current?.nodeCanvasObject((node, ctx, scale) => drawNode(node, ctx, scale));
  }, [theme, drawNode]);

  const paintNodeHit = useCallback(
    (node: PhysicsNode, color: string, ctx: CanvasRenderingContext2D) => {
      if (node.x == null || node.y == null) return;
      ctx.beginPath();
      ctx.arc(node.x, node.y, node.radius + 2, 0, Math.PI * 2);
      ctx.fillStyle = color;
      ctx.fill();
    },
    [],
  );

  // ---- Zoom / view controls ----
  const zoomIn  = useCallback(() => {
    initialFitRef.current = false; settleFitRef.current = false;
    const g = graphRef.current;
    if (g) g.zoom(g.zoom() * ZOOM_STEP, 200);
  }, []);
  const zoomOut = useCallback(() => {
    initialFitRef.current = false; settleFitRef.current = false;
    const g = graphRef.current;
    if (g) g.zoom(g.zoom() / ZOOM_STEP, 200);
  }, []);
  const resetView = useCallback(() => {
    initialFitRef.current = false; settleFitRef.current = false;
    fitGraph(250);
  }, [fitGraph]);

  // ---- Fullscreen ----
  useEffect(() => {
    if (!isFullscreen) return;
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') setIsFullscreen(false); };
    window.addEventListener('keydown', onKey);
    const prev = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    return () => { window.removeEventListener('keydown', onKey); document.body.style.overflow = prev; };
  }, [isFullscreen]);

  // ---- Render ----
  // NOTE: the canvas container is ALWAYS rendered (even with no nodes) so the
  // one-shot init effect can attach to it. Early-returning a placeholder when
  // nodes are momentarily empty (e.g. while records load) left the container
  // unmounted, so the graph never initialised until it was remounted by a tab
  // switch. The empty state is now an overlay instead.
  const isEmpty = !nodes.length;
  const tipNode      = hoveredId ? physicsNodes.find((n) => n.id === hoveredId) : null;
  const tipNbrCount  = hoveredId ? neighbours.get(hoveredId)?.size ?? 0 : 0;

  return (
    <Box
      sx={{
        border: 0,
        borderRadius: isFullscreen ? 0 : 2,
        position: isFullscreen ? 'fixed' : 'relative',
        ...(isFullscreen
          ? { top: 0, left: 0, right: 0, bottom: 0, width: '100vw', height: '100vh', zIndex: 2000 }
          : { height }),
        overflow: 'hidden',
        bgcolor: 'background.paper',
      }}
    >
      <Box
        ref={containerRef}
        sx={{ width: '100%', height: '100%', cursor: 'grab' }}
      />

      {/* Empty-state overlay (container stays mounted underneath) */}
      {isEmpty && (
        <Box
          sx={{
            position: 'absolute',
            inset: 0,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            color: 'text.secondary',
            pointerEvents: 'none',
            px: 2,
            textAlign: 'center',
          }}
        >
          <Typography variant="body2">
            No concepts yet — run a crew with memory enabled.
          </Typography>
        </Box>
      )}

      {/* Hover tooltip */}
      {tipNode && (
        <Box
          sx={{
            position: 'absolute',
            top: 12,
            right: 12,
            px: 1.5,
            py: 1,
            minWidth: 160,
            maxWidth: 240,
            borderRadius: 1.5,
            bgcolor: alpha(theme.palette.background.paper, 0.96),
            color: 'text.primary',
            boxShadow: `0 4px 20px ${alpha(theme.palette.common.black, dark ? 0.22 : 0.08)}`,
            pointerEvents: 'none',
            backdropFilter: 'blur(6px)',
            border: 0,
          }}
        >
          <Typography variant="subtitle2" sx={{ fontWeight: 600, mb: 0.25, color: 'text.primary' }}>
            {tipNode.label}
          </Typography>
          <Typography variant="caption" sx={{ display: 'block', color: 'text.secondary' }}>
            {tipNode.count} {tipNode.count === 1 ? 'record' : 'records'}
            {' · '}importance {tipNode.avgImportance.toFixed(2)}
          </Typography>
          {tipNbrCount > 0 && (
            <Typography variant="caption" sx={{ display: 'block', color: 'text.secondary', mt: 0.25 }}>
              {tipNbrCount} connected concepts
            </Typography>
          )}
        </Box>
      )}

      {/* Legend */}
      <Box
        sx={{
          position: 'absolute',
          bottom: 8,
          left: 8,
          display: 'flex',
          gap: 1.25,
          alignItems: 'center',
          px: 1.25,
          py: 0.5,
          borderRadius: 1,
          bgcolor: alpha(theme.palette.background.paper, 0.92),
          fontSize: 11,
          color: 'text.secondary',
          backdropFilter: 'blur(3px)',
          border: 0,
          flexWrap: 'wrap', maxWidth: 'calc(100% - 16px)',
        }}
      >
        <span>size = frequency</span>
        <span style={{ opacity: 0.35 }}>·</span>
        <span>color = concept</span>
        <span style={{ opacity: 0.35 }}>·</span>
        <span>ring = importance</span>
        <span style={{ opacity: 0.35 }}>·</span>
        <span>click = filter</span>
      </Box>

      {/* Zoom / view controls */}
      <Box
        sx={{
          position: 'absolute',
          top: 12,
          left: 12,
          display: 'flex',
          flexDirection: 'column',
          gap: 0.5,
          p: 0.5,
          borderRadius: 1.5,
          bgcolor: alpha(theme.palette.background.paper, 0.95),
          boxShadow: '0 1px 6px rgba(0,0,0,0.07)',
          backdropFilter: 'blur(4px)',
          border: 0,
        }}
      >
        <Tooltip title="Zoom in" placement="right">
          <IconButton size="small" onClick={zoomIn}>
            <AddIcon fontSize="small" />
          </IconButton>
        </Tooltip>
        <Tooltip title="Zoom out" placement="right">
          <IconButton size="small" onClick={zoomOut}>
            <RemoveIcon fontSize="small" />
          </IconButton>
        </Tooltip>
        <Tooltip title="Fit to view" placement="right">
          <IconButton size="small" onClick={resetView}>
            <CenterFocusStrongIcon fontSize="small" />
          </IconButton>
        </Tooltip>
        <Tooltip title={isFullscreen ? 'Exit fullscreen (Esc)' : 'Fullscreen'} placement="right">
          <IconButton size="small" onClick={() => setIsFullscreen((v) => !v)}>
            {isFullscreen ? <FullscreenExitIcon fontSize="small" /> : <FullscreenIcon fontSize="small" />}
          </IconButton>
        </Tooltip>
      </Box>
    </Box>
  );
};

export default ConceptForceGraph;
