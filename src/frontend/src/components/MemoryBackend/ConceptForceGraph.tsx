/**
 * Force-directed concept graph for cognitive memory categories.
 *
 * Built on `force-graph` (vasturiano) — canvas rendering, d3-force physics,
 * native pan/zoom/drag. We keep MUI overlays for the legend, zoom controls,
 * fullscreen toggle and the hover tooltip so the component blends with the
 * rest of the Cognitive Memory Browser.
 *
 * Visual identity preserved from the previous SVG version:
 *  - Flat-fill nodes coloured by importance
 *  - White-outlined labels readable on any fill
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
  fill: string;
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

// Curated 16-colour palette (Tailwind 500 shades — perceptually balanced).
// Each concept hashes to one of these so every node looks distinct, the way
// graph UIs like Obsidian / Logseq / Gephi present concept maps.
const CONCEPT_PALETTE = [
  '#6366f1', '#06b6d4', '#10b981', '#f59e0b',
  '#ef4444', '#ec4899', '#8b5cf6', '#3b82f6',
  '#14b8a6', '#84cc16', '#f97316', '#a855f7',
  '#0ea5e9', '#22c55e', '#eab308', '#f43f5e',
];

// djb2 — fast, decent distribution, deterministic per id.
function hashId(id: string): number {
  let h = 5381;
  for (let i = 0; i < id.length; i++) h = ((h << 5) + h + id.charCodeAt(i)) | 0;
  return Math.abs(h);
}

function paletteColor(id: string): string {
  return CONCEPT_PALETTE[hashId(id) % CONCEPT_PALETTE.length];
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
  const containerRef = useRef<HTMLDivElement | null>(null);
  const graphRef     = useRef<ForceGraphInstance | null>(null);

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

  // Build the physics node payload. We re-derive radius / colour here so
  // canvas callbacks can read them straight off the node object.
  const physicsNodes = useMemo<PhysicsNode[]>(() => {
    const maxCount = nodes.reduce((m, n) => Math.max(m, n.count), 1);
    return nodes.map((n) => {
      const lines  = splitLabel(n.label);
      const baseR  = 14 + Math.sqrt(n.count / maxCount) * 22;
      const radius = Math.max(baseR, requiredRadiusForLabel(lines));
      return { ...n, lines, radius, fill: paletteColor(n.id) };
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
      .minZoom(0.2)
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
    const sync = () => {
      const r = container.getBoundingClientRect();
      g.width(r.width).height(r.height);
    };
    sync();
    const ro = new ResizeObserver(sync);
    ro.observe(container);

    return () => {
      ro.disconnect();
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
    const prev    = (g as unknown as { graphData(): { nodes: PhysicsNode[] } }).graphData();
    const prevMap = new Map(prev.nodes.map((n) => [n.id, n]));
    for (const n of physicsNodes) {
      const p = prevMap.get(n.id);
      if (p) { n.x = p.x; n.y = p.y; n.vx = p.vx; n.vy = p.vy; }
    }
    g.graphData({ nodes: physicsNodes, links: displayEdges as PhysicsLink[] });
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
        ? `rgba(99,102,241,0.70)`
        : dimmed
          ? `rgba(148,163,184,0.05)`
          : `rgba(148,163,184,0.22)`;
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
        ctx.shadowColor = 'rgba(99,102,241,0.55)';
        ctx.shadowBlur  = 14;
      } else {
        ctx.shadowColor = 'rgba(15,23,42,0.18)';
        ctx.shadowBlur  = 4;
        ctx.shadowOffsetY = 1.5;
      }

      // Main fill.
      ctx.beginPath();
      ctx.arc(node.x, node.y, r, 0, Math.PI * 2);
      ctx.fillStyle = node.fill;
      ctx.globalAlpha *= 0.92;
      ctx.fill();
      ctx.globalAlpha = dimmed ? 0.18 : 1;
      ctx.shadowBlur  = 0;
      ctx.shadowOffsetY = 0;

      // Border.
      ctx.lineWidth   = isFocused ? 2.5 : isPinned ? 2 : 1.2;
      ctx.strokeStyle = isFocused || isPinned
        ? '#ffffff'
        : 'rgba(255,255,255,0.65)';
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
        ctx.lineWidth    = 2.8;
        ctx.strokeStyle  = 'rgba(0,0,0,0.32)';
        ctx.fillStyle    = '#ffffff';

        const lines  = node.lines;
        const startY = node.y - ((lines.length - 1) / 2) * LABEL_LINE_H;
        for (let i = 0; i < lines.length; i++) {
          const y = startY + i * LABEL_LINE_H;
          ctx.strokeText(lines[i], node.x, y);
          ctx.fillText(lines[i], node.x, y);
        }
      }

      ctx.restore();
    },
    [],
  );

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
    const g = graphRef.current;
    if (g) g.zoom(g.zoom() * ZOOM_STEP, 200);
  }, []);
  const zoomOut = useCallback(() => {
    const g = graphRef.current;
    if (g) g.zoom(g.zoom() / ZOOM_STEP, 200);
  }, []);
  const resetView = useCallback(() => {
    graphRef.current?.zoomToFit(400, 60);
  }, []);

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
        border: isFullscreen ? 'none' : '1px solid',
        borderColor: 'divider',
        borderRadius: isFullscreen ? 0 : 2,
        position: isFullscreen ? 'fixed' : 'relative',
        ...(isFullscreen
          ? { top: 0, left: 0, right: 0, bottom: 0, width: '100vw', height: '100vh', zIndex: 2000 }
          : { height }),
        overflow: 'hidden',
        bgcolor: 'background.paper',
        backgroundImage: 'radial-gradient(ellipse 70% 55% at 50% 10%, rgba(99,102,241,0.05) 0%, transparent 100%)',
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
            No concepts yet — run a crew with cognitive memory enabled.
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
            bgcolor: 'rgba(15,23,42,0.90)',
            color: '#f8fafc',
            boxShadow: '0 4px 20px rgba(0,0,0,0.22)',
            pointerEvents: 'none',
            backdropFilter: 'blur(6px)',
            border: '1px solid rgba(255,255,255,0.10)',
          }}
        >
          <Typography variant="subtitle2" sx={{ fontWeight: 700, mb: 0.25, color: '#f1f5f9' }}>
            {tipNode.label}
          </Typography>
          <Typography variant="caption" sx={{ display: 'block', color: '#94a3b8' }}>
            {tipNode.count} {tipNode.count === 1 ? 'record' : 'records'}
            {' · '}importance {tipNode.avgImportance.toFixed(2)}
          </Typography>
          {tipNbrCount > 0 && (
            <Typography variant="caption" sx={{ display: 'block', color: '#64748b', mt: 0.25 }}>
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
          bgcolor: 'rgba(248,250,252,0.92)',
          fontSize: 11,
          color: 'text.disabled',
          backdropFilter: 'blur(3px)',
          border: '1px solid',
          borderColor: 'divider',
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
          bgcolor: 'rgba(248,250,252,0.95)',
          boxShadow: '0 1px 6px rgba(0,0,0,0.07)',
          backdropFilter: 'blur(4px)',
          border: '1px solid',
          borderColor: 'divider',
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
