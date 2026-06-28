import React from 'react';

// Lightweight, dependency-free SVG chart utility (bar / line / pie). Extracted
// from the retired legacy A2UI renderer so the Genie result renderer can keep its
// data visuals — this is a pure chart helper, NOT A2UI surface rendering (which
// now goes exclusively through the shared A2UIRenderer / A2uiSurface).

const ACCENT = 'var(--ui-accent, #5aa2ff)';
const TEXT = 'var(--ui-text, #eaf0ff)';
const MUTED = 'var(--ui-muted, #aab3d4)';
const CHART_PALETTE = ['#5aa2ff', '#34d6b6', '#fbbf24', '#fb7185', '#a78bfa', '#38bdf8', '#f472b6'];

export interface ChartPoint {
  label: string;
  value: number;
}

/** Truncate long axis labels so they don't collide. */
function shortLabel(s: string, max = 12): string {
  return s.length > max ? `${s.slice(0, max - 1)}…` : s;
}

/** Lightweight SVG chart (bar / line / pie) — no external chart dependency.
 *  `colors` overrides the (dark-stage) defaults so the same chart stays readable
 *  on a LIGHT background (e.g. inline in the chat) — pass theme tokens. */
export const ChartView: React.FC<{
  chartType: string;
  data: ChartPoint[];
  title?: string;
  colors?: { text?: string; muted?: string };
}> = ({ chartType, data, title, colors }) => {
  const text = colors?.text ?? TEXT;
  const muted = colors?.muted ?? MUTED;
  const points = data.filter((d) => d && typeof d.value === 'number' && isFinite(d.value));
  if (points.length === 0) return null;
  const max = Math.max(1, ...points.map((p) => Math.abs(p.value)));
  const W = 520;
  const H = 260;

  let chart: React.ReactNode = null;
  if (chartType === 'pie') {
    const total = points.reduce((s, p) => s + Math.abs(p.value), 0) || 1;
    let angle = -Math.PI / 2;
    const cx = 82;
    const cy = H / 2;
    const r = 62;
    chart = (
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" height={H}>
        {points.map((p, i) => {
          const slice = (Math.abs(p.value) / total) * Math.PI * 2;
          const x1 = cx + r * Math.cos(angle);
          const y1 = cy + r * Math.sin(angle);
          angle += slice;
          const x2 = cx + r * Math.cos(angle);
          const y2 = cy + r * Math.sin(angle);
          const large = slice > Math.PI ? 1 : 0;
          const d = `M ${cx} ${cy} L ${x1} ${y1} A ${r} ${r} 0 ${large} 1 ${x2} ${y2} Z`;
          return <path key={i} d={d} fill={CHART_PALETTE[i % CHART_PALETTE.length]} opacity={0.9} />;
        })}
        {points.map((p, i) => (
          <g key={`l${i}`} transform={`translate(180, ${24 + i * 20})`}>
            <rect width="11" height="11" rx="2" fill={CHART_PALETTE[i % CHART_PALETTE.length]} />
            <text x="17" y="10" fontSize="11.5" fill={muted}>{`${shortLabel(p.label, 22)} (${p.value})`}</text>
          </g>
        ))}
      </svg>
    );
  } else if (chartType === 'line') {
    const stepX = (W - 40) / Math.max(1, points.length - 1);
    const coords = points.map((p, i) => [20 + i * stepX, H - 34 - (Math.abs(p.value) / max) * (H - 64)]);
    const poly = coords.map((c) => c.join(',')).join(' ');
    const area = `20,${H - 34} ${poly} ${20 + (points.length - 1) * stepX},${H - 34}`;
    chart = (
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" height={H}>
        <polygon points={area} fill="rgba(90,162,255,0.15)" />
        <polyline points={poly} fill="none" stroke={ACCENT} strokeWidth={2.5} />
        {coords.map((c, i) => <circle key={i} cx={c[0]} cy={c[1]} r={3.5} fill={ACCENT} />)}
        {points.map((p, i) => (
          <text
            key={`t${i}`}
            x={coords[i][0]}
            y={H - 12}
            fontSize="9.5"
            textAnchor="end"
            fill={muted}
            transform={`rotate(-35 ${coords[i][0]} ${H - 12})`}
          >
            {shortLabel(p.label)}
          </text>
        ))}
      </svg>
    );
  } else {
    const bw = (W - 32) / points.length;
    // Many bars → rotate the labels so long names (e.g. "Washington") don't overlap.
    const rotate = points.length > 6;
    chart = (
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" height={H}>
        {points.map((p, i) => {
          const h = (Math.abs(p.value) / max) * (H - 64);
          const cx = 16 + i * bw + bw / 2;
          const barY = H - 38 - h;
          return (
            <g key={i}>
              <rect x={16 + i * bw + bw * 0.15} y={barY} width={bw * 0.7} height={h} rx={4} fill={CHART_PALETTE[i % CHART_PALETTE.length]} />
              {/* value on top of the bar */}
              <text x={cx} y={barY - 4} fontSize="9.5" textAnchor="middle" fill={muted}>{p.value}</text>
              {rotate ? (
                <text x={cx} y={H - 20} fontSize="9.5" textAnchor="end" fill={muted} transform={`rotate(-35 ${cx} ${H - 20})`}>
                  {shortLabel(p.label)}
                </text>
              ) : (
                <text x={cx} y={H - 12} fontSize="10" textAnchor="middle" fill={muted}>{shortLabel(p.label, 16)}</text>
              )}
            </g>
          );
        })}
      </svg>
    );
  }

  return (
    <div>
      {title && <div style={{ color: text, fontWeight: 600, marginBottom: 6, fontSize: '0.95rem' }}>{title}</div>}
      {chart}
    </div>
  );
};

export default ChartView;
