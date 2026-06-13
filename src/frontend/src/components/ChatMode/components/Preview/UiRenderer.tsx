import React, { useState, useEffect, useMemo, useRef, useCallback } from 'react';
import { UiComponent, UiSurface, UiTheme, resolveValue } from '../../utils/uiDocument';
import { sanitizeUrl } from '../../../Chat/components/MessageRenderer';

// SECURITY: A2UI documents are LLM/crew output (untrusted). Links must be run
// through sanitizeUrl() (blocks javascript:/data:/vbscript:) before binding to
// an anchor href, or a `javascript:` URL would execute in the Kasal origin on
// click. Image `src` may legitimately be a data:/blob: URI, so it uses a
// lighter check that only strips script schemes.
function sanitizeImageSrc(uri: string | undefined | null): string {
  if (!uri) return '';
  const u = uri.trim().toLowerCase();
  if (u.startsWith('javascript:') || u.startsWith('vbscript:')) return '';
  return uri;
}

/**
 * Kasal React renderer for a structured UI document (A2UI-conformant — see
 * THIRD_PARTY_NOTICES). It renders on a self-contained, premium "presentation"
 * theme (dark gradient stage + glassmorphism + accent) so agent-produced UIs
 * look like a polished keynote rather than ad-hoc markup, independent of the
 * app's light/dark theme.
 *
 * The component list is flat with id references, so we render recursively from
 * the root, resolving children by id and guarding against missing/cyclic refs.
 */

interface UiRendererProps {
  surface: UiSurface;
}

// --- Presentation theme (self-contained; not the app tokens) ---------------
// Tokens resolve through CSS custom properties so a surface theme (set from the
// UI-Configurator palette) can override them once at the root. The second arg of
// each var() is the built-in premium-dark fallback used when a doc is un-themed,
// so an un-themed document renders exactly as it always did.
const ACCENT = 'var(--ui-accent, #5aa2ff)';
// Readable text/icon color drawn ON TOP of an accent fill (root mindmap node,
// primary buttons). A token (not a literal) so a theme can override it.
const ON_ACCENT = 'var(--ui-on-accent, #06122b)';
const TEXT = 'var(--ui-text, #eaf0ff)';
const MUTED = 'var(--ui-muted, #aab3d4)';
const GLASS = 'var(--ui-surface, rgba(255,255,255,0.06))';
const GLASS_STRONG = 'var(--ui-surface-strong, rgba(255,255,255,0.10))';
const GLASS_BORDER = 'var(--ui-border, rgba(255,255,255,0.16))';
// OPAQUE node fill for the mindmap: the connector curves run into each node's
// center, and a translucent glass fill lets those lines show THROUGH the node
// (looking like the lines are drawn on top). Compositing the glass tint over an
// opaque base occludes the line so it reads as being BEHIND the node, while
// keeping the glass hue. Themes can override the solid base via --ui-surface-solid.
const NODE_FILL = `linear-gradient(${GLASS}, ${GLASS}), var(--ui-surface-solid, #141b35)`;
const STAGE_BG =
  'var(--ui-stage,' +
  'radial-gradient(1100px 560px at 12% -10%, rgba(90,162,255,0.20), transparent 60%),' +
  'radial-gradient(900px 520px at 92% 8%, rgba(167,139,250,0.18), transparent 55%),' +
  'linear-gradient(135deg, #0b1020, #131a33 45%, #1b2347))';
// Presentation decks default to a Databricks-grade identity (deep teal radial
// stage, brand-orange accent, hairline alpha borders) so a generated deck has
// the caliber of a hand-built one out of the box. These are pure DEFAULTS:
// they sit UNDER the UI-Configurator palette (themeVars) and any root-style
// refine, both of which still win.
const DECK_THEME_VARS = {
  '--ui-accent': '#FF3621',
  '--ui-on-accent': '#FFFFFF',
  '--ui-stage': 'radial-gradient(ellipse at 50% 38%, #162A34 0%, #080F14 72%)',
  '--ui-surface': 'rgba(255,255,255,0.04)',
  '--ui-surface-strong': 'rgba(255,255,255,0.08)',
  '--ui-border': 'rgba(255,255,255,0.12)',
  '--ui-muted': 'rgba(255,255,255,0.55)',
} as React.CSSProperties;

const TONE: Record<string, string> = {
  good: '#34d6b6',
  warn: '#fbbf24',
  bad: '#fb7185',
  neutral: MUTED,
};

const CARD_STYLE: React.CSSProperties = {
  background: GLASS,
  border: `1px solid ${GLASS_BORDER}`,
  backdropFilter: 'blur(12px)',
  WebkitBackdropFilter: 'blur(12px)',
  borderRadius: 16,
  boxShadow: '0 10px 30px rgba(0,0,0,0.28)',
};

const TEXT_STYLES: Record<string, React.CSSProperties> = {
  h1: { fontSize: '2.8rem', fontWeight: 800, lineHeight: 1.1, letterSpacing: '-0.02em' },
  h2: { fontSize: '2.1rem', fontWeight: 700, lineHeight: 1.15 },
  h3: { fontSize: '1.55rem', fontWeight: 700 },
  h4: { fontSize: '1.3rem', fontWeight: 500, color: MUTED, lineHeight: 1.4 },
  h5: { fontSize: '1.1rem', fontWeight: 600, color: MUTED },
  caption: { fontSize: '0.95rem', color: MUTED },
  // No hardcoded color: body inherits the cascaded/themed text color (var(--ui-text)),
  // so it stays light on the built-in dark stage AND dark on a light theme. A literal
  // here (was #dbe3ff) overrode the theme and rendered light-on-light = unreadable.
  body: { fontSize: '1.3rem', lineHeight: 1.55 },
};

const JUSTIFY: Record<string, string> = {
  start: 'flex-start', center: 'center', end: 'flex-end', stretch: 'stretch',
  spaceBetween: 'space-between', spaceAround: 'space-around', spaceEvenly: 'space-evenly',
};
const ALIGN: Record<string, string> = {
  start: 'flex-start', center: 'center', end: 'flex-end', stretch: 'stretch',
};

const CHART_PALETTE = ['#5aa2ff', '#34d6b6', '#fbbf24', '#fb7185', '#a78bfa', '#38bdf8', '#f472b6'];

// Font stacks the theme's `font` token maps to (applied at the stage root).
const FONT_STACK: Record<string, string> = {
  sans: 'Inter, system-ui, -apple-system, sans-serif',
  serif: 'Georgia, "Times New Roman", Cambria, serif',
  rounded: '"Nunito", "Quicksand", "Segoe UI", system-ui, sans-serif',
  mono: '"JetBrains Mono", "SF Mono", Menlo, Consolas, monospace',
};

/**
 * Map a surface theme → CSS custom properties consumed by the token constants
 * above. Only tokens the palette actually defines are emitted, so anything it
 * omits keeps the premium-dark fallback baked into each var(). Returned as a
 * style object spread onto the stage root.
 */
function themeVars(theme?: UiTheme): React.CSSProperties {
  if (!theme) return {};
  const v: Record<string, string> = {};
  if (theme.accent) v['--ui-accent'] = theme.accent;
  if (theme.text) v['--ui-text'] = theme.text;
  if (theme.muted) v['--ui-muted'] = theme.muted;
  if (theme.heading) v['--ui-heading'] = theme.heading;
  if (theme.surface) {
    v['--ui-surface'] = theme.surface;
    v['--ui-surface-strong'] = theme.surface;
  }
  if (theme.background) v['--ui-stage'] = theme.background;
  // The default border is translucent white (for the dark stage); on a custom
  // light OR dark surface a neutral grey reads correctly on both.
  if (theme.surface || theme.background) v['--ui-border'] = 'rgba(128,128,128,0.30)';
  return v as React.CSSProperties;
}

// Professional inline-SVG icons (Heroicons-style stroke paths), keyed by common
// names + synonyms agents tend to use. Falls back to a neutral dot.
const ICON_PATHS: Record<string, string> = {
  chart: 'M3 3v18h18 M7 14l3-3 3 3 5-6',
  trending: 'M3 17l6-6 4 4 8-8 M17 7h4v4',
  growth: 'M3 17l6-6 4 4 8-8 M17 7h4v4',
  decline: 'M3 7l6 6 4-4 8 8 M17 17h4v-4',
  shield: 'M12 3l8 3v6c0 5-3.5 8-8 9-4.5-1-8-4-8-9V6l8-3z',
  globe: 'M12 3a9 9 0 100 18 9 9 0 000-18z M3 12h18 M12 3c3 3 3 15 0 18 M12 3c-3 3-3 15 0 18',
  alert: 'M12 9v4 M12 17h.01 M10.3 4l-8 14a2 2 0 001.7 3h16a2 2 0 001.7-3l-8-14a2 2 0 00-3.4 0z',
  warning: 'M12 9v4 M12 17h.01 M10.3 4l-8 14a2 2 0 001.7 3h16a2 2 0 001.7-3l-8-14a2 2 0 00-3.4 0z',
  check: 'M20 6L9 17l-5-5',
  info: 'M12 3a9 9 0 100 18 9 9 0 000-18z M12 8h.01 M11 12h1v4h1',
  money: 'M12 3a9 9 0 100 18 9 9 0 000-18z M9 9a3 3 0 016 0c0 2-3 2-3 3 M12 16h.01',
  bank: 'M3 21h18 M5 21V9l7-5 7 5v12 M9 21v-6h6v6',
  building: 'M3 21h18 M5 21V9l7-5 7 5v12 M9 21v-6h6v6',
  energy: 'M13 2L4 14h7l-1 8 9-12h-7l1-8z',
  users: 'M16 21v-2a4 4 0 00-8 0v2 M12 11a4 4 0 100-8 4 4 0 000 8',
  flag: 'M4 21V4 M4 4h13l-2 4 2 4H4',
  calendar: 'M4 6h16v15H4z M4 10h16 M8 3v4 M16 3v4',
  star: 'M12 3l2.9 6 6.6.9-4.8 4.6 1.2 6.5L12 18.8 6.1 21l1.2-6.5L2.5 9.9 9.1 9z',
};
const ICON_ALIASES: Record<string, string> = {
  'trending-up': 'trending', 'trending-down': 'decline', 'arrow-up': 'growth', 'arrow-down': 'decline',
  security: 'shield', risk: 'alert', success: 'check', finance: 'money', currency: 'money',
  economy: 'money', bolt: 'energy', power: 'energy', people: 'users', team: 'users', date: 'calendar',
};
function iconPath(name: string): string | null {
  const key = name.toLowerCase().trim();
  return ICON_PATHS[key] || ICON_PATHS[ICON_ALIASES[key]] || null;
}

export interface ChartPoint {
  label: string;
  value: number;
}

/** Truncate long axis labels so they don't collide. */
function shortLabel(s: string, max = 12): string {
  return s.length > max ? `${s.slice(0, max - 1)}…` : s;
}

/** Lightweight SVG chart (bar / line / pie) — no external chart dependency.
 *  Exported so the Genie result renderer reuses the same advanced visuals.
 *  `colors` overrides the (dark-stage) defaults so the same chart stays
 *  readable on a LIGHT background (e.g. inline in the chat) — pass theme tokens. */
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

/**
 * A navigable slide deck: one slide fills the stage (vertically centered, large
 * text), navigated by clickable dots OR the ← / → keyboard keys. No arrows.
 */
const SlidesNode: React.FC<{ childIds: string[]; renderChild: (id: string) => React.ReactNode }> = ({ childIds, renderChild }) => {
  const [idx, setIdx] = useState(0);
  const count = childIds.length;

  useEffect(() => {
    if (count <= 1) return undefined;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'ArrowRight') setIdx((i) => Math.min(count - 1, i + 1));
      else if (e.key === 'ArrowLeft') setIdx((i) => Math.max(0, i - 1));
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [count]);

  if (count === 0) return null;
  const current = Math.min(idx, count - 1);
  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '84vh' }}>
      {/* One slide = one screen. It centers when it fits; if a slide is overly
          dense it scrolls WITHIN this area so the dots below stay visible. */}
      <div
        key={childIds[current]}
        className="ui-slide-enter"
        style={{ flex: 1, minHeight: 0, overflowY: 'auto', display: 'flex', flexDirection: 'column', justifyContent: 'center' }}
      >
        {renderChild(childIds[current])}
      </div>
      {/* Clickable dots (keyboard ← / → also navigate) + deck-style counter */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 12, paddingTop: 20, position: 'relative' }}>
        <span
          aria-hidden
          style={{
            position: 'absolute', left: 4, color: MUTED, fontSize: '0.78rem',
            fontVariantNumeric: 'tabular-nums', letterSpacing: '0.08em',
          }}
        >
          {String(current + 1).padStart(2, '0')} / {String(count).padStart(2, '0')}
        </span>
        {childIds.map((cid, i) => (
          <button
            key={cid}
            type="button"
            onClick={() => setIdx(i)}
            aria-label={`Go to slide ${i + 1}`}
            style={{
              width: i === current ? 28 : 10, height: 10, borderRadius: 999,
              background: i === current ? ACCENT : 'rgba(255,255,255,0.28)',
              border: 'none', padding: 0, cursor: 'pointer', transition: 'all 0.2s',
            }}
          />
        ))}
      </div>
    </div>
  );
};

interface QuizQuestion {
  question: string;
  options: string[];
  answer?: number | string;
  explanation?: string;
}

/**
 * A fully interactive quiz: one question at a time, single-choice selection,
 * live score, prev/next, submit, and a final breakdown. The agent only supplies
 * the question data; all interactivity lives here so it actually works.
 */
const QuizNode: React.FC<{ title?: string; questions: QuizQuestion[] }> = ({ title, questions }) => {
  const [idx, setIdx] = useState(0);
  const [answers, setAnswers] = useState<Record<number, number>>({});
  const [submitted, setSubmitted] = useState(false);

  const count = questions.length;
  if (count === 0) return null;

  // Resolve the correct option index for a question (accepts index or text).
  const correctIdx = (q: QuizQuestion): number => {
    if (typeof q.answer === 'number') return q.answer;
    if (typeof q.answer === 'string') {
      const byText = q.options.findIndex((o) => o === q.answer);
      if (byText >= 0) return byText;
      const n = Number(q.answer);
      return Number.isInteger(n) ? n : -1;
    }
    return -1;
  };
  const answeredCount = Object.keys(answers).length;
  const score = questions.reduce((s, q, i) => s + (answers[i] === correctIdx(q) ? 1 : 0), 0);
  const current = Math.min(idx, count - 1);

  const statTile = (label: string, value: string) => (
    <div style={{ ...CARD_STYLE, background: GLASS_STRONG, padding: '12px 16px', flex: 1 }}>
      <div style={{ fontSize: 10, fontWeight: 700, letterSpacing: '0.07em', textTransform: 'uppercase', color: MUTED }}>{label}</div>
      <div style={{ fontSize: '1.4rem', fontWeight: 800, color: TEXT }}>{value}</div>
    </div>
  );

  if (submitted) {
    const pct = Math.round((score / count) * 100);
    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: 18 }}>
        {title && <div style={{ fontSize: '2rem', fontWeight: 800, color: TEXT }}>{title}</div>}
        <div style={{ ...CARD_STYLE, padding: 24, textAlign: 'center' }}>
          <div style={{ fontSize: 12, fontWeight: 700, letterSpacing: '0.08em', textTransform: 'uppercase', color: MUTED }}>Final Score</div>
          <div style={{ fontSize: '3rem', fontWeight: 800, color: pct >= 70 ? TONE.good : pct >= 40 ? TONE.warn : TONE.bad }}>{score} / {count}</div>
          <div style={{ color: MUTED }}>{pct}%</div>
        </div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
          {questions.map((q, i) => {
            const ok = answers[i] === correctIdx(q);
            return (
              <div key={i} style={{ ...CARD_STYLE, padding: 14 }}>
                <div style={{ color: TEXT, fontWeight: 600, marginBottom: 6 }}>{i + 1}. {q.question}</div>
                <div style={{ color: ok ? TONE.good : TONE.bad, fontSize: '0.9rem' }}>
                  Your answer: {answers[i] != null ? q.options[answers[i]] : '—'} {ok ? '✓' : `✗ (correct: ${q.options[correctIdx(q)] ?? '—'})`}
                </div>
              </div>
            );
          })}
        </div>
        <button type="button" onClick={() => { setSubmitted(false); setAnswers({}); setIdx(0); }}
          style={{ background: ACCENT, color: ON_ACCENT, fontWeight: 700, border: 'none', borderRadius: 10, padding: '10px 18px', alignSelf: 'center', cursor: 'pointer' }}>
          Retake quiz
        </button>
      </div>
    );
  }

  const q = questions[current];
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 18 }}>
      {title && <div style={{ fontSize: '2rem', fontWeight: 800, color: TEXT }}>{title}</div>}
      <div style={{ display: 'flex', gap: 12 }}>
        {statTile('Score', String(score))}
        {statTile('Answered', `${answeredCount} / ${count}`)}
        {statTile('Question', `${current + 1} / ${count}`)}
      </div>
      <div style={{ height: 6, borderRadius: 999, background: 'rgba(255,255,255,0.12)', overflow: 'hidden' }}>
        <div style={{ width: `${(answeredCount / count) * 100}%`, height: '100%', background: ACCENT, transition: 'width 0.2s' }} />
      </div>
      <div style={{ ...CARD_STYLE, padding: 22, display: 'flex', flexDirection: 'column', gap: 14 }}>
        <div style={{ fontSize: '1.3rem', fontWeight: 700, color: TEXT }}>{q.question}</div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
          {q.options.map((opt, oi) => {
            const chosen = answers[current] === oi;
            return (
              <button key={oi} type="button" onClick={() => setAnswers((a) => ({ ...a, [current]: oi }))}
                style={{
                  textAlign: 'left', padding: '12px 16px', borderRadius: 12, cursor: 'pointer',
                  background: chosen ? `${ACCENT}22` : GLASS,
                  border: `1px solid ${chosen ? ACCENT : GLASS_BORDER}`,
                  color: TEXT, fontSize: '1.02rem', transition: 'all 0.15s',
                }}>
                {String.fromCharCode(65 + oi)}. {opt}
              </button>
            );
          })}
        </div>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginTop: 4 }}>
          <button type="button" onClick={() => setIdx((i) => Math.max(0, i - 1))} disabled={current === 0}
            style={{ background: GLASS, color: TEXT, border: `1px solid ${GLASS_BORDER}`, borderRadius: 10, padding: '9px 16px', cursor: 'pointer', opacity: current === 0 ? 0.4 : 1 }}>
            Previous
          </button>
          {current < count - 1 ? (
            <button type="button" onClick={() => setIdx((i) => Math.min(count - 1, i + 1))}
              style={{ background: ACCENT, color: ON_ACCENT, fontWeight: 700, border: 'none', borderRadius: 10, padding: '9px 18px', cursor: 'pointer' }}>
              Next
            </button>
          ) : (
            <button type="button" onClick={() => setSubmitted(true)} disabled={answeredCount === 0}
              style={{ background: TONE.good, color: '#06251c', fontWeight: 700, border: 'none', borderRadius: 10, padding: '9px 18px', cursor: 'pointer', opacity: answeredCount === 0 ? 0.5 : 1 }}>
              Submit Quiz
            </button>
          )}
        </div>
      </div>
    </div>
  );
};

/** A node in a Mindmap tree: a short label, optional longer detail/description
 *  (surfaced in the hover tooltip), and optional nested children. */
interface MindmapData {
  label?: unknown;
  text?: unknown;
  /** Longer explanatory content for the node, shown in the hover tooltip. The
   *  agent may name it any of these; the first present wins. */
  description?: unknown;
  detail?: unknown;
  note?: unknown;
  children?: MindmapData[];
}

/** A node's children as a clean array (tolerant of missing / non-array). */
function mindmapChildren(node: MindmapData): MindmapData[] {
  return Array.isArray(node.children)
    ? node.children.filter((c): c is MindmapData => Boolean(c) && typeof c === 'object')
    : [];
}

// Auto-layout spacing: horizontal distance per depth level, vertical per leaf row.
// MM_COL must exceed the widest node (root = MM_NODE_W + 20) so a node never
// reaches into the next depth column; MM_ROW exceeds a (two-line) node's height
// for the same reason vertically.
// Nodes render at this FIXED width (root a touch wider) so a multi-word label
// wraps by WORD onto up to two lines at a stable, readable size — instead of the
// flex box collapsing to a sliver and breaking the text mid-word.
const MM_NODE_W = 220;
const MM_COL = 260;
const MM_ROW = 76;
const MM_MIN_ZOOM = 0.3;
const MM_MAX_ZOOM = 2.5;

interface MMNode {
  id: string;
  label: string;
  /** Longer content shown in the hover tooltip; '' when the node has none. */
  detail: string;
  depth: number;
  parentId: string | null;
  childIds: string[];
  /** Branch color (root passes ACCENT; each top-level branch + its subtree share one). */
  color: string;
}

type XY = { x: number; y: number };

/** Flatten the tree into addressable nodes (id = path like "r.0.1"), assigning
 *  each top-level branch its own palette color, inherited by its descendants. */
function buildMindmap(root: MindmapData): { nodes: Record<string, MMNode>; rootId: string } {
  const nodes: Record<string, MMNode> = {};
  const walk = (node: MindmapData, id: string, depth: number, parentId: string | null, color: string) => {
    const kids = mindmapChildren(node);
    const childIds = kids.map((_, i) => `${id}.${i}`);
    const label = String(node.label ?? node.text ?? '');
    // Detail = an explicit description/detail/note, else a `text` that's distinct
    // from the label (when the agent puts the short name in `label` and the long
    // form in `text`). The tooltip shows this; '' means "nothing extra to show".
    const explicit = node.description ?? node.detail ?? node.note;
    const textVal = node.text != null ? String(node.text) : '';
    const detail = explicit != null ? String(explicit) : (textVal && textVal !== label ? textVal : '');
    nodes[id] = { id, label, detail, depth, parentId, childIds, color };
    kids.forEach((k, i) => {
      const childColor = depth === 0 ? CHART_PALETTE[i % CHART_PALETTE.length] : color;
      walk(k, childIds[i], depth + 1, id, childColor);
    });
  };
  walk(root, 'r', 0, null, ACCENT);
  return { nodes, rootId: 'r' };
}

/** Leaf rows a subtree occupies (a childless node is one row). */
function leafCount(nodes: Record<string, MMNode>, id: string): number {
  const n = nodes[id];
  return n.childIds.length === 0 ? 1 : n.childIds.reduce((s, c) => s + leafCount(nodes, c), 0);
}

/** Tidy BILATERAL tree layout → a center point per node: the root sits in the
 *  middle and its top-level branches split left/right (balanced by leaf rows),
 *  so the map grows symmetrically around the center instead of marching off to
 *  the right. Computed once for the full tree; positions are then
 *  user-editable (drag) and stable across collapse. */
function layoutMindmap(nodes: Record<string, MMNode>, rootId: string): Record<string, XY> {
  const pos: Record<string, XY> = {};
  const root = nodes[rootId];

  // Greedy balance: each top-level branch goes to the lighter side (ties →
  // right), so both sides end up with a similar number of leaf rows.
  const right: string[] = [];
  const left: string[] = [];
  let rightLeaves = 0;
  let leftLeaves = 0;
  for (const branchId of root.childIds) {
    const leaves = leafCount(nodes, branchId);
    if (leftLeaves < rightLeaves) {
      left.push(branchId);
      leftLeaves += leaves;
    } else {
      right.push(branchId);
      rightLeaves += leaves;
    }
  }

  // Lay out one side: x grows away from the root with the side's sign.
  const placeSide = (branchIds: string[], sign: 1 | -1) => {
    let nextLeaf = 0;
    const place = (id: string): number => {
      const node = nodes[id];
      const x = sign * node.depth * MM_COL;
      let y: number;
      if (node.childIds.length === 0) {
        y = nextLeaf * MM_ROW;
        nextLeaf += 1;
      } else {
        const ys = node.childIds.map(place);
        y = (ys[0] + ys[ys.length - 1]) / 2; // center the parent on its children
      }
      pos[id] = { x, y };
      return y;
    };
    branchIds.forEach(place);
  };
  placeSide(right, 1);
  placeSide(left, -1);

  // Center both sides (and the root) on the same vertical midline.
  const rightHeight = Math.max(0, rightLeaves - 1) * MM_ROW;
  const leftHeight = Math.max(0, leftLeaves - 1) * MM_ROW;
  const mid = Math.max(rightHeight, leftHeight) / 2;
  const shiftSide = (branchIds: string[], height: number) => {
    const offset = mid - height / 2;
    if (offset === 0) return;
    for (const branchId of branchIds) {
      for (const id of [branchId, ...descendantsOf(nodes, branchId)]) {
        pos[id] = { ...pos[id], y: pos[id].y + offset };
      }
    }
  };
  shiftSide(right, rightHeight);
  shiftSide(left, leftHeight);
  pos[rootId] = { x: 0, y: mid };

  // Normalize so the leftmost node sits at x = 0 — the initial pan expects
  // content to start near the origin (negative x would render off-screen).
  const minX = Math.min(...Object.values(pos).map((p) => p.x));
  if (minX !== 0) {
    for (const id of Object.keys(pos)) pos[id] = { ...pos[id], x: pos[id].x - minX };
  }
  return pos;
}

/** All transitive descendants of a node (so dragging moves the whole subtree). */
function descendantsOf(nodes: Record<string, MMNode>, id: string): string[] {
  const out: string[] = [];
  const stack = [...nodes[id].childIds];
  while (stack.length) {
    const cur = stack.pop() as string;
    out.push(cur);
    stack.push(...nodes[cur].childIds);
  }
  return out;
}

/**
 * An interactive mindmap canvas: nodes are absolutely positioned from a tidy
 * auto-layout, joined by curved SVG edges. The user can PAN the whole canvas
 * (drag empty space) and DRAG any node (which carries its subtree). Nodes with
 * children expand/collapse; deeper levels start collapsed so a big map stays
 * tidy. All colors come from the themed tokens (UIConfigurator is the source of
 * truth); branch accents reuse the shared chart palette.
 */
const MindmapCanvas: React.FC<{ root: MindmapData }> = ({ root }) => {
  const { nodes, rootId } = useMemo(() => buildMindmap(root), [root]);
  const initial = useMemo(() => layoutMindmap(nodes, rootId), [nodes, rootId]);

  const [positions, setPositions] = useState<Record<string, XY>>(() => initial);
  const [collapsed, setCollapsed] = useState<Set<string>>(
    () => new Set(Object.values(nodes).filter((n) => n.depth >= 2 && n.childIds.length > 0).map((n) => n.id)),
  );
  // pan (x,y) + zoom (scale) in one object so a wheel-zoom updates both atomically.
  // The pan is re-centered on the root node as soon as the canvas mounts and
  // reports its size (see centerView), so the map opens centered.
  const [view, setView] = useState({ scale: 1, x: 48, y: 32 });
  const [grabbing, setGrabbing] = useState(false);
  // The node the pointer is currently over → drives the detail tooltip. Tracked
  // with mouse enter/leave (not the pointer-drag handlers), so hovering and
  // dragging stay independent.
  const [hovered, setHovered] = useState<string | null>(null);

  // Latest positions behind a ref so centerView stays identity-stable (it is a
  // dependency of the canvas ref callback — a new identity would re-attach the
  // wheel listener and re-center on every node drag).
  const positionsRef = useRef(positions);
  positionsRef.current = positions;

  const canvasRef = useRef<HTMLDivElement | null>(null);
  const sizeRef = useRef({ w: 0, h: 0 }); // viewport size, for centering button-zoom
  const wheelCleanup = useRef<(() => void) | null>(null);
  const dragRef = useRef<
    | { mode: 'pan'; startX: number; startY: number; panStart: XY }
    | { mode: 'node'; startX: number; startY: number; ids: string[]; orig: Record<string, XY> }
    | null
  >(null);

  const toggle = (id: string) =>
    setCollapsed((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });

  // Visible = every node not hidden beneath a collapsed ancestor.
  const visible = useMemo(() => {
    const vis = new Set<string>();
    const stack = [rootId];
    while (stack.length) {
      const id = stack.pop() as string;
      vis.add(id);
      if (!collapsed.has(id)) nodes[id].childIds.forEach((c) => stack.push(c));
    }
    return vis;
  }, [nodes, rootId, collapsed]);

  // Zoom around a viewport point (cx, cy): scale changes and pan shifts so the
  // point under the cursor stays put. One functional update → no stale state.
  const zoomAt = useCallback((factor: number, cx: number, cy: number) => {
    setView((v) => {
      const scale = Math.min(MM_MAX_ZOOM, Math.max(MM_MIN_ZOOM, v.scale * factor));
      const k = scale / v.scale;
      return { scale, x: cx - (cx - v.x) * k, y: cy - (cy - v.y) * k };
    });
  }, []);

  // Center the viewport on the root node at scale 1 — the initial view and the
  // "Reset view" button both land here, so the map opens symmetric around its
  // central node instead of anchored to the top-left corner.
  const centerView = useCallback(() => {
    const { w, h } = sizeRef.current;
    const p = positionsRef.current[rootId];
    setView({ scale: 1, x: w / 2 - p.x, y: h / 2 - p.y });
  }, [rootId]);

  // Callback ref: attach a NON-passive wheel listener (so we can preventDefault
  // the page scroll) when the canvas mounts; detach on unmount. Also stash the
  // element + size for the zoom buttons, and center the view now that the
  // viewport size is known.
  const canvasRefCb = useCallback(
    (el: HTMLDivElement | null) => {
      canvasRef.current = el;
      if (wheelCleanup.current) {
        wheelCleanup.current();
        wheelCleanup.current = null;
      }
      if (el) {
        sizeRef.current = { w: el.clientWidth, h: el.clientHeight };
        const handler = (e: WheelEvent) => {
          e.preventDefault();
          const r = el.getBoundingClientRect();
          zoomAt(e.deltaY < 0 ? 1.1 : 1 / 1.1, e.clientX - r.left, e.clientY - r.top);
        };
        el.addEventListener('wheel', handler, { passive: false });
        wheelCleanup.current = () => el.removeEventListener('wheel', handler);
        centerView();
      }
    },
    [zoomAt, centerView],
  );

  const zoomButton = (factor: number) => () => zoomAt(factor, sizeRef.current.w / 2, sizeRef.current.h / 2);
  const resetView = centerView;

  const startNodeDrag = (id: string) => (e: React.PointerEvent) => {
    e.stopPropagation(); // don't also pan the canvas
    const ids = [id, ...descendantsOf(nodes, id)];
    const orig: Record<string, XY> = {};
    ids.forEach((d) => (orig[d] = positions[d]));
    dragRef.current = { mode: 'node', startX: e.clientX, startY: e.clientY, ids, orig };
    setGrabbing(true);
  };
  const startPan = (e: React.PointerEvent) => {
    dragRef.current = { mode: 'pan', startX: e.clientX, startY: e.clientY, panStart: { x: view.x, y: view.y } };
    setGrabbing(true);
  };
  const onMove = (e: React.PointerEvent) => {
    const d = dragRef.current;
    if (!d) return;
    const dx = e.clientX - d.startX;
    const dy = e.clientY - d.startY;
    if (d.mode === 'pan') {
      setView((v) => ({ ...v, x: d.panStart.x + dx, y: d.panStart.y + dy }));
    } else {
      // Node positions live in world space; a screen delta is delta / scale there.
      setPositions((prev) => {
        const next = { ...prev };
        d.ids.forEach((id) => (next[id] = { x: d.orig[id].x + dx / view.scale, y: d.orig[id].y + dy / view.scale }));
        return next;
      });
    }
  };
  const endDrag = () => {
    dragRef.current = null;
    setGrabbing(false);
  };

  const visibleIds = Object.keys(positions).filter((id) => visible.has(id));
  // SVG sized to the content bounds so the edge layer covers every node.
  const maxX = visibleIds.reduce((m, id) => Math.max(m, positions[id].x), 0) + 240;
  const maxY = visibleIds.reduce((m, id) => Math.max(m, positions[id].y), 0) + 140;

  const gridSize = 22 * view.scale;

  return (
    <div
      data-mm-canvas=""
      ref={canvasRefCb}
      onPointerDown={startPan}
      onPointerMove={onMove}
      onPointerUp={endDrag}
      onPointerLeave={endDrag}
      style={{
        position: 'relative',
        height: '84vh',
        minHeight: 640,
        overflow: 'hidden',
        borderRadius: 14,
        border: `1px solid ${GLASS_BORDER}`,
        cursor: grabbing ? 'grabbing' : 'grab',
        touchAction: 'none',
        // faint dot grid that pans + zooms with the content
        backgroundImage: `radial-gradient(${GLASS_BORDER} 1px, transparent 1px)`,
        backgroundSize: `${gridSize}px ${gridSize}px`,
        backgroundPosition: `${view.x}px ${view.y}px`,
      }}
    >
      <div
        data-mm-world=""
        style={{ position: 'absolute', left: 0, top: 0, transformOrigin: '0 0', transform: `translate(${view.x}px, ${view.y}px) scale(${view.scale})` }}
      >
        <svg
          width={maxX}
          height={maxY}
          style={{ position: 'absolute', left: 0, top: 0, overflow: 'visible', pointerEvents: 'none', zIndex: 0 }}
        >
          {visibleIds
            .filter((id) => nodes[id].parentId !== null)
            .map((id) => {
              const a = positions[nodes[id].parentId as string];
              const b = positions[id];
              const midX = (a.x + b.x) / 2;
              return (
                <path
                  key={id}
                  d={`M ${a.x} ${a.y} C ${midX} ${a.y} ${midX} ${b.y} ${b.x} ${b.y}`}
                  fill="none"
                  stroke={nodes[id].color}
                  strokeWidth={2}
                  strokeOpacity={0.85}
                />
              );
            })}
        </svg>
        {visibleIds.map((id) => {
          const node = nodes[id];
          const isRoot = node.parentId === null;
          const p = positions[id];
          const hasKids = node.childIds.length > 0;
          const isCollapsed = collapsed.has(id);
          // Bilateral layout: nodes left of the root mirror their chrome (accent
          // bar on the outer edge, toggle facing outward) so both sides read
          // symmetrically from the center.
          const onLeft = !isRoot && p.x < positions[rootId].x;
          return (
            <div
              key={id}
              data-mm-node={id}
              onPointerDown={startNodeDrag(id)}
              onMouseEnter={() => setHovered(id)}
              onMouseLeave={() => setHovered((h) => (h === id ? null : h))}
              style={{
                position: 'absolute',
                left: p.x,
                top: p.y,
                // Above the connector SVG (zIndex 0) so the lines read as
                // being BEHIND every node.
                zIndex: 1,
                transform: 'translate(-50%, -50%)',
                display: 'inline-flex',
                flexDirection: onLeft ? 'row-reverse' : 'row',
                alignItems: 'center',
                gap: 8,
                cursor: 'grab',
                userSelect: 'none',
                touchAction: 'none',
                background: isRoot ? ACCENT : NODE_FILL,
                color: isRoot ? ON_ACCENT : TEXT,
                border: `1px solid ${isRoot ? ACCENT : GLASS_BORDER}`,
                ...(isRoot
                  ? { borderLeft: `1px solid ${ACCENT}` }
                  : onLeft
                    ? { borderRight: `3px solid ${node.color}` }
                    : { borderLeft: `3px solid ${node.color}` }),
                borderRadius: isRoot ? 14 : 11,
                padding: isRoot ? '11px 17px' : '8px 13px',
                fontWeight: isRoot ? 800 : 600,
                fontSize: isRoot ? '1.02rem' : '0.9rem',
                // FIXED width (not a cap) so the label has a stable box to wrap
                // its WORDS into; a flexible cap let the box collapse to a sliver
                // and break the text after ~2 characters. The width stays < MM_COL
                // so the node never overlaps the neighbouring depth column, and the
                // label wraps to at most TWO lines (see the label span) — a longer
                // description is clamped there and revealed in full on hover (title).
                width: isRoot ? MM_NODE_W + 20 : MM_NODE_W,
                boxShadow: isRoot ? '0 8px 26px rgba(0,0,0,0.30)' : '0 2px 10px rgba(0,0,0,0.16)',
              }}
            >
              {!isRoot && (
                <span aria-hidden="true" style={{ width: 7, height: 7, borderRadius: 99, background: node.color, flexShrink: 0 }} />
              )}
              <span
                style={{
                  // Fill the node's fixed width and wrap the label by WORD onto at
                  // most two lines, then clamp with an ellipsis: short / important
                  // labels read in full inline, while an over-long label is
                  // truncated here and stays available on hover via the detail
                  // tooltip below. `break-word` only splits a single word that is
                  // itself wider than the box — normal text breaks at spaces.
                  flex: 1,
                  minWidth: 0,
                  display: '-webkit-box',
                  WebkitBoxOrient: 'vertical',
                  WebkitLineClamp: 2,
                  overflow: 'hidden',
                  whiteSpace: 'normal',
                  overflowWrap: 'break-word',
                  lineHeight: 1.25,
                }}
              >
                {node.label}
              </span>
              {hasKids && (
                <button
                  type="button"
                  onPointerDown={(e) => e.stopPropagation()} // click to toggle, don't start a drag
                  onClick={() => toggle(id)}
                  aria-expanded={!isCollapsed}
                  aria-label={`${isCollapsed ? 'Expand' : 'Collapse'} ${node.label || 'node'}`}
                  title={isCollapsed ? `Expand (${node.childIds.length})` : 'Collapse'}
                  style={{
                    marginLeft: 2, minWidth: 20, height: 20, padding: '0 5px',
                    display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
                    borderRadius: 99, cursor: 'pointer', flexShrink: 0,
                    fontSize: '0.72rem', fontWeight: 700, lineHeight: 1, fontVariantNumeric: 'tabular-nums',
                    background: isRoot ? 'rgba(0,0,0,0.18)' : GLASS_STRONG,
                    color: isRoot ? ON_ACCENT : node.color,
                    border: `1px solid ${isRoot ? 'transparent' : GLASS_BORDER}`,
                  }}
                >
                  {isCollapsed ? `+${node.childIds.length}` : '−'}
                </button>
              )}
            </div>
          );
        })}
      </div>
      {/* Detail tooltip — shows the hovered node's full content (a separate
          description, else the full label when it was clamped to two lines).
          Rendered OUTSIDE the world transform so it stays unscaled and readable,
          positioned at the node's screen coordinates. Hidden while panning/dragging. */}
      {(() => {
        if (!hovered || grabbing) return null;
        const n = nodes[hovered];
        const p = positions[hovered];
        if (!n || !p) return null;
        // Only worth a tooltip when there is MORE than the inline label already
        // shows: a real detail, or a label long enough to have been clamped.
        if (!n.detail && n.label.length <= 44) return null;
        const sx = p.x * view.scale + view.x;
        const sy = p.y * view.scale + view.y;
        const lift = 34 * view.scale + 10; // clear the node's edge
        const below = sy < 150; // flip under the node when too near the top to fit above
        return (
          <div
            data-mm-tooltip=""
            style={{
              position: 'absolute',
              left: sx,
              top: below ? sy + lift : sy - lift,
              transform: below ? 'translate(-50%, 0)' : 'translate(-50%, -100%)',
              maxWidth: 300,
              padding: '8px 11px',
              borderRadius: 10,
              background: `linear-gradient(${GLASS_STRONG}, ${GLASS_STRONG}), var(--ui-surface-solid, #141b35)`,
              border: `1px solid ${GLASS_BORDER}`,
              color: TEXT,
              fontSize: '0.8rem',
              lineHeight: 1.4,
              whiteSpace: 'normal',
              overflowWrap: 'break-word',
              boxShadow: '0 10px 30px rgba(0,0,0,0.4)',
              pointerEvents: 'none',
              zIndex: 3,
            }}
          >
            {n.detail ? (
              <>
                <div style={{ fontWeight: 700, marginBottom: 3 }}>{n.label}</div>
                <div style={{ color: MUTED }}>{n.detail}</div>
              </>
            ) : (
              n.label
            )}
          </div>
        );
      })()}
      {/* Zoom controls (don't start a pan when pressed). */}
      <div style={{ position: 'absolute', top: 10, right: 10, display: 'flex', flexDirection: 'column', gap: 6, zIndex: 2 }}>
        {[
          { sym: '+', aria: 'Zoom in', on: zoomButton(1.2) },
          { sym: '−', aria: 'Zoom out', on: zoomButton(1 / 1.2) },
          { sym: '↺', aria: 'Reset view', on: resetView },
        ].map((b) => (
          <button
            key={b.aria}
            type="button"
            aria-label={b.aria}
            title={b.aria}
            onPointerDown={(e) => e.stopPropagation()}
            onClick={b.on}
            style={{
              width: 30, height: 30, display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
              borderRadius: 8, cursor: 'pointer', fontSize: '1rem', fontWeight: 700, lineHeight: 1,
              color: TEXT, background: GLASS, border: `1px solid ${GLASS_BORDER}`, boxShadow: '0 2px 8px rgba(0,0,0,0.18)',
            }}
          >
            {b.sym}
          </button>
        ))}
      </div>
      <div
        style={{ position: 'absolute', left: 12, bottom: 10, fontSize: '0.7rem', color: MUTED, pointerEvents: 'none', userSelect: 'none' }}
      >
        Drag to pan · scroll to zoom · drag a node to move it
      </div>
    </div>
  );
};

type AlbumImage = { url: string; alt: string; caption: string };

/**
 * An Album in "carousel" layout: ONE image per screen, navigated by the ← / →
 * arrow keys (mirrors SlidesNode's keyboard model), plus on-image prev/next
 * buttons, an "n / N" counter and clickable dots. Keyboard listener is only
 * attached when there's more than one image.
 */
const AlbumCarousel: React.FC<{ title?: string; items: AlbumImage[] }> = ({ title, items }) => {
  const [idx, setIdx] = useState(0);
  const count = items.length;

  useEffect(() => {
    if (count <= 1) return undefined;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'ArrowRight') setIdx((i) => Math.min(count - 1, i + 1));
      else if (e.key === 'ArrowLeft') setIdx((i) => Math.max(0, i - 1));
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [count]);

  const current = Math.min(idx, count - 1);
  const img = items[current];
  const atStart = current === 0;
  const atEnd = current === count - 1;
  const navBtn: React.CSSProperties = {
    position: 'absolute', top: '50%', transform: 'translateY(-50%)',
    width: 38, height: 38, borderRadius: 999, border: `1px solid ${GLASS_BORDER}`,
    background: GLASS_STRONG, color: TEXT, fontSize: 22, lineHeight: 1,
    display: 'flex', alignItems: 'center', justifyContent: 'center',
    backdropFilter: 'blur(8px)', WebkitBackdropFilter: 'blur(8px)',
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
      {title && <div style={{ color: TEXT, fontWeight: 700, fontSize: '1.05rem' }}>{title}</div>}
      <div style={{ position: 'relative' }}>
        <a href={sanitizeUrl(img.url)} target="_blank" rel="noopener noreferrer" style={{ display: 'block' }}>
          <img
            src={sanitizeImageSrc(img.url)}
            alt={img.alt}
            style={{ width: '100%', aspectRatio: '16 / 9', maxHeight: '70vh', objectFit: 'cover', borderRadius: 16, border: `1px solid ${GLASS_BORDER}`, display: 'block' }}
          />
        </a>
        {count > 1 && (
          <>
            <button
              type="button" aria-label="Previous image" disabled={atStart}
              onClick={() => setIdx((i) => Math.max(0, i - 1))}
              style={{ ...navBtn, left: 10, opacity: atStart ? 0.35 : 1, cursor: atStart ? 'default' : 'pointer' }}
            >‹</button>
            <button
              type="button" aria-label="Next image" disabled={atEnd}
              onClick={() => setIdx((i) => Math.min(count - 1, i + 1))}
              style={{ ...navBtn, right: 10, opacity: atEnd ? 0.35 : 1, cursor: atEnd ? 'default' : 'pointer' }}
            >›</button>
            <div style={{ position: 'absolute', top: 10, right: 12, background: GLASS_STRONG, color: TEXT, fontSize: 12, fontWeight: 600, padding: '2px 8px', borderRadius: 999, border: `1px solid ${GLASS_BORDER}` }}>
              {current + 1} / {count}
            </div>
          </>
        )}
      </div>
      {img.caption && <div style={{ fontSize: 13, color: MUTED, textAlign: 'center' }}>{img.caption}</div>}
      {count > 1 && (
        <div style={{ display: 'flex', justifyContent: 'center', gap: 8 }}>
          {items.map((_, i) => (
            <button
              key={i} type="button" aria-label={`Go to image ${i + 1}`} onClick={() => setIdx(i)}
              style={{ width: i === current ? 22 : 8, height: 8, borderRadius: 999, background: i === current ? ACCENT : GLASS_BORDER, border: 'none', padding: 0, cursor: 'pointer', transition: 'all 0.2s' }}
            />
          ))}
        </div>
      )}
    </div>
  );
};

type Flashcard = { front: string; back: string };

/**
 * An Anki-style flashcard deck: grid or carousel of cards, each showing its
 * QUESTION (front) and FLIPPING (a real 3D rotateY) to reveal the ANSWER (back)
 * on click. Self-graded — no scoring — so it complements the multiple-choice
 * Quiz. Both faces always exist in the DOM (backface-hidden); flipping just
 * rotates the card. All colors come from the themed tokens.
 */
const FlashcardsNode: React.FC<{ title?: string; cards: Flashcard[]; layout?: 'grid' | 'carousel' }> = ({ title, cards, layout = 'grid' }) => {
  const [flipped, setFlipped] = useState<Set<number>>(new Set());
  const [idx, setIdx] = useState(0);
  if (cards.length === 0) return null;

  const toggle = (i: number) =>
    setFlipped((prev) => {
      const next = new Set(prev);
      if (next.has(i)) next.delete(i);
      else next.add(i);
      return next;
    });

  // Keyboard navigation for carousel
  React.useEffect(() => {
    if (layout !== 'carousel') return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'ArrowRight') setIdx((i) => Math.min(cards.length - 1, i + 1));
      else if (e.key === 'ArrowLeft') setIdx((i) => Math.max(0, i - 1));
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [layout, cards.length]);

  // Shared face style: fills the card, centers content, hides its back side so
  // only the up-facing side shows during/after the flip.
  const face: React.CSSProperties = {
    ...CARD_STYLE,
    background: GLASS_STRONG,
    position: 'absolute',
    inset: 0,
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
    justifyContent: 'center',
    gap: 8,
    padding: 18,
    textAlign: 'center',
    backfaceVisibility: 'hidden',
    WebkitBackfaceVisibility: 'hidden',
  };
  const kicker: React.CSSProperties = {
    fontSize: 10, fontWeight: 700, letterSpacing: '0.08em', textTransform: 'uppercase',
  };

  // Render card component
  const renderCard = (c: Flashcard, i: number, isSelected: boolean) => {
    const isBack = flipped.has(i);
    return (
      <button
        key={i}
        type="button"
        onClick={() => toggle(i)}
        aria-pressed={isBack}
        aria-label={isBack ? `Answer: ${c.back}` : `Question: ${c.front}. Click to reveal the answer.`}
        style={{ perspective: 1000, background: 'none', border: 'none', padding: 0, height: 180, cursor: 'pointer', outline: 'none', boxShadow: 'none' }}
      >
        <div
          data-fc-inner=""
          style={{
            position: 'relative', width: '100%', height: '100%',
            transformStyle: 'preserve-3d', transition: 'transform 0.5s',
            transform: isBack ? 'rotateY(180deg)' : 'none',
          }}
        >
          {/* Front = question / term */}
          <div style={face}>
            <span style={{ ...kicker, color: MUTED }}>Question</span>
            <span style={{ color: TEXT, fontSize: '1.05rem', fontWeight: 600 }}>{c.front}</span>
            <span style={{ fontSize: 11, color: MUTED }}>Click to flip</span>
          </div>
          {/* Back = answer (pre-rotated so it faces out once flipped) */}
          <div style={{ ...face, transform: 'rotateY(180deg)' }}>
            <span style={{ ...kicker, color: ACCENT }}>Answer</span>
            <span style={{ color: TEXT, fontSize: '1rem' }}>{c.back}</span>
            <span style={{ fontSize: 11, color: MUTED }}>Click to flip back</span>
          </div>
        </div>
      </button>
    );
  };

  if (layout === 'carousel') {
    const current = Math.min(idx, cards.length - 1);
    const atStart = current === 0;
    const atEnd = current === cards.length - 1;
    const navBtn: React.CSSProperties = {
      position: 'absolute', top: '50%', transform: 'translateY(-50%)',
      width: 48, height: 48, borderRadius: 999, border: `1px solid ${GLASS_BORDER}`,
      background: GLASS_STRONG, color: TEXT, fontSize: 28, lineHeight: 1,
      display: 'flex', alignItems: 'center', justifyContent: 'center',
      backdropFilter: 'blur(8px)', WebkitBackdropFilter: 'blur(8px)', zIndex: 10,
    };

    // Carousel card with larger dimensions
    const carouselCardStyle: React.CSSProperties = {
      perspective: 1000, background: 'none', border: 'none', padding: 0,
      height: 500, width: '100%', maxWidth: '65vw', cursor: 'pointer',
      outline: 'none', boxShadow: 'none',
    };

    const carouselFace: React.CSSProperties = {
      ...CARD_STYLE,
      background: GLASS_STRONG,
      position: 'absolute',
      inset: 0,
      display: 'flex',
      flexDirection: 'column',
      alignItems: 'center',
      justifyContent: 'center',
      gap: 16,
      padding: 32,
      textAlign: 'center',
      backfaceVisibility: 'hidden',
      WebkitBackfaceVisibility: 'hidden',
    };

    const carouselKicker: React.CSSProperties = {
      fontSize: 13, fontWeight: 700, letterSpacing: '0.1em', textTransform: 'uppercase', color: MUTED,
    };

    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: 20, height: '100%', justifyContent: 'center' }}>
        {title && <div style={{ color: TEXT, fontWeight: 700, fontSize: '1.3rem', textAlign: 'center', marginBottom: -10 }}>{title}</div>}
        <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', flex: 1, minHeight: 450 }}>
          <div style={{ display: 'flex', justifyContent: 'center', position: 'relative', width: '100%', maxWidth: '70vw' }}>
            <button
              type="button"
              onClick={() => toggle(current)}
              aria-pressed={flipped.has(current)}
              aria-label={flipped.has(current) ? `Answer: ${cards[current].back}` : `Question: ${cards[current].front}. Click to reveal the answer.`}
              style={carouselCardStyle}
            >
              <div
                data-fc-inner=""
                style={{
                  position: 'relative', width: '100%', height: '100%',
                  transformStyle: 'preserve-3d', transition: 'transform 0.5s',
                  transform: flipped.has(current) ? 'rotateY(180deg)' : 'none',
                }}
              >
                {/* Front */}
                <div style={carouselFace}>
                  <span style={carouselKicker}>Question</span>
                  <span style={{ color: TEXT, fontSize: '1.4rem', fontWeight: 600, lineHeight: 1.4 }}>{cards[current].front}</span>
                  <span style={{ fontSize: 13, color: MUTED, marginTop: 12 }}>Tap to reveal answer</span>
                </div>
                {/* Back */}
                <div style={{ ...carouselFace, transform: 'rotateY(180deg)' }}>
                  <span style={{ ...carouselKicker, color: ACCENT }}>Answer</span>
                  <span style={{ color: TEXT, fontSize: '1.2rem', lineHeight: 1.5 }}>{cards[current].back}</span>
                  <span style={{ fontSize: 13, color: MUTED, marginTop: 12 }}>Tap to flip back</span>
                </div>
              </div>
            </button>
            {cards.length > 1 && (
              <div style={{ position: 'absolute', top: 16, right: 16, background: GLASS_STRONG, color: TEXT, fontSize: 14, fontWeight: 700, padding: '6px 12px', borderRadius: 999, border: `1px solid ${GLASS_BORDER}` }}>
                {current + 1} / {cards.length}
              </div>
            )}
          </div>
        </div>

        {cards.length > 1 && (
          <div style={{ display: 'flex', justifyContent: 'center', gap: 8, marginTop: 8 }}>
            {cards.map((_, i) => (
              <button
                key={i} type="button" aria-label={`Go to flashcard ${i + 1}`} onClick={() => setIdx(i)}
                style={{ width: i === current ? 24 : 10, height: 10, borderRadius: 999, background: i === current ? ACCENT : GLASS_BORDER, border: 'none', padding: 0, cursor: 'pointer', transition: 'all 0.2s' }}
              />
            ))}
          </div>
        )}
      </div>
    );
  }

  // Grid layout (default)
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
      {title && <div style={{ color: TEXT, fontWeight: 700, fontSize: '1.05rem' }}>{title}</div>}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(240px, 1fr))', gap: 14 }}>
        {cards.map((c, i) => renderCard(c, i, false))}
      </div>
    </div>
  );
};

/**
 * A whitelisted, appearance-only subset of an agent-provided `style` object.
 * The renderer imposes its own premium theme by default, but a node may
 * intentionally override appearance — e.g. a "change the background to black"
 * refine sets `style.background` on the Slides root. Without honoring it the
 * refine produces a valid new document that renders identically, so it looks
 * like "nothing happened". We pass through only visual props (color/background/
 * typography) and never layout (display/position/width/height/margin/padding-
 * flow) so an override can't break the deck's structure.
 */
const STYLE_PASSTHROUGH = [
  'background', 'backgroundColor', 'color', 'border', 'borderColor',
  'borderRadius', 'boxShadow', 'opacity', 'textAlign', 'fontWeight',
  'fontSize', 'fontStyle', 'textDecoration', 'letterSpacing', 'lineHeight',
] as const;

function extractNodeStyle(node: UiComponent | undefined): React.CSSProperties {
  const raw = node && typeof node.style === 'object' && node.style
    ? (node.style as Record<string, unknown>)
    : null;
  if (!raw) return {};
  const out: Record<string, unknown> = {};
  for (const key of STYLE_PASSTHROUGH) {
    const v = raw[key];
    if (typeof v === 'string' || typeof v === 'number') out[key] = v;
  }
  return out as React.CSSProperties;
}

const Node: React.FC<{
  id: string;
  surface: UiSurface;
  data: Record<string, unknown>;
  setData: (path: string, value: unknown) => void;
  seen: Set<string>;
  /** Text color cascaded from an ancestor that set `style.color` (default theme TEXT). */
  inheritedColor?: string;
}> = ({ id, surface, data, setData, seen, inheritedColor = TEXT }) => {
  const node: UiComponent | undefined = surface.components[id];
  if (!node || seen.has(id)) return null;
  const nextSeen = new Set(seen).add(id);
  // Honor a node's own appearance overrides, and cascade its text color to
  // descendants so a "black background / white text" override reads correctly.
  const nodeStyle = extractNodeStyle(node);
  const myColor = typeof nodeStyle.color === 'string' ? nodeStyle.color : inheritedColor;
  const renderChild = (childId: string) => (
    <Node key={childId} id={childId} surface={surface} data={data} setData={setData} seen={nextSeen} inheritedColor={myColor} />
  );
  const pathOf = (v: unknown): string =>
    v && typeof v === 'object' && 'path' in (v as Record<string, unknown>)
      ? String((v as Record<string, unknown>).path)
      : '';

  switch (node.component) {
    case 'Text': {
      const variant = typeof node.variant === 'string' ? node.variant : 'body';
      const variantStyle = TEXT_STYLES[variant] || TEXT_STYLES.body;
      // Headings adopt the theme's heading color when set, else the cascaded text
      // color (the nested var() keeps un-themed docs identical to before).
      const isHeading = variant === 'h1' || variant === 'h2' || variant === 'h3';
      const baseColor = isHeading ? `var(--ui-heading, ${myColor})` : myColor;
      // Precedence: explicit node style > variant's own color (e.g. muted body) > inherited.
      return <div style={{ color: baseColor, ...variantStyle, ...nodeStyle }}>{String(resolveValue(node.text, data) ?? '')}</div>;
    }
    case 'Row':
    case 'Column': {
      const isRow = node.component === 'Row';
      const childIds = Array.isArray(node.children) ? node.children : [];
      return (
        <div style={{
          display: 'flex', flexDirection: isRow ? 'row' : 'column', gap: 18,
          justifyContent: JUSTIFY[String(node.justify)] || 'flex-start',
          alignItems: ALIGN[String(node.align)] || 'stretch',
          flexWrap: isRow ? 'wrap' : 'nowrap',
          ...nodeStyle,
        }}>
          {/* Row children become equal, flexible columns so they fill the width
              (no dead space / tiny side element). Columns stack as-is. */}
          {isRow
            ? childIds.map((cid) => (
                <div key={cid} style={{ flex: '1 1 0', minWidth: 260, display: 'flex', flexDirection: 'column' }}>
                  {renderChild(cid)}
                </div>
              ))
            : childIds.map(renderChild)}
        </div>
      );
    }
    case 'Button':
      return (
        <button type="button" className="transition-all hover:opacity-90"
          style={{ background: ACCENT, color: ON_ACCENT, fontWeight: 600, border: 'none', borderRadius: 10, padding: '9px 16px', alignSelf: 'flex-start', cursor: 'pointer' }}>
          {typeof node.child === 'string' ? renderChild(node.child) : 'Action'}
        </button>
      );
    case 'TextField': {
      const path = pathOf(node.value);
      return (
        <label style={{ display: 'flex', flexDirection: 'column', gap: 4, flex: node.weight ? String(node.weight) : undefined }}>
          {node.label && <span style={{ fontSize: 10, fontWeight: 700, letterSpacing: '0.08em', textTransform: 'uppercase', color: MUTED }}>{String(node.label)}</span>}
          <input value={String(resolveValue(node.value, data) ?? '')} onChange={(e) => path && setData(path, e.target.value)}
            style={{ background: 'rgba(0,0,0,0.25)', color: TEXT, border: `1px solid ${GLASS_BORDER}`, borderRadius: 10, padding: '9px 12px', outline: 'none' }} />
        </label>
      );
    }
    case 'Card': {
      const childIds = Array.isArray(node.children) ? node.children : [];
      return (
        <div style={{ ...CARD_STYLE, padding: 18, display: 'flex', flexDirection: 'column', gap: 10, flex: node.weight ? String(node.weight) : undefined, ...nodeStyle }}>
          {node.title != null && <div style={{ color: myColor, fontWeight: 700, fontSize: '1.05rem' }}>{String(resolveValue(node.title, data))}</div>}
          {childIds.map(renderChild)}
        </div>
      );
    }
    case 'List': {
      const childIds = Array.isArray(node.children) ? node.children : [];
      return (
        <div style={{ display: 'flex', flexDirection: 'column' }}>
          {childIds.map((cid, i) => (
            <div key={cid} style={{ padding: '9px 0', borderTop: i === 0 ? 'none' : `1px solid ${GLASS_BORDER}` }}>{renderChild(cid)}</div>
          ))}
        </div>
      );
    }
    case 'Divider':
      return <div style={{ height: 1, background: GLASS_BORDER, margin: '6px 0' }} />;
    case 'Image': {
      const url = sanitizeImageSrc(String(resolveValue(node.url, data) ?? ''));
      if (!url) return null;
      return <img src={url} alt={node.alt ? String(node.alt) : ''} style={{ maxWidth: '100%', borderRadius: 12, display: 'block' }} />;
    }
    case 'Album': {
      // A gallery of existing image links. Accepts `images: [{url, alt?, caption?}]`
      // or a plain `urls: ["..."]`. Each tile links to its source. `layout:
      // "carousel"` (aka "slideshow") shows ONE image per screen, navigable by the
      // ← / → keys + prev/next buttons (see AlbumCarousel); default is a grid.
      const rawItems = (Array.isArray(node.images)
        ? node.images
        : Array.isArray(node.urls)
          ? node.urls
          : []) as unknown[];
      const items = rawItems
        .map((it) => {
          if (typeof it === 'string') return { url: it, alt: '', caption: '' };
          if (it && typeof it === 'object') {
            const o = it as Record<string, unknown>;
            return {
              url: String(resolveValue(o.url ?? o.src ?? o.link, data) ?? ''),
              alt: String(o.alt ?? o.caption ?? o.title ?? ''),
              caption: String(o.caption ?? o.title ?? ''),
            };
          }
          return { url: '', alt: '', caption: '' };
        })
        .filter((i) => i.url);
      if (items.length === 0) return null;
      const isCarousel = node.layout === 'carousel' || node.layout === 'slideshow';
      const titleText = node.title != null ? String(resolveValue(node.title, data)) : undefined;
      // Carousel = one image per screen, keyboard / button navigable.
      if (isCarousel) {
        return (
          <div style={{ ...nodeStyle }}>
            <AlbumCarousel title={titleText} items={items} />
          </div>
        );
      }
      // Default = responsive grid of all images.
      return (
        <div style={{ ...nodeStyle }}>
          {titleText != null && (
            <div style={{ color: myColor, fontWeight: 700, fontSize: '1.05rem', marginBottom: 10 }}>{titleText}</div>
          )}
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(180px, 1fr))', gap: 12 }}>
            {items.map((img, i) => (
              <a
                key={i}
                href={sanitizeUrl(img.url)}
                target="_blank"
                rel="noopener noreferrer"
                style={{ display: 'flex', flexDirection: 'column', gap: 6, textDecoration: 'none' }}
              >
                <img
                  src={sanitizeImageSrc(img.url)}
                  alt={img.alt}
                  loading="lazy"
                  style={{ width: '100%', aspectRatio: '4 / 3', objectFit: 'cover', borderRadius: 12, border: `1px solid ${GLASS_BORDER}`, display: 'block' }}
                />
                {img.caption && <span style={{ fontSize: 12, color: MUTED }}>{img.caption}</span>}
              </a>
            ))}
          </div>
        </div>
      );
    }
    case 'Icon': {
      const d = iconPath(String(node.name ?? ''));
      if (!d) return <span style={{ width: 10, height: 10, borderRadius: '50%', background: ACCENT, display: 'inline-block' }} />;
      return (
        <svg width={26} height={26} viewBox="0 0 24 24" fill="none" stroke={ACCENT} strokeWidth={1.8} strokeLinecap="round" strokeLinejoin="round">
          <path d={d} />
        </svg>
      );
    }
    case 'Badge': {
      const color = TONE[String(node.tone ?? 'neutral')] || TONE.neutral;
      return (
        <span style={{ fontSize: 11.5, padding: '4px 11px', borderRadius: 999, color, border: `1px solid ${color}66`, background: `${color}1a`, alignSelf: 'flex-start', fontWeight: 600 }}>
          {String(resolveValue(node.text, data) ?? '')}
        </span>
      );
    }
    case 'CheckBox': {
      const path = pathOf(node.value);
      return (
        <label style={{ display: 'flex', alignItems: 'center', gap: 8, color: TEXT }}>
          <input type="checkbox" checked={Boolean(resolveValue(node.value, data))} onChange={(e) => path && setData(path, e.target.checked)} style={{ accentColor: ACCENT }} />
          {node.label && <span style={{ fontSize: '0.95rem' }}>{String(node.label)}</span>}
        </label>
      );
    }
    case 'ChoicePicker': {
      const path = pathOf(node.value);
      const selected = String(resolveValue(node.value, data) ?? '');
      const options = (Array.isArray(node.options) ? node.options : []).map((o) =>
        o && typeof o === 'object'
          ? { label: String((o as Record<string, unknown>).label ?? (o as Record<string, unknown>).value ?? ''), value: String((o as Record<string, unknown>).value ?? (o as Record<string, unknown>).label ?? '') }
          : { label: String(o), value: String(o) });
      return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
          {node.label && <span style={{ fontSize: 10, fontWeight: 700, letterSpacing: '0.08em', textTransform: 'uppercase', color: MUTED }}>{String(node.label)}</span>}
          {options.map((opt) => (
            <label key={opt.value} style={{ display: 'flex', alignItems: 'center', gap: 8, color: TEXT }}>
              <input type="radio" name={`cp-${id}`} checked={selected === opt.value} onChange={() => path && setData(path, opt.value)} style={{ accentColor: ACCENT }} />
              <span style={{ fontSize: '0.95rem' }}>{opt.label}</span>
            </label>
          ))}
        </div>
      );
    }
    case 'Dashboard': {
      const childIds = Array.isArray(node.children) ? node.children : [];
      return (
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(190px, 1fr))', gap: 14, ...nodeStyle }}>
          {childIds.map(renderChild)}
        </div>
      );
    }
    case 'Stat': {
      const color = TONE[String(node.tone ?? 'neutral')] || TONE.neutral;
      return (
        <div style={{ ...CARD_STYLE, background: GLASS_STRONG, padding: 16 }}>
          <div style={{ fontSize: 11, fontWeight: 700, letterSpacing: '0.08em', textTransform: 'uppercase', color: MUTED }}>{String(resolveValue(node.label, data) ?? '')}</div>
          <div style={{ fontSize: '1.7rem', fontWeight: 800, color: TEXT, marginTop: 5, lineHeight: 1.1 }}>{String(resolveValue(node.value, data) ?? '')}</div>
          {node.delta != null && String(node.delta) !== '' && (
            <div style={{ fontSize: '0.82rem', fontWeight: 600, color, marginTop: 4 }}>{String(node.delta)}</div>
          )}
        </div>
      );
    }
    case 'Chart': {
      const raw = (resolveValue(node.data, data) as unknown) ?? node.data;
      const dataPoints = Array.isArray(raw)
        ? (raw as Record<string, unknown>[]).map((d) => ({ label: String(d?.label ?? ''), value: Number(d?.value) }))
        : [];
      return (
        <div style={{ ...CARD_STYLE, padding: 16, flex: node.weight ? String(node.weight) : undefined }}>
          <ChartView chartType={String(node.chartType ?? 'bar')} data={dataPoints} title={node.title != null ? String(node.title) : undefined} />
        </div>
      );
    }
    case 'Table': {
      const columns = (Array.isArray(node.columns) ? node.columns : []).map((c) => String(c));
      // resolveValue returns a literal array as-is and resolves `{path}` bindings,
      // so this single check covers literal, bound and missing rows.
      const resolvedRows = resolveValue(node.rows, data);
      const rawRows = Array.isArray(resolvedRows) ? resolvedRows : [];
      const cell = (v: unknown) => (v === null || v === undefined ? '' : typeof v === 'object' ? JSON.stringify(v) : String(v));
      const rowToCells = (r: unknown): string[] =>
        Array.isArray(r)
          ? r.map(cell)
          : (r && typeof r === 'object'
            ? columns.map((c) => cell((r as Record<string, unknown>)[c]))
            : [cell(r)]);
      const rows = (rawRows as unknown[]).map(rowToCells);
      return (
        <div style={{ ...CARD_STYLE, padding: 4, overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.95rem' }}>
            {columns.length > 0 && (
              <thead>
                <tr>
                  {columns.map((c) => (
                    <th key={c} style={{ textAlign: 'left', padding: '10px 12px', color: MUTED, fontSize: '0.78rem', textTransform: 'uppercase', letterSpacing: '0.05em', borderBottom: `1px solid ${GLASS_BORDER}`, whiteSpace: 'nowrap' }}>{c}</th>
                  ))}
                </tr>
              </thead>
            )}
            <tbody>
              {rows.map((cells, ri) => (
                <tr key={ri}>
                  {cells.map((val, ci) => (
                    // Themed tokens (not a hardcoded light color): TEXT falls back to
                    // the dark-theme default when unthemed, but follows --ui-text on a
                    // light theme so cells stay readable instead of washing out.
                    <td key={ci} style={{ padding: '9px 12px', color: TEXT, borderBottom: `1px solid ${GLASS_BORDER}` }}>{val}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      );
    }
    case 'Slide': {
      const childIds = Array.isArray(node.children) ? node.children : [];
      return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 22, ...nodeStyle }}>
          {node.title != null && (
            <div style={{ color: myColor, fontWeight: 800, fontSize: '2.9rem', letterSpacing: '-0.025em', lineHeight: 1.08 }}>{String(resolveValue(node.title, data))}</div>
          )}
          {childIds.map(renderChild)}
        </div>
      );
    }
    case 'Slides': {
      const childIds = Array.isArray(node.children) ? node.children : [];
      return <SlidesNode childIds={childIds} renderChild={renderChild} />;
    }
    case 'Quiz': {
      const rawQ = (resolveValue(node.questions, data) as unknown) ?? node.questions;
      const questions = (Array.isArray(rawQ) ? rawQ : []).map((q) => {
        const obj = (q || {}) as Record<string, unknown>;
        return {
          question: String(obj.question ?? obj.prompt ?? ''),
          options: Array.isArray(obj.options) ? obj.options.map((o) => String(o)) : [],
          answer: obj.answer as number | string | undefined,
          explanation: obj.explanation != null ? String(obj.explanation) : undefined,
        };
      }).filter((q) => q.question && q.options.length > 0);
      return <QuizNode title={node.title != null ? String(node.title) : undefined} questions={questions} />;
    }
    case 'Flashcards': {
      // Anki-style flip cards. Accept `cards` (or `items`) and tolerate the
      // common key synonyms agents use for the two faces. Supports `layout` property
      // for grid (default) or carousel (one per screen) display.
      const rawCards = (resolveValue(node.cards, data) as unknown) ?? node.cards ?? node.items;
      const cards = (Array.isArray(rawCards) ? rawCards : []).map((c) => {
        const o = (c || {}) as Record<string, unknown>;
        return {
          front: String(o.front ?? o.question ?? o.term ?? o.q ?? ''),
          back: String(o.back ?? o.answer ?? o.definition ?? o.a ?? ''),
        };
      }).filter((c) => c.front || c.back);
      const layout = typeof node.layout === 'string' ? (node.layout as 'grid' | 'carousel') : 'grid';
      return (
        <div style={{ ...nodeStyle }}>
          <FlashcardsNode title={node.title != null ? String(node.title) : undefined} cards={cards} layout={layout} />
        </div>
      );
    }
    case 'Mindmap': {
      // The tree lives in `root` (or `data`/`tree`): a nested
      // { label, children:[{label, children:[…]}] } structure.
      const rawRoot = (node.root ?? node.data ?? node.tree) as MindmapData | undefined;
      if (!rawRoot || typeof rawRoot !== 'object') return null;
      return (
        <div style={{ ...nodeStyle }}>
          {node.title != null && (
            <div style={{ color: myColor, fontWeight: 700, fontSize: '1.05rem', marginBottom: 10 }}>
              {String(resolveValue(node.title, data))}
            </div>
          )}
          <MindmapCanvas root={rawRoot} />
        </div>
      );
    }
    default:
      return null;
  }
};

const UiRenderer: React.FC<UiRendererProps> = ({ surface }) => {
  const [data, setLocalData] = useState<Record<string, unknown>>(surface.data || {});

  const setData = (path: string, value: unknown) => {
    const segments = path.split('/').filter(Boolean);
    if (segments.length === 0) return;
    setLocalData((prev) => {
      const next = { ...prev };
      let cur: Record<string, unknown> = next;
      for (let i = 0; i < segments.length - 1; i++) {
        const seg = segments[i];
        cur[seg] = { ...((cur[seg] as Record<string, unknown>) || {}) };
        cur = cur[seg] as Record<string, unknown>;
      }
      cur[segments[segments.length - 1]] = value;
      return next;
    });
  };

  // The root node (typically Slides) drives the stage: a refine like "change the
  // background to black" sets style.background/color there. Fall back to the
  // built-in premium theme when the agent specifies nothing.
  const rootStyle = extractNodeStyle(surface.components[surface.rootId]);
  const theme = surface.theme;
  const isDeck = surface.components[surface.rootId]?.component === 'Slides';
  // The surface theme defines the --ui-* CSS vars on the stage; an explicit
  // root-node style override (e.g. a "make the background black" refine) still wins.
  const stageBackground = rootStyle.background ?? rootStyle.backgroundColor ?? STAGE_BG;
  const stageColor = typeof rootStyle.color === 'string' ? rootStyle.color : TEXT;
  const fontFamily = (theme?.font && FONT_STACK[theme.font]) || FONT_STACK.sans;
  const pad = theme?.density === 'compact' ? '22px 30px' : '36px 48px';

  return (
    <div style={{ ...(isDeck ? DECK_THEME_VARS : {}), ...themeVars(theme), minHeight: '100%', background: stageBackground, padding: pad, color: stageColor, fontFamily }}>
      <div style={{ maxWidth: 1280, margin: '0 auto' }}>
        <Node id={surface.rootId} surface={surface} data={data} setData={setData} seen={new Set()} inheritedColor={stageColor} />
      </div>
    </div>
  );
};

export default UiRenderer;
