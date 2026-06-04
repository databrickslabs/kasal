import React, { useState, useEffect } from 'react';
import { UiComponent, UiSurface, resolveValue } from '../../utils/uiDocument';

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
const ACCENT = '#5aa2ff';
const TEXT = '#eaf0ff';
const MUTED = '#aab3d4';
const GLASS = 'rgba(255,255,255,0.06)';
const GLASS_STRONG = 'rgba(255,255,255,0.10)';
const GLASS_BORDER = 'rgba(255,255,255,0.16)';
const STAGE_BG =
  'radial-gradient(1100px 560px at 12% -10%, rgba(90,162,255,0.20), transparent 60%),' +
  'radial-gradient(900px 520px at 92% 8%, rgba(167,139,250,0.18), transparent 55%),' +
  'linear-gradient(135deg, #0b1020, #131a33 45%, #1b2347)';
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
  body: { fontSize: '1.3rem', lineHeight: 1.55, color: '#dbe3ff' },
};

const JUSTIFY: Record<string, string> = {
  start: 'flex-start', center: 'center', end: 'flex-end', stretch: 'stretch',
  spaceBetween: 'space-between', spaceAround: 'space-around', spaceEvenly: 'space-evenly',
};
const ALIGN: Record<string, string> = {
  start: 'flex-start', center: 'center', end: 'flex-end', stretch: 'stretch',
};

const CHART_PALETTE = ['#5aa2ff', '#34d6b6', '#fbbf24', '#fb7185', '#a78bfa', '#38bdf8', '#f472b6'];

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

interface ChartPoint {
  label: string;
  value: number;
}

/** Lightweight SVG chart (bar / line / pie) — no external chart dependency. */
const ChartView: React.FC<{ chartType: string; data: ChartPoint[]; title?: string }> = ({ chartType, data, title }) => {
  const points = data.filter((d) => d && typeof d.value === 'number' && isFinite(d.value));
  if (points.length === 0) return null;
  const max = Math.max(1, ...points.map((p) => Math.abs(p.value)));
  const W = 520;
  const H = 240;

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
            <text x="17" y="10" fontSize="11.5" fill={MUTED}>{`${p.label} (${p.value})`}</text>
          </g>
        ))}
      </svg>
    );
  } else if (chartType === 'line') {
    const stepX = (W - 32) / Math.max(1, points.length - 1);
    const coords = points.map((p, i) => [16 + i * stepX, H - 28 - (Math.abs(p.value) / max) * (H - 52)]);
    const poly = coords.map((c) => c.join(',')).join(' ');
    const area = `16,${H - 28} ${poly} ${16 + (points.length - 1) * stepX},${H - 28}`;
    chart = (
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" height={H}>
        <polygon points={area} fill="rgba(90,162,255,0.15)" />
        <polyline points={poly} fill="none" stroke={ACCENT} strokeWidth={2.5} />
        {coords.map((c, i) => <circle key={i} cx={c[0]} cy={c[1]} r={3.5} fill={ACCENT} />)}
        {points.map((p, i) => (
          <text key={`t${i}`} x={coords[i][0]} y={H - 10} fontSize="10" textAnchor="middle" fill={MUTED}>{p.label}</text>
        ))}
      </svg>
    );
  } else {
    const bw = (W - 32) / points.length;
    chart = (
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" height={H}>
        {points.map((p, i) => {
          const h = (Math.abs(p.value) / max) * (H - 44);
          return (
            <g key={i}>
              <rect x={16 + i * bw + bw * 0.15} y={H - 26 - h} width={bw * 0.7} height={h} rx={4} fill={CHART_PALETTE[i % CHART_PALETTE.length]} />
              <text x={16 + i * bw + bw / 2} y={H - 9} fontSize="10" textAnchor="middle" fill={MUTED}>{p.label}</text>
            </g>
          );
        })}
      </svg>
    );
  }

  return (
    <div>
      {title && <div style={{ color: TEXT, fontWeight: 600, marginBottom: 6 }}>{title}</div>}
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
      <div style={{ flex: 1, minHeight: 0, overflowY: 'auto', display: 'flex', flexDirection: 'column', justifyContent: 'center' }}>
        {renderChild(childIds[current])}
      </div>
      {/* Clickable dots (keyboard ← / → also navigate) */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 12, paddingTop: 20 }}>
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
          style={{ background: ACCENT, color: '#06122b', fontWeight: 700, border: 'none', borderRadius: 10, padding: '10px 18px', alignSelf: 'center', cursor: 'pointer' }}>
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
              style={{ background: ACCENT, color: '#06122b', fontWeight: 700, border: 'none', borderRadius: 10, padding: '9px 18px', cursor: 'pointer' }}>
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

const Node: React.FC<{
  id: string;
  surface: UiSurface;
  data: Record<string, unknown>;
  setData: (path: string, value: unknown) => void;
  seen: Set<string>;
}> = ({ id, surface, data, setData, seen }) => {
  const node: UiComponent | undefined = surface.components[id];
  if (!node || seen.has(id)) return null;
  const nextSeen = new Set(seen).add(id);
  const renderChild = (childId: string) => (
    <Node key={childId} id={childId} surface={surface} data={data} setData={setData} seen={nextSeen} />
  );
  const pathOf = (v: unknown): string =>
    v && typeof v === 'object' && 'path' in (v as Record<string, unknown>)
      ? String((v as Record<string, unknown>).path)
      : '';

  switch (node.component) {
    case 'Text': {
      const variant = typeof node.variant === 'string' ? node.variant : 'body';
      const style = TEXT_STYLES[variant] || TEXT_STYLES.body;
      return <div style={{ color: TEXT, ...style }}>{String(resolveValue(node.text, data) ?? '')}</div>;
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
          style={{ background: ACCENT, color: '#06122b', fontWeight: 600, border: 'none', borderRadius: 10, padding: '9px 16px', alignSelf: 'flex-start', cursor: 'pointer' }}>
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
        <div style={{ ...CARD_STYLE, padding: 18, display: 'flex', flexDirection: 'column', gap: 10, flex: node.weight ? String(node.weight) : undefined }}>
          {node.title != null && <div style={{ color: TEXT, fontWeight: 700, fontSize: '1.05rem' }}>{String(resolveValue(node.title, data))}</div>}
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
      const url = String(resolveValue(node.url, data) ?? '');
      if (!url) return null;
      return <img src={url} alt={node.alt ? String(node.alt) : ''} style={{ maxWidth: '100%', borderRadius: 12, display: 'block' }} />;
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
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(190px, 1fr))', gap: 14 }}>
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
                    <td key={ci} style={{ padding: '9px 12px', color: '#dbe3ff', borderBottom: `1px solid rgba(255,255,255,0.07)` }}>{val}</td>
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
        <div style={{ display: 'flex', flexDirection: 'column', gap: 22 }}>
          {node.title != null && (
            <div style={{ color: TEXT, fontWeight: 800, fontSize: '2.6rem', letterSpacing: '-0.02em', lineHeight: 1.1 }}>{String(resolveValue(node.title, data))}</div>
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

  return (
    <div style={{ minHeight: '100%', background: STAGE_BG, padding: '36px 48px', color: TEXT, fontFamily: 'Inter, system-ui, sans-serif' }}>
      <div style={{ maxWidth: 1280, margin: '0 auto' }}>
        <Node id={surface.rootId} surface={surface} data={data} setData={setData} seen={new Set()} />
      </div>
    </div>
  );
};

export default UiRenderer;
