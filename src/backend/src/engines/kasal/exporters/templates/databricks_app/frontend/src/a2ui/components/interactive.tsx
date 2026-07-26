/**
 * Interactive study surfaces: Quiz and Flashcards.
 */
import { useContext, useState } from 'react'
import type { NodeProps } from '../types'
import { Check, ChevronLeft, ChevronRight, Lightbulb, RotateCcw, RotateCw, Shuffle, Trophy, X } from 'lucide-react'
import { Card } from '../ui/card'
import { Button } from '../ui/button'
import { DeckThemeContext } from '../lib/deckThemes'
import { cn } from '../lib/utils'
import { asArr, asStr } from './values'

// FNV-1a hash of a string -> uint32 seed (stable, dependency-free).
function hashStr(s: string): number {
  let h = 2166136261
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i)
    h = Math.imul(h, 16777619)
  }
  return h >>> 0
}

// A deterministic permutation of [0..n-1] from a seed (seeded Fisher-Yates with a
// small LCG). The same seed always yields the same order, so calling it inline on
// every render is stable — no hook / state needed.
function shuffleIndices(n: number, seed: number): number[] {
  const idx = Array.from({ length: n }, (_, i) => i)
  let s = (seed || 1) >>> 0
  for (let i = n - 1; i > 0; i--) {
    s = (Math.imul(s, 1664525) + 1013904223) >>> 0
    const j = s % (i + 1)
    const t = idx[i]
    idx[i] = idx[j]
    idx[j] = t
  }
  return idx
}

// ---- Quiz (interactive multiple-choice assessment) -------------------------
// One question at a time with Prev/Next + progress dots (mirrors SlideDeck), an
// immediate right/wrong reveal on select, and a final score summary. Self-grades
// from each question's `answer` index — the composer supplies ONLY the data.
type QuizQuestion = {
  question?: unknown
  options?: unknown
  answer?: unknown
  explanation?: unknown
}

export function Quiz({ node, resolve }: NodeProps) {
  const title = asStr(resolve(node.title))
  const questions = asArr(resolve(node.questions)) as QuizQuestion[]
  const total = questions.length
  // idx ranges over [0, total]; idx === total is the results summary (the deck's
  // "closing slide" analogue). Hooks run before the empty-guard so order is stable.
  const [idx, setIdx] = useState(0)
  const [picked, setPicked] = useState<Record<number, number>>({})
  // Themed like a slide deck: the active theme flows in via DeckThemeContext from
  // the QuizSurface picker (App.tsx). Hook runs before the empty-guard so the hook
  // order is stable.
  const theme = useContext(DeckThemeContext)
  if (!total) return null

  const clamp = (n: number) => Math.max(0, Math.min(total, n))
  const correctOf = (i: number) => Number(questions[i]?.answer)
  const score = questions.reduce(
    (acc, q, i) => acc + (picked[i] === Number(q.answer) ? 1 : 0),
    0,
  )
  const onResults = idx >= total

  const OK = '#10b981'
  const BAD = '#ef4444'

  // A slim progress meter that tracks how far through the quiz the user is — a
  // staple of the quiz "feel" (1-indexed so question 1 already reads as progress).
  const progressPct = onResults ? 100 : Math.round(((idx + 1) / total) * 100)
  const progress = (
    <div className="h-2 w-full overflow-hidden rounded-full" style={{ background: theme.panel }}>
      <div
        className="h-full rounded-full transition-all duration-500 ease-out"
        style={{ width: `${progressPct}%`, background: theme.accent }}
      />
    </div>
  )

  // Navigation dots double as an answer key: green = answered correctly, red =
  // answered wrong, muted = unseen. The current question gets an accent pill.
  const dots = (
    <div className="flex flex-wrap items-center justify-center gap-1.5">
      {Array.from({ length: total + 1 }).map((_, i) => {
        const isCur = i === idx
        let bg = theme.muted
        let op = 0.4
        if (i === total) {
          bg = theme.accent
          op = 0.9
        } else if (picked[i] != null) {
          bg = picked[i] === correctOf(i) ? OK : BAD
          op = 0.95
        }
        return (
          <button
            key={i}
            aria-label={i === total ? 'Results' : `Go to question ${i + 1}`}
            onClick={() => setIdx(i)}
            className="h-2 rounded-full transition-all"
            style={isCur ? { width: 22, background: theme.accent } : { width: 8, background: bg, opacity: op }}
          />
        )
      })}
    </div>
  )

  const nav = (
    <div className="flex items-center justify-between gap-3">
      <Button variant="outline" size="sm" className="gap-1" onClick={() => setIdx((i) => clamp(i - 1))} disabled={idx === 0}>
        <ChevronLeft className="size-4" /> Prev
      </Button>
      {dots}
      <Button variant="outline" size="sm" className="gap-1" onClick={() => setIdx((i) => clamp(i + 1))} disabled={idx >= total}>
        Next <ChevronRight className="size-4" />
      </Button>
    </div>
  )

  if (onResults) {
    const pct = Math.round((score / total) * 100)
    const grade =
      pct >= 90 ? 'Outstanding!' : pct >= 75 ? 'Great job!' : pct >= 50 ? 'Good effort!' : 'Keep practicing!'
    // Circular score ring: an SVG donut whose accent arc sweeps to `pct`.
    const radius = 54
    const circ = 2 * Math.PI * radius
    return (
      <div className="flex flex-col gap-4">
        {title && <h3 className="text-lg font-semibold tracking-tight" style={{ color: theme.title }}>{title}</h3>}
        {progress}
        <div
          className="flex flex-col items-center gap-3 rounded-2xl border p-8 text-center"
          style={{ background: theme.stage, borderColor: theme.panelBorder, color: theme.fg }}
        >
          <Trophy className="size-7" style={{ color: theme.accent }} />
          <div className="text-xl font-bold tracking-tight" style={{ color: theme.title }}>{grade}</div>
          <div className="relative" style={{ width: 128, height: 128 }}>
            <svg width="128" height="128" className="-rotate-90">
              <circle cx="64" cy="64" r={radius} fill="none" stroke={theme.panel} strokeWidth="10" />
              <circle
                cx="64"
                cy="64"
                r={radius}
                fill="none"
                stroke={theme.accent}
                strokeWidth="10"
                strokeLinecap="round"
                strokeDasharray={circ}
                strokeDashoffset={circ - circ * (pct / 100)}
                style={{ transition: 'stroke-dashoffset 0.8s ease' }}
              />
            </svg>
            <div className="absolute inset-0 flex flex-col items-center justify-center">
              <div className="text-3xl font-extrabold leading-none" style={{ color: theme.accent }}>{pct}%</div>
              <div className="mt-1 text-xs font-medium" style={{ color: theme.muted }}>{score} / {total} correct</div>
            </div>
          </div>
          <Button
            variant="outline"
            size="sm"
            className="mt-1 gap-1"
            onClick={() => {
              setPicked({})
              setIdx(0)
            }}
          >
            <RotateCcw className="size-3.5" /> Retake quiz
          </Button>
        </div>
        {nav}
      </div>
    )
  }

  const q = questions[idx] || {}
  const options = asArr(q.options)
  const correct = Number(q.answer)
  const chosen = picked[idx]
  const answered = chosen != null
  const isRight = answered && chosen === correct
  const explanation = asStr(q.explanation)
  // Display options in a deterministic, per-question shuffled order so the correct
  // answer isn't always in the same slot (composer models tend to park it at a
  // fixed index, e.g. always the 2nd option). Seeded by the question text → stable
  // across re-renders and navigation (never reshuffles under the user) yet varied
  // question-to-question. `picked` and scoring keep the ORIGINAL option index, so
  // grading is unaffected.
  const order = shuffleIndices(options.length, hashStr(asStr(q.question)))

  return (
    <div className="flex flex-col gap-4">
      {title && <h3 className="text-lg font-semibold tracking-tight" style={{ color: theme.title }}>{title}</h3>}
      {progress}
      <div className="rounded-2xl border p-6" style={{ background: theme.stage, borderColor: theme.panelBorder, color: theme.fg }}>
        <div className="flex items-center justify-between gap-3">
          <span
            className="inline-flex items-center rounded-full px-3 py-1 text-xs font-bold uppercase tracking-wide"
            style={{ background: theme.panel, color: theme.accent }}
          >
            Question {idx + 1} of {total}
          </span>
          {answered && (
            <span className="inline-flex items-center gap-1 text-xs font-bold" style={{ color: isRight ? OK : BAD }}>
              {isRight ? <Check className="size-3.5" /> : <X className="size-3.5" />}
              {isRight ? 'Correct' : 'Incorrect'}
            </span>
          )}
        </div>
        <p className="mt-3 text-base font-semibold leading-snug" style={{ color: theme.fg }}>{asStr(q.question)}</p>
        <div className="mt-4 flex flex-col gap-2.5">
          {order.map((oi, pos) => {
            const opt = options[oi]
            const isCorrect = oi === correct
            const isChosen = oi === chosen
            const showCorrect = answered && isCorrect
            const showWrong = answered && isChosen && !isCorrect
            const dim = answered && !isCorrect && !isChosen
            const letter = String.fromCharCode(65 + pos)
            // Themed until answered; on reveal, semantic green/red overrides the
            // theme so right/wrong reads clearly on any palette.
            const cardStyle = showCorrect
              ? { borderColor: OK, background: 'rgba(16,185,129,0.12)', color: theme.fg }
              : showWrong
                ? { borderColor: BAD, background: 'rgba(239,68,68,0.12)', color: theme.fg }
                : { borderColor: theme.panelBorder, color: theme.fg, opacity: dim ? 0.55 : 1 }
            // The A/B/C/D badge turns into a green check / red cross on reveal.
            const badgeStyle = showCorrect
              ? { background: OK, color: '#fff', borderColor: OK }
              : showWrong
                ? { background: BAD, color: '#fff', borderColor: BAD }
                : { background: theme.panel, color: theme.fg, borderColor: theme.panelBorder }
            return (
              <button
                key={oi}
                disabled={answered}
                onClick={() => setPicked((p) => ({ ...p, [idx]: oi }))}
                className={cn(
                  'flex items-center gap-3 rounded-xl border px-4 py-3.5 text-left text-sm font-medium transition-all',
                  !answered && 'hover:-translate-y-0.5 hover:shadow-md',
                )}
                style={cardStyle}
              >
                <span
                  className="flex size-7 shrink-0 items-center justify-center rounded-full border text-xs font-bold"
                  style={badgeStyle}
                >
                  {showCorrect ? <Check className="size-4" /> : showWrong ? <X className="size-4" /> : letter}
                </span>
                <span className="flex-1">{asStr(opt)}</span>
              </button>
            )
          })}
        </div>
        {answered && explanation && (
          <div
            className="mt-4 flex items-start gap-2 rounded-lg px-3 py-2.5 text-sm"
            style={{ background: theme.panel, color: theme.muted }}
          >
            <Lightbulb className="mt-0.5 size-4 shrink-0" style={{ color: theme.accent }} />
            <span>{explanation}</span>
          </div>
        )}
      </div>
      {nav}
    </div>
  )
}

// ---- Flashcards (Anki-style study deck) ------------------------------------
// Flippable cards (click to reveal the back), Prev/Next deck navigation, a
// deterministic shuffle, and a "known" tally — themed like the Quiz/SlideDeck.
type FlashCard = {
  front?: unknown
  back?: unknown
  hint?: unknown
}

export function Flashcards({ node, resolve }: NodeProps) {
  const title = asStr(resolve(node.title))
  const cards = asArr(resolve(node.cards)) as FlashCard[]
  const total = cards.length
  // Hooks run before the empty-guard so hook order is stable.
  const [idx, setIdx] = useState(0)
  const [flipped, setFlipped] = useState(false)
  const [known, setKnown] = useState<Record<number, boolean>>({})
  // null = natural order; a number seeds a deterministic shuffle (no Math.random,
  // so the order is stable across re-renders until the user reshuffles).
  const [shuffleSeed, setShuffleSeed] = useState<number | null>(null)
  const theme = useContext(DeckThemeContext)
  if (!total) return null

  const OK = '#10b981'
  const order = shuffleSeed == null ? cards.map((_, i) => i) : shuffleIndices(total, shuffleSeed)
  const clamp = (n: number) => Math.max(0, Math.min(total - 1, n))
  const cur = clamp(idx)
  const realIdx = order[cur]
  const card = cards[realIdx] || {}
  const hint = asStr(card.hint)
  const knownCount = Object.values(known).filter(Boolean).length
  const go = (n: number) => {
    setIdx(clamp(n))
    setFlipped(false)
  }

  return (
    <div className="flex flex-col gap-4">
      {title && <h3 className="text-lg font-semibold tracking-tight" style={{ color: theme.title }}>{title}</h3>}
      <div className="flex items-center justify-between gap-3">
        <span
          className="inline-flex items-center rounded-full px-3 py-1 text-xs font-bold uppercase tracking-wide"
          style={{ background: theme.panel, color: theme.accent }}
        >
          Card {cur + 1} of {total}
        </span>
        <div className="flex items-center gap-4 text-xs" style={{ color: theme.muted }}>
          <span className="inline-flex items-center gap-1"><Check className="size-3.5" style={{ color: OK }} /> {knownCount} known</span>
          <button
            type="button"
            onClick={() => {
              setShuffleSeed((s) => (s == null ? 1 : s + 1))
              setIdx(0)
              setFlipped(false)
            }}
            className="inline-flex items-center gap-1 font-semibold transition-opacity hover:opacity-80"
            style={{ color: theme.accent }}
          >
            <Shuffle className="size-3.5" /> Shuffle
          </button>
        </div>
      </div>

      {/* Flip card — click to toggle front/back. 3D transforms are INLINE so they
          work regardless of the host's Tailwind build (the exported app too). */}
      <div style={{ perspective: '1400px' }}>
        <button
          type="button"
          onClick={() => setFlipped((f) => !f)}
          className="relative w-full"
          style={{ height: 280 }}
          aria-label="Flip card"
        >
          <div
            className="absolute inset-0"
            style={{
              transformStyle: 'preserve-3d',
              transition: 'transform 0.5s',
              transform: flipped ? 'rotateY(180deg)' : 'rotateY(0deg)',
            }}
          >
            {/* Front */}
            <div
              className="absolute inset-0 flex flex-col items-center justify-center gap-3 rounded-2xl border p-8 text-center"
              style={{ background: theme.stage, borderColor: theme.panelBorder, color: theme.fg, backfaceVisibility: 'hidden', WebkitBackfaceVisibility: 'hidden' }}
            >
              <span className="text-xs font-semibold uppercase tracking-wide" style={{ color: theme.muted }}>Question</span>
              <p className="text-xl font-semibold leading-snug" style={{ color: theme.fg }}>{asStr(card.front)}</p>
              {hint && <p className="text-sm" style={{ color: theme.muted }}>Hint: {hint}</p>}
              <span className="mt-2 inline-flex items-center gap-1 text-xs" style={{ color: theme.muted }}><RotateCw className="size-3.5" /> Click to flip</span>
            </div>
            {/* Back. OPAQUE fill (solid stage color + panel tint) so the answer
                text contrasts — `theme.panel` alone is semi-transparent on the
                built-in themes and composites to ~white over a light chat page,
                hiding the light foreground text. */}
            <div
              className="absolute inset-0 flex flex-col items-center justify-center gap-3 rounded-2xl border p-8 text-center"
              style={{ backgroundColor: theme.bg, backgroundImage: `linear-gradient(0deg, ${theme.panel}, ${theme.panel})`, borderColor: theme.accent, color: theme.fg, backfaceVisibility: 'hidden', WebkitBackfaceVisibility: 'hidden', transform: 'rotateY(180deg)' }}
            >
              <span className="text-xs font-semibold uppercase tracking-wide" style={{ color: theme.accent }}>Answer</span>
              <p className="text-lg leading-snug" style={{ color: theme.fg }}>{asStr(card.back)}</p>
            </div>
          </div>
        </button>
      </div>

      {/* Self-grade — marks the card and advances. */}
      <div className="flex items-center justify-center gap-2">
        <button
          type="button"
          onClick={() => { setKnown((k) => ({ ...k, [realIdx]: false })); go(cur + 1) }}
          className="rounded-xl border px-3 py-1.5 text-sm font-medium transition-opacity hover:opacity-80"
          style={{ borderColor: theme.panelBorder, color: theme.fg }}
        >
          Still learning
        </button>
        <button
          type="button"
          onClick={() => { setKnown((k) => ({ ...k, [realIdx]: true })); go(cur + 1) }}
          className="inline-flex items-center gap-1.5 rounded-xl px-3 py-1.5 text-sm font-semibold text-white transition-transform hover:scale-[1.02]"
          style={{ background: OK }}
        >
          <Check className="size-4" /> Got it
        </button>
      </div>

      {/* Nav: Prev / status dots (green = marked known) / Next */}
      <div className="flex items-center justify-between gap-3">
        <Button variant="outline" size="sm" className="gap-1" onClick={() => go(cur - 1)} disabled={cur === 0}>
          <ChevronLeft className="size-4" /> Prev
        </Button>
        <div className="flex flex-wrap items-center justify-center gap-1.5">
          {order.map((ri, i) => (
            <button
              key={i}
              aria-label={`Go to card ${i + 1}`}
              onClick={() => go(i)}
              className="h-2 rounded-full transition-all"
              style={
                i === cur
                  ? { width: 22, background: theme.accent }
                  : { width: 8, background: known[ri] ? OK : theme.muted, opacity: known[ri] ? 0.95 : 0.45 }
              }
            />
          ))}
        </div>
        <Button variant="outline" size="sm" className="gap-1" onClick={() => go(cur + 1)} disabled={cur >= total - 1}>
          Next <ChevronRight className="size-4" />
        </Button>
      </div>
    </div>
  )
}
