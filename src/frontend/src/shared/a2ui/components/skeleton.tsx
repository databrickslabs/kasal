/**
 * The placeholder a surface shows before it has any content.
 *
 * A composed surface takes tens of seconds, and the request already tells us
 * WHAT is coming — a quiz, a card stack, a mindmap, a map. The backend ships a
 * frame for it (`stream.py::shell_from_request`) the instant the request
 * arrives, long before a model has run, and this draws that frame.
 *
 * It is never emitted by the composer: `Skeleton` is deliberately absent from
 * `catalog.json`, so the model cannot produce one. Only the instant shell does,
 * and every node it makes is replaced by real content as the surface streams in.
 *
 * Decks do NOT use this — a deck's frame is real slides carrying real titles
 * from the outline, and `Slide` draws its own pending state.
 */
import { useContext } from 'react'
import type { NodeProps } from '../types'
import { DeckThemeContext } from '../lib/deckThemes'
import { asStr } from './values'

/** Widths that read as text rather than as a progress bar. */
const LINE_WIDTHS = ['92%', '78%', '85%', '60%']

function Bar({ w, delay, height = 'h-4' }: { w: string; delay: number; height?: string }) {
  const theme = useContext(DeckThemeContext)
  return (
    <div
      className={`a2-skeleton-bar ${height} animate-pulse rounded`}
      style={{ width: w, background: theme.accent, opacity: 0.22, animationDelay: `${delay}ms` }}
    />
  )
}

/** Per-kind body: the shape the reader is waiting for, not a generic spinner. */
function Body({ variant }: { variant: string }) {
  const theme = useContext(DeckThemeContext)
  // Accent-tinted: `panelBorder` on a light workspace theme is near-white, so
  // the old panelBorder-at-0.35 shimmer was INVISIBLE on the white panel and
  // the shell read as an empty card with a lone accent bar.
  const panel = {
    background: theme.accent,
    opacity: 0.1,
  }

  if (variant === 'quiz') {
    // A question, then its options — the shape a quiz always has.
    return (
      <div className="flex flex-col gap-6">
        {[0, 1].map((q) => (
          <div key={q} className="flex flex-col gap-3">
            <Bar w="70%" delay={q * 160} height="h-5" />
            <div className="ml-1 flex flex-col gap-2">
              {[0, 1, 2, 3].map((o) => (
                <Bar key={o} w={['55%', '48%', '62%', '44%'][o]} delay={q * 160 + o * 70} height="h-3" />
              ))}
            </div>
          </div>
        ))}
      </div>
    )
  }

  if (variant === 'flashcards') {
    return (
      <div className="flex flex-wrap gap-4">
        {[0, 1, 2].map((c) => (
          <div
            key={c}
            className="a2-skeleton-card h-32 flex-1 animate-pulse rounded-xl"
            style={{ ...panel, minWidth: '9rem', animationDelay: `${c * 140}ms` }}
          />
        ))}
      </div>
    )
  }

  if (variant === 'mindmap') {
    // A centre with branches off it.
    return (
      <div className="flex items-center gap-6">
        <div
          className="a2-skeleton-node h-16 w-40 flex-shrink-0 animate-pulse rounded-xl"
          style={panel}
        />
        <div className="flex flex-1 flex-col gap-3">
          {[0, 1, 2, 3].map((b) => (
            <Bar key={b} w={['70%', '55%', '64%', '48%'][b]} delay={b * 110} height="h-6" />
          ))}
        </div>
      </div>
    )
  }

  if (variant === 'kanban') {
    // Columns of cards — the one shape a board always has.
    return (
      <div className="flex gap-4">
        {[0, 1, 2].map((col) => (
          <div key={col} className="flex flex-1 flex-col gap-2">
            <Bar w="60%" delay={col * 120} height="h-3" />
            {[0, 1, 2].slice(0, 3 - col).map((c) => (
              <div
                key={c}
                className="a2-skeleton-card h-14 animate-pulse rounded-lg"
                style={{ ...panel, animationDelay: `${col * 120 + c * 90}ms` }}
              />
            ))}
          </div>
        ))}
      </div>
    )
  }

  if (variant === 'album') {
    return (
      <div className="grid grid-cols-3 gap-3">
        {[0, 1, 2, 3, 4, 5].map((i) => (
          <div
            key={i}
            className="a2-skeleton-tile aspect-[4/3] animate-pulse rounded-lg"
            style={{ ...panel, animationDelay: `${i * 90}ms` }}
          />
        ))}
      </div>
    )
  }

  if (variant === 'graph' || variant === 'sequence' || variant === 'diagram') {
    return (
      <div
        className="a2-skeleton-canvas h-56 w-full animate-pulse rounded-xl"
        style={panel}
      />
    )
  }

  if (variant === 'dashboard' || variant === 'forecast') {
    // A headline row of tiles over a chart — the standard dashboard silhouette.
    return (
      <div className="flex flex-col gap-4">
        <div className="flex gap-3">
          {[0, 1, 2, 3].map((i) => (
            <div
              key={i}
              className="a2-skeleton-tile h-16 flex-1 animate-pulse rounded-lg"
              style={{ ...panel, animationDelay: `${i * 90}ms` }}
            />
          ))}
        </div>
        <div
          className="a2-skeleton-chart h-40 w-full animate-pulse rounded-xl"
          style={{ ...panel, animationDelay: '360ms' }}
        />
      </div>
    )
  }

  if (variant === 'map') {
    return (
      <div
        className="a2-skeleton-map h-64 w-full animate-pulse rounded-xl"
        style={panel}
      />
    )
  }

  return (
    <div className="flex flex-col gap-4">
      {LINE_WIDTHS.map((w, i) => (
        <Bar key={i} w={w} delay={i * 120} />
      ))}
    </div>
  )
}

export function Skeleton({ node, resolve }: NodeProps) {
  const theme = useContext(DeckThemeContext)
  const variant = asStr(resolve(node.variant)) || 'document'
  const title = asStr(resolve(node.title))
  return (
    <div
      // Card + title use the APP's adaptive tokens, not the deck theme: the
      // default DeckTheme is a dark-stage palette, so `theme.panel` /
      // `theme.title` render white-on-white in the chat — an invisible card
      // whose (wrapped) invisible title read as a mystery gap between the
      // accent bar and the shimmer. Only the accents keep the deck theme.
      className="a2-skeleton flex w-full flex-col gap-6 rounded-2xl border border-border bg-secondary/40 p-8"
      // aria-busy so a screen reader announces work in progress rather than
      // reading out a pile of empty boxes.
      aria-busy="true"
      aria-label={title ? `${title} — being prepared` : 'Being prepared'}
    >
      <div className="h-1.5 w-16 rounded-full" style={{ background: theme.accent }} />
      {title && (
        <h2 className="text-balance text-3xl font-bold leading-tight tracking-tight text-foreground">
          {title}
        </h2>
      )}
      <Body variant={variant} />
    </div>
  )
}
