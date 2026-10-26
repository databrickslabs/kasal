/**
 * Kanban board: columns of swimlanes (To Do / In Progress / Done) each carrying
 * card items. Pure React/Tailwind — no charting dependency.
 */
import { useContext } from 'react'
import type { NodeProps } from '../types'
import { DeckThemeContext } from '../lib/deckThemes'
import { asArr, asNum, asStr } from './values'

interface KanbanCard {
  id: string
  label: string
  detail?: string
  color?: string
  tag?: string
}

interface KanbanColumn {
  id: string
  title: string
  color?: string
  cards: KanbanCard[]
}

export function Kanban({ node, resolve }: NodeProps) {
  const theme = useContext(DeckThemeContext)
  const rawCols = asArr(resolve(node.columns))
  const defaultColor = theme.accent

  const { columns, maxCards } = (() => {
    const cols: KanbanColumn[] = rawCols
      .filter((c) => c && typeof c === 'object')
      .map((c) => c as Record<string, unknown>)
      .map((c) => ({
        id: asStr(c.id ?? c.title),
        title: asStr(c.title ?? c.id),
        color: asStr(c.color) || undefined,
        cards: (asArr(c.cards) as Record<string, unknown>[])
          .filter((k) => k && typeof k === 'object')
          .map((k) => ({
            id: asStr(k.id ?? k.label),
            label: asStr(k.label ?? k.title ?? k.id),
            detail: asStr(k.detail ?? k.description) || undefined,
            color: asStr(k.color) || undefined,
            tag: asStr(k.tag) || undefined,
          }))
          .filter((k) => k.id),
      }))
      .filter((c) => c.id)

    return { columns: cols, maxCards: Math.max(...cols.map((c) => c.cards.length), 1) }
  })()

  if (!columns.length) return null

  // Determine column colors: cycle through a small palette derived from the accent.
  const palette = [
    defaultColor,
    theme.accent,
    'hsl(200, 70%, 50%)',
    'hsl(30, 80%, 55%)',
    'hsl(150, 60%, 45%)',
    'hsl(280, 60%, 55%)',
  ]

  return (
    <div className="flex w-full min-w-0 flex-col">
      {node.title != null && <div className="mb-3 font-semibold">{asStr(node.title)}</div>}
      <div className="flex w-full gap-3 overflow-x-auto">
        {columns.map((col, ci) => {
          const colColor = col.color || palette[ci % palette.length]
          return (
            <div key={col.id} className="flex min-w-[200px] max-w-[320px] flex-1 flex-col rounded-lg border bg-muted/40">
              <div
                className="rounded-t-lg px-3 py-2 font-semibold text-white"
                style={{ backgroundColor: colColor }}
              >
                {col.title}
                <span className="ml-2 text-xs opacity-80">{col.cards.length}</span>
              </div>
              <div className="flex flex-1 flex-col gap-2 p-2">
                {col.cards.map((card, ki) => {
                  const cardColor = card.color || colColor
                  return (
                    <div
                      key={card.id}
                      className="rounded-md border p-2 shadow-sm"
                      style={{ borderLeftWidth: 3, borderLeftColor: cardColor }}
                    >
                      <div className="flex items-start justify-between gap-1">
                        <span className="font-medium text-sm">{card.label}</span>
                        {card.tag && (
                          <span
                            className="rounded px-1.5 py-0.5 text-[10px] font-semibold text-white shrink-0"
                            style={{ backgroundColor: cardColor }}
                          >
                            {card.tag}
                          </span>
                        )}
                      </div>
                      {card.detail && (
                        <p className="mt-1 text-xs text-muted-foreground leading-snug">{card.detail}</p>
                      )}
                    </div>
                  )
                })}
                {col.cards.length === 0 && (
                  <div className="flex flex-1 items-center justify-center py-6 text-xs text-muted-foreground">
                    Empty
                  </div>
                )}
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}
