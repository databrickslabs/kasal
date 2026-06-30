import { useEffect, useRef, useState, type CSSProperties, type ReactNode, type RefObject } from 'react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import { ArrowUp, Check, ChevronDown, Download, Maximize2, Moon, MoreVertical, PanelLeft, PanelLeftClose, Palette, Pencil, Plus, Sparkles, Square, Sun, Trash2 } from 'lucide-react'
import { sendMessage, fetchProgress, fetchA2ui, cancelTurn } from './api'
import type { Surface } from './a2ui/types'
import { A2UIRenderer } from './a2ui/A2UIRenderer'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { downloadElementPng } from '@/a2ui/lib/download'
import { mdComponents, linkifyCitations } from '@/a2ui/lib/markdown'
import {
  DECK_THEMES,
  DECK_THEME_KEY,
  DEFAULT_DECK_THEME_ID,
  DeckThemeContext,
  themeToDeck,
  themeToTokens,
} from '@/a2ui/lib/deckThemes'
// Aliased: `Palette` is also the lucide-react icon imported above.
import type { DeckTheme, Palette as ThemePalette } from '@/a2ui/lib/deckThemes'
import { cn } from '@/lib/utils'

interface Msg {
  role: 'user' | 'assistant'
  text: string
  a2ui?: Surface
  // Stable id so the out-of-band A2UI poll can patch THIS message when its surface
  // arrives, even if the user has sent more messages meanwhile.
  id?: string
}

interface Session {
  id: string
  title: string
  messages: Msg[]
  createdAt: number
}

// Surface kinds that render as a rich A2UI surface in addition to the text bubble.
// Keep in sync with the shared renderer's surface kinds (src/shared/a2ui — live
// chat's A2uiSurface). A kind the composer can emit but that's missing here is
// silently dropped at render time, so new kinds (flashcards, map, …) must be added.
const RICH = new Set([
  'document',
  'presentation',
  'dashboard',
  'mindmap',
  'quiz',
  'flashcards',
  'map',
])
const STORAGE_KEY = 'kasal.sessions.v1'
// The quiz keeps its own theme choice (independent of the deck's), using the same
// palette set as presentations.
const QUIZ_THEME_KEY = 'kasal.quizTheme'

// Answer depth: Chat = a single fast agent (no crew); Research = the full crew
// with fast tools; Deep Research = the full crew with deep tools + reasoning.
const MODES: { id: string; label: string; hint: string }[] = [
  { id: 'chat', label: 'Chat', hint: 'Quick answer from a single agent' },
  { id: 'research', label: 'Research', hint: 'Full crew with reasoning' },
  { id: 'deep', label: 'Deep Research', hint: 'Deep tools with planning & reasoning' },
]

const newId = () =>
  (crypto as any).randomUUID?.() ?? `c-${Date.now()}-${Math.random().toString(16).slice(2)}`

// Crew-relevant starter prompts, generated at export time from this crew's tasks
// (empty array => no suggestion row is shown). See databricks_app_exporter._starter_prompts.
const STARTER_PROMPTS: string[] = {{STARTER_PROMPTS_JSON}}

// Workspace deck/quiz theme palettes from the UIConfigurator (deliverable ->
// palette), baked at export time. "{}" when the workspace is unconfigured or
// Predefined UI is disabled → built-in themes only (today's behavior). The cast
// is robust to the stored Theme type carrying keys beyond the renderer Palette.
const WORKSPACE_THEMES = {{WORKSPACE_THEMES_JSON}} as Record<string, ThemePalette>

// The theme list + default id for a deliverable. When the workspace has branded
// that deliverable, its palette leads the picker (labelled "Workspace") and is the
// default; otherwise fall back to the built-in DECK_THEMES (midnight default). So
// a deployed app's decks/quizzes match this workspace's live chat out of the box,
// while users can still switch among the built-ins.
function themesFor(deliverable: string): { themes: DeckTheme[]; defaultId: string } {
  const ws = WORKSPACE_THEMES[deliverable]
  if (!ws) return { themes: DECK_THEMES, defaultId: DEFAULT_DECK_THEME_ID }
  const wsTheme: DeckTheme = {
    ...themeToDeck(ws),
    id: `workspace-${deliverable}`,
    name: 'Workspace',
  }
  return { themes: [wsTheme, ...DECK_THEMES], defaultId: wsTheme.id }
}

const resolveTheme = (themes: DeckTheme[], id: string): DeckTheme =>
  themes.find((t) => t.id === id) ?? themes[0]

// A2UI surfaceKind → UIConfigurator deliverable key, for resolving the workspace
// branding palette (mirrors live chat's A2uiSurface). 'document' maps to the
// closest configurable type ('report'); unknowns fall back to 'default'.
const SURFACE_TO_DELIVERABLE: Record<string, string> = {
  presentation: 'presentation',
  dashboard: 'dashboard',
  mindmap: 'mindmap',
  quiz: 'quiz',
  flashcards: 'flashcards',
  map: 'map',
  document: 'report',
}

// Built-in default palette (UIConfigurator "Default") so a surface always has a
// full --a2-* token set even when this workspace shipped no branding — surfaces
// are never unstyled.
const DEFAULT_PALETTE: ThemePalette = {
  accent: '#2272B4',
  background: '#FFFFFF',
  surface: '#F8FAFC',
  text: '#0F172A',
  heading: '#0F172A',
  muted: '#64748B',
}

// The workspace palette for a surface kind, with sensible fallbacks. Fed to
// themeToTokens so cards/tables/dashboards/decks inherit the workspace colors —
// the SAME --a2-* token mechanism the inline chat surface uses.
function paletteForKind(kind: string): ThemePalette {
  const deliverable = SURFACE_TO_DELIVERABLE[kind] ?? 'default'
  return WORKSPACE_THEMES[deliverable] ?? WORKSPACE_THEMES.default ?? DEFAULT_PALETTE
}

function loadSessions(): Session[] {
  try {
    const raw = localStorage.getItem(STORAGE_KEY)
    if (raw) {
      const parsed = JSON.parse(raw) as Session[]
      if (Array.isArray(parsed) && parsed.length) return parsed
    }
  } catch {
    /* ignore corrupt storage */
  }
  return [{ id: newId(), title: 'New chat', messages: [], createdAt: Date.now() }]
}

const Prose = ({ text }: { text: string }) => (
  <div className="prose prose-sm prose-neutral max-w-none dark:prose-invert prose-pre:bg-muted prose-pre:text-foreground prose-code:before:content-none prose-code:after:content-none">
    <ReactMarkdown remarkPlugins={[remarkGfm]} components={mdComponents}>
      {linkifyCitations(text)}
    </ReactMarkdown>
  </div>
)

// The shared frame every rich surface renders in. It injects the workspace
// branding palette as the `--a2-*` tokens the shadcn primitives consume (the
// SAME mechanism Kasal chat's A2uiSurface uses, so cards/tables/dashboards/decks
// match this workspace's colors) and adds a native "Full screen" control. The
// `.kasal-a2ui` class wires the fullscreen page background/padding (index.css).
// `controls` receives the content ref so per-surface buttons (e.g. PNG) can
// snapshot just the content, not the toolbar.
function SurfaceShell({
  kind,
  controls,
  children,
}: {
  kind: string
  controls?: (contentRef: RefObject<HTMLDivElement | null>) => ReactNode
  children: ReactNode
}) {
  const frameRef = useRef<HTMLDivElement>(null)
  const contentRef = useRef<HTMLDivElement>(null)
  const tokenStyle = themeToTokens(paletteForKind(kind)) as CSSProperties
  const toggleFullscreen = () => {
    const el = frameRef.current
    if (!el) return
    if (document.fullscreenElement === el) void document.exitFullscreen?.()
    else void el.requestFullscreen?.().catch(() => {})
  }
  return (
    <div ref={frameRef} className="kasal-a2ui mt-3" style={tokenStyle}>
      <Card className="overflow-hidden">
        <div className="flex items-center justify-end gap-1 border-b bg-muted/40 px-3 py-1.5">
          {controls?.(contentRef)}
          <Button
            variant="ghost"
            size="sm"
            className="h-7 gap-1 px-2 text-xs"
            onClick={toggleFullscreen}
            aria-label="Full screen"
            title="Full screen"
          >
            <Maximize2 className="size-3.5" />
          </Button>
        </div>
        <div ref={contentRef} className="bg-card p-5">
          {children}
        </div>
      </Card>
    </div>
  )
}

// The deck/quiz theme picker (one palette set, shared by presentations + quizzes).
function ThemePicker({
  themes,
  themeId,
  setThemeId,
  activeName,
}: {
  themes: DeckTheme[]
  themeId: string
  setThemeId: (id: string) => void
  activeName: string
}) {
  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="ghost" size="sm" className="h-7 gap-1 px-2 text-xs">
          <Palette className="size-3.5" /> Theme: {activeName}
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end">
        {themes.map((t) => (
          <DropdownMenuItem key={t.id} onSelect={() => setThemeId(t.id)} className="gap-2">
            <span className="size-3 rounded-full" style={{ background: t.accent }} />
            {t.name}
            {t.id === themeId && <Check className="ml-auto size-3.5" />}
          </DropdownMenuItem>
        ))}
      </DropdownMenuContent>
    </DropdownMenu>
  )
}

// A presentation surface: ONE theme across all slides (default Midnight),
// switchable via the Theme picker; the choice persists and drives the deck's
// PowerPoint export (the download button lives inside the shared SlideDeck, so it
// behaves identically here and in Kasal chat — one implementation).
function PresentationSurface({ surface }: { surface: Surface }) {
  const { themes, defaultId } = themesFor('presentation')
  const [themeId, setThemeId] = useState(
    () => localStorage.getItem(DECK_THEME_KEY) || defaultId,
  )
  useEffect(() => {
    localStorage.setItem(DECK_THEME_KEY, themeId)
  }, [themeId])
  const theme = resolveTheme(themes, themeId)
  return (
    <SurfaceShell
      kind="presentation"
      controls={() => (
        <ThemePicker themes={themes} themeId={themeId} setThemeId={setThemeId} activeName={theme.name} />
      )}
    >
      <DeckThemeContext.Provider value={theme}>
        <A2UIRenderer payload={surface} />
      </DeckThemeContext.Provider>
    </SurfaceShell>
  )
}

// A quiz surface: a theme picker (same palette as decks) wrapping the interactive
// quiz; the choice persists. Mirrors PresentationSurface, minus the PPTX export.
function QuizSurface({ surface }: { surface: Surface }) {
  const { themes, defaultId } = themesFor('quiz')
  const [themeId, setThemeId] = useState(
    () => localStorage.getItem(QUIZ_THEME_KEY) || defaultId,
  )
  useEffect(() => {
    localStorage.setItem(QUIZ_THEME_KEY, themeId)
  }, [themeId])
  const theme = resolveTheme(themes, themeId)
  return (
    <SurfaceShell
      kind="quiz"
      controls={() => (
        <ThemePicker themes={themes} themeId={themeId} setThemeId={setThemeId} activeName={theme.name} />
      )}
    >
      <DeckThemeContext.Provider value={theme}>
        <A2UIRenderer payload={surface} />
      </DeckThemeContext.Provider>
    </SurfaceShell>
  )
}

// A rich A2UI surface. Presentations/quizzes get a themed deck picker; visual
// surfaces (dashboard, mindmap, map) get a PNG snapshot. All render in the shared
// SurfaceShell, so every kind inherits the workspace palette + full-screen control
// and looks the same as Kasal chat. (Tables carry their own CSV button.)
function RichSurface({ surface }: { surface: Surface }) {
  const kind = surface.surfaceKind
  if (kind === 'presentation') return <PresentationSurface surface={surface} />
  if (kind === 'quiz') return <QuizSurface surface={surface} />
  const png = kind === 'dashboard' || kind === 'mindmap' || kind === 'map'
  return (
    <SurfaceShell
      kind={kind}
      controls={
        png
          ? (contentRef) => (
              <Button
                variant="ghost"
                size="sm"
                className="h-7 gap-1 px-2 text-xs"
                onClick={() => contentRef.current && downloadElementPng(contentRef.current, `${kind}.png`)}
              >
                <Download className="size-3.5" /> PNG
              </Button>
            )
          : undefined
      }
    >
      <A2UIRenderer payload={surface} />
    </SurfaceShell>
  )
}

export default function App() {
  const [sessions, setSessions] = useState<Session[]>(loadSessions)
  const [activeId, setActiveId] = useState<string>(() => sessions[0].id)
  const [input, setInput] = useState('')
  // In-flight requests are tracked per session id (not a single global flag) so a
  // turn still running in one chat never shows its "working…"/progress in another.
  const [pending, setPending] = useState<Set<string>>(() => new Set())
  const [error, setError] = useState<string | null>(null)
  const [sidebarOpen, setSidebarOpen] = useState<boolean>(
    () => localStorage.getItem('kasal.sidebar') !== 'closed',
  )
  const [renamingId, setRenamingId] = useState<string | null>(null)
  const [renameValue, setRenameValue] = useState('')
  const [dark, setDark] = useState<boolean>(() => localStorage.getItem('kasal.theme') === 'dark')
  const [mode, setMode] = useState<string>(() => localStorage.getItem('kasal.mode') || 'research')
  const [userName, setUserName] = useState<string>('')
  // Ephemeral live status ("Using tool: …") while a turn runs; never persisted.
  const [progress, setProgress] = useState<string | null>(null)
  const renameRef = useRef<HTMLInputElement>(null)
  const threadRef = useRef<HTMLDivElement>(null)
  // Current session id, readable from async callbacks without stale closures.
  const activeIdRef = useRef(activeId)
  // One AbortController per in-flight session, so Stop can abort the right turn.
  const ctrls = useRef<Map<string, AbortController>>(new Map())
  // One AbortController per session's out-of-band A2UI poll, so a new turn (or
  // Stop) cancels the prior turn's poll — the surface always lands on its message.
  const a2uiCtrls = useRef<Map<string, AbortController>>(new Map())
  // Whether the thread is scrolled to (near) the bottom. We only auto-scroll
  // when it is, so we never yank the user back down while they read history.
  const atBottomRef = useRef(true)

  const active = sessions.find((s) => s.id === activeId) ?? sessions[0]
  // True only when the *currently viewed* session has a turn in flight.
  const activeBusy = pending.has(activeId)
  const setSessionPending = (id: string, on: boolean) =>
    setPending((prev) => {
      const next = new Set(prev)
      if (on) next.add(id)
      else next.delete(id)
      return next
    })

  // Persist all sessions so reopening / refreshing keeps every conversation.
  useEffect(() => {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(sessions))
    } catch {
      /* ignore quota errors */
    }
  }, [sessions])

  useEffect(() => {
    localStorage.setItem('kasal.sidebar', sidebarOpen ? 'open' : 'closed')
  }, [sidebarOpen])

  useEffect(() => {
    localStorage.setItem('kasal.mode', mode)
  }, [mode])

  // Dark mode: toggle the `dark` class on <html> (drives the CSS-var theme) and persist.
  useEffect(() => {
    document.documentElement.classList.toggle('dark', dark)
    localStorage.setItem('kasal.theme', dark ? 'dark' : 'light')
  }, [dark])

  // Signed-in user: Databricks injects identity via forwarded headers, which the
  // backend surfaces at /me. Falls back to "You" locally where there's no header.
  useEffect(() => {
    let alive = true
    fetch('/me')
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => alive && setUserName(d?.name || ''))
      .catch(() => {})
    return () => {
      alive = false
    }
  }, [])

  // While a turn is in flight, poll the agent's ephemeral progress so the user
  // sees what's happening (current task / tool). Stops + resets when idle.
  useEffect(() => {
    if (!activeBusy) {
      setProgress(null)
      return
    }
    let alive = true
    const ctrl = new AbortController()
    const tick = async () => {
      const s = await fetchProgress(activeId, ctrl.signal)
      if (alive) setProgress(s)
    }
    tick()
    const id = setInterval(tick, 700)
    return () => {
      alive = false
      ctrl.abort()
      clearInterval(id)
    }
  }, [activeBusy, activeId])

  // Switching sessions must not carry transient UI across screens: clear any
  // error banner (progress is already reset by the poller above when idle).
  useEffect(() => {
    activeIdRef.current = activeId
    setError(null)
  }, [activeId])

  function onThreadScroll() {
    const el = threadRef.current
    if (!el) return
    atBottomRef.current = el.scrollHeight - el.scrollTop - el.clientHeight < 80
  }

  useEffect(() => {
    if (atBottomRef.current) {
      threadRef.current?.scrollTo({ top: threadRef.current.scrollHeight, behavior: 'smooth' })
    }
  }, [active?.messages, activeBusy])

  function patchActive(updater: (s: Session) => Session) {
    setSessions((prev) => prev.map((s) => (s.id === activeId ? updater(s) : s)))
  }

  // Poll for a turn's out-of-band A2UI surface and attach it to its message.
  // Bounded so a never-arriving surface can't poll forever; stops on ready/none.
  async function pollA2ui(convId: string, msgId: string, signal: AbortSignal) {
    const deadline = Date.now() + 60_000
    while (Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 1200))
      if (signal.aborted) return
      const { status, surface } = await fetchA2ui(convId, signal)
      if (status === 'ready' && surface) {
        setSessions((prev) =>
          prev.map((s) =>
            s.id === convId
              ? {
                  ...s,
                  messages: s.messages.map((m) =>
                    m.id === msgId ? { ...m, a2ui: surface } : m,
                  ),
                }
              : s,
          ),
        )
        return
      }
      if (status === 'none') return // composed, but no rich surface for this turn
    }
  }

  async function submit(text: string) {
    const q = text.trim()
    if (!q || pending.has(activeId)) return
    setError(null)
    setInput('')
    atBottomRef.current = true // the user just sent — follow the new messages
    const convId = activeId
    patchActive((s) => ({
      ...s,
      title: s.messages.length === 0 ? q.slice(0, 48) : s.title,
      messages: [...s.messages, { role: 'user', text: q }],
    }))
    const ctrl = new AbortController()
    ctrls.current.set(convId, ctrl)
    setSessionPending(convId, true)
    try {
      const reply = await sendMessage(q, convId, ctrl.signal, mode)
      const msgId = newId()
      setSessions((prev) =>
        prev.map((s) =>
          s.id === convId
            ? {
                ...s,
                messages: [
                  ...s.messages,
                  { role: 'assistant', text: reply.text, a2ui: reply.a2ui, id: msgId },
                ],
              }
            : s,
        ),
      )
      // The rich surface is composed out-of-band so this answer returns fast; poll
      // for it and attach it to the message when ready. (If an older server already
      // inlined it via reply.a2ui, skip the poll.) A fresh controller, replacing
      // any prior poll for this session, keeps the surface bound to its message.
      if (!reply.a2ui) {
        a2uiCtrls.current.get(convId)?.abort()
        const pollCtrl = new AbortController()
        a2uiCtrls.current.set(convId, pollCtrl)
        void pollA2ui(convId, msgId, pollCtrl.signal)
      }
    } catch (e: any) {
      // A user-initiated Stop aborts the fetch — that's not an error to show.
      if (e?.name !== 'AbortError' && activeIdRef.current === convId) {
        setError(e?.message ?? String(e))
      }
    } finally {
      ctrls.current.delete(convId)
      setSessionPending(convId, false)
    }
  }

  // Stop the active session's turn: abort the fetch (frees the UI immediately)
  // and tell the backend to cancel the crew so it stops spending tokens.
  function stop() {
    const id = activeId
    ctrls.current.get(id)?.abort()
    a2uiCtrls.current.get(id)?.abort() // also stop polling for this turn's surface
    cancelTurn(id)
  }

  // New chat: keep ALL existing sessions, just add and switch to a fresh one.
  function newChat() {
    const s: Session = { id: newId(), title: 'New chat', messages: [], createdAt: Date.now() }
    setSessions((prev) => [s, ...prev])
    setActiveId(s.id)
    setError(null)
    setInput('')
  }

  function removeSession(id: string) {
    setSessions((prev) => {
      const next = prev.filter((s) => s.id !== id)
      if (next.length === 0) {
        const fresh = { id: newId(), title: 'New chat', messages: [], createdAt: Date.now() }
        setActiveId(fresh.id)
        return [fresh]
      }
      if (id === activeId) setActiveId(next[0].id)
      return next
    })
  }

  function startRename(s: Session) {
    setRenameValue(s.title || '')
    setRenamingId(s.id)
  }

  function commitRename() {
    const id = renamingId
    if (!id) return
    const title = renameValue.trim()
    setSessions((prev) =>
      prev.map((s) => (s.id === id ? { ...s, title: title || s.title || 'New chat' } : s)),
    )
    setRenamingId(null)
  }

  // Keep the rename input focused even after the dropdown closes.
  useEffect(() => {
    if (renamingId) {
      const el = renameRef.current
      el?.focus()
      el?.select()
    }
  }, [renamingId])

  const empty = active.messages.length === 0

  // The composer is shared between the centered empty state and the pinned
  // bottom bar, so it stays identical in both places.
  const composerForm = (
    <form
      onSubmit={(e) => {
        e.preventDefault()
        submit(input)
      }}
      className="mx-auto w-full max-w-3xl rounded-3xl border border-input bg-background shadow-sm transition-colors focus-within:border-muted-foreground/40"
    >
      {/* Top row — sparkle icon + textarea, matching Kasal's chat-mode input. */}
      <div className="flex items-start gap-3 px-5 pt-4 pb-1">
        <Sparkles className="mt-1 size-5 shrink-0 text-muted-foreground" strokeWidth={1.5} />
        <textarea
          className="max-h-40 min-h-[28px] flex-1 resize-none bg-transparent py-1 text-[15px] leading-relaxed outline-none placeholder:text-muted-foreground"
          value={input}
          placeholder="Ask a question…"
          rows={2}
          onChange={(e) => setInput(e.target.value)}
          onInput={(e) => {
            const el = e.currentTarget
            el.style.height = 'auto'
            el.style.height = Math.min(el.scrollHeight, 160) + 'px'
          }}
          onKeyDown={(e) => {
            if (e.key === 'Enter' && !e.shiftKey) {
              e.preventDefault()
              submit(input)
            }
          }}
        />
      </div>
      {/* Bottom row — answer-mode pill + send, right-aligned like Kasal.
          Enter submits; the send button mirrors Kasal's up-arrow, and becomes a
          Stop (square) button while a turn is generating. */}
      <div className="flex items-center justify-end gap-2 px-4 pb-2.5 pt-1">
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button
              type="button"
              variant="ghost"
              size="sm"
              className="h-8 shrink-0 gap-1 rounded-lg border bg-transparent px-2.5 text-xs font-medium text-muted-foreground hover:bg-accent hover:text-foreground"
              aria-label="Answer mode"
            >
              {MODES.find((m) => m.id === mode)?.label ?? 'Research'}
              <ChevronDown className="size-3.5 opacity-70" />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end" side="top" className="w-64">
            <DropdownMenuLabel className="px-2.5 py-1.5 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
              Answer mode
            </DropdownMenuLabel>
            {MODES.map((m) => (
              <DropdownMenuItem key={m.id} onSelect={() => setMode(m.id)} className="items-start gap-2">
                <Check
                  className={cn('mt-0.5 size-4 shrink-0', mode === m.id ? 'opacity-100' : 'opacity-0')}
                />
                <div className="flex flex-col">
                  <span className="font-medium">{m.label}</span>
                  <span className="text-xs text-muted-foreground">{m.hint}</span>
                </div>
              </DropdownMenuItem>
            ))}
          </DropdownMenuContent>
        </DropdownMenu>
        {activeBusy ? (
          <Button
            type="button"
            variant="secondary"
            size="icon"
            className="size-8 shrink-0 rounded-xl"
            onClick={stop}
            aria-label="Stop generating"
            title="Stop"
          >
            <Square className="size-4 fill-current" />
          </Button>
        ) : (
          <Button
            type="submit"
            variant="secondary"
            size="icon"
            disabled={!input.trim()}
            className="size-8 shrink-0 rounded-xl"
            aria-label="Send message"
            title="Send"
          >
            <ArrowUp className="size-4" />
          </Button>
        )}
      </div>
    </form>
  )

  // Crew-relevant example prompts, shown under the composer on the empty screen.
  const starterPrompts = STARTER_PROMPTS.length > 0 && (
    <div className="mx-auto mt-4 flex max-w-3xl flex-wrap justify-center gap-2.5">
      {STARTER_PROMPTS.map((s) => (
        <Button
          key={s}
          variant="outline"
          className="h-auto rounded-full px-4 py-2 text-left font-normal"
          onClick={() => submit(s)}
        >
          {s}
        </Button>
      ))}
    </div>
  )

  return (
    <div className={cn('grid h-screen', sidebarOpen ? 'grid-cols-[264px_1fr]' : 'grid-cols-1')}>
      {/* Sidebar */}
      {sidebarOpen && (
      <aside className="flex min-h-0 flex-col gap-3 border-r bg-secondary/40 p-4">
        <div className="flex items-center justify-between gap-2">
          <div className="px-1 py-1 text-[17px] font-bold tracking-tight">{'{{DISPLAY_NAME}}'}</div>
          <Button
            variant="ghost"
            size="icon"
            className="size-8 shrink-0 text-muted-foreground hover:text-foreground"
            onClick={() => setSidebarOpen(false)}
            aria-label="Collapse sidebar"
            title="Collapse sidebar"
          >
            <PanelLeftClose className="size-4" />
          </Button>
        </div>
        <Button onClick={newChat} variant="secondary" className="w-full justify-center">
          <Plus className="size-4" /> New chat
        </Button>
        {sessions.length > 0 && (
          <div className="px-1 pt-1 text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">
            Recent
          </div>
        )}
        <nav className="-mr-1 flex flex-1 flex-col gap-0.5 overflow-y-auto pr-1">
          {sessions.map((s) => (
            <div
              key={s.id}
              onClick={() => renamingId !== s.id && setActiveId(s.id)}
              title={s.title}
              className={cn(
                'group flex items-center gap-1.5 rounded-lg px-2.5 py-2 text-sm text-muted-foreground transition-colors hover:bg-accent hover:text-foreground',
                renamingId !== s.id && 'cursor-pointer',
                s.id === activeId && 'bg-accent font-medium text-foreground',
              )}
            >
              {renamingId === s.id ? (
                <input
                  ref={renameRef}
                  value={renameValue}
                  onChange={(e) => setRenameValue(e.target.value)}
                  onClick={(e) => e.stopPropagation()}
                  onBlur={commitRename}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') commitRename()
                    if (e.key === 'Escape') setRenamingId(null)
                  }}
                  className="min-w-0 flex-1 rounded border border-input bg-background px-1.5 py-0.5 text-sm text-foreground outline-none ring-1 ring-ring"
                />
              ) : (
                <>
                  {/* Session "icon" — a tiny spinner while this session has a
                      turn in flight, matching Kasal's per-session SessionSpinner. */}
                  {pending.has(s.id) && (
                    <span
                      className="size-2 shrink-0 animate-spin rounded-full border border-current border-t-transparent text-primary"
                      aria-label="Running"
                    />
                  )}
                  <span className="flex-1 truncate text-[13px]">{s.title || 'New chat'}</span>
                  <DropdownMenu>
                    <DropdownMenuTrigger asChild>
                      <button
                        onClick={(e) => e.stopPropagation()}
                        aria-label="Chat options"
                        className="shrink-0 rounded p-0.5 text-muted-foreground/60 opacity-0 transition-colors hover:bg-background hover:text-foreground focus:opacity-100 group-hover:opacity-100 data-[state=open]:opacity-100 data-[state=open]:text-foreground"
                      >
                        <MoreVertical className="size-4" />
                      </button>
                    </DropdownMenuTrigger>
                    <DropdownMenuContent align="end" onCloseAutoFocus={(e) => e.preventDefault()}>
                      <DropdownMenuItem onSelect={() => startRename(s)}>
                        <Pencil /> Rename
                      </DropdownMenuItem>
                      <DropdownMenuSeparator />
                      <DropdownMenuItem
                        className="text-destructive focus:text-destructive"
                        onSelect={() => removeSession(s.id)}
                      >
                        <Trash2 /> Delete
                      </DropdownMenuItem>
                    </DropdownMenuContent>
                  </DropdownMenu>
                </>
              )}
            </div>
          ))}
        </nav>
        <div className="mt-1 flex items-center justify-between gap-2 border-t pt-3">
          <div className="flex min-w-0 items-center gap-2">
            <span className="grid size-7 shrink-0 place-items-center rounded-full bg-muted text-xs font-semibold text-foreground">
              {(userName || 'Y').slice(0, 1).toUpperCase()}
            </span>
            <span className="truncate text-sm text-foreground">{userName || 'You'}</span>
          </div>
          <Button
            variant="ghost"
            size="icon"
            className="size-8 shrink-0 text-muted-foreground hover:text-foreground"
            onClick={() => setDark((d) => !d)}
            aria-label={dark ? 'Switch to light mode' : 'Switch to dark mode'}
            title={dark ? 'Light mode' : 'Dark mode'}
          >
            {dark ? <Sun className="size-4" /> : <Moon className="size-4" />}
          </Button>
        </div>
      </aside>
      )}

      {/* Main */}
      <main className="relative flex min-h-0 min-w-0 flex-col bg-background">
        {!sidebarOpen && (
          <Button
            variant="outline"
            size="icon"
            className="absolute left-3 top-3 z-10 size-8 bg-background text-muted-foreground hover:text-foreground"
            onClick={() => setSidebarOpen(true)}
            aria-label="Open sidebar"
            title="Open sidebar"
          >
            <PanelLeft className="size-4" />
          </Button>
        )}
        {empty ? (
          // New chat: composer centered on screen, example prompts underneath.
          <div className="flex flex-1 flex-col items-center justify-center overflow-y-auto px-6 py-7">
            <div className="w-full">
              <h1 className="mb-6 text-center text-3xl font-semibold tracking-tight">
                How can I help you today?
              </h1>
              {composerForm}
              {starterPrompts}
            </div>
          </div>
        ) : (
          <>
            <div ref={threadRef} onScroll={onThreadScroll} className="flex-1 overflow-y-auto py-7">
              {active.messages.map((m, i) =>
                m.role === 'user' ? (
                  // The user's own message: right-aligned grey bubble, no avatar.
                  <div key={i} className="mx-auto flex max-w-3xl justify-end px-6 py-2.5">
                    <div className="max-w-[80%] whitespace-pre-wrap rounded-2xl bg-muted px-4 py-2.5 text-[15px] leading-relaxed text-foreground">
                      {m.text}
                    </div>
                  </div>
                ) : (
                  // The assistant: left-aligned, full width, no avatar.
                  <div key={i} className="mx-auto max-w-3xl px-6 py-2.5">
                    <Prose text={m.text} />
                    {m.a2ui && RICH.has(m.a2ui.surfaceKind) && <RichSurface surface={m.a2ui} />}
                  </div>
                ),
              )}

              {activeBusy && (
                <div className="mx-auto max-w-3xl px-6 py-2.5">
                  <div className="flex items-center gap-1.5 text-sm text-muted-foreground">
                    <span className="a2-dot size-1.5 rounded-full bg-current" />
                    <span className="a2-dot size-1.5 rounded-full bg-current [animation-delay:0.2s]" />
                    <span className="a2-dot size-1.5 rounded-full bg-current [animation-delay:0.4s]" />
                    <span className="ml-1 transition-opacity">{progress || 'working…'}</span>
                  </div>
                </div>
              )}

              {error && (
                <div className="mx-auto my-2 max-w-3xl rounded-lg border border-destructive/30 bg-destructive/5 px-4 py-3 text-sm text-destructive">
                  {error}
                </div>
              )}
            </div>

            {/* Composer — pinned; only the thread above scrolls. */}
            <div className="shrink-0 bg-background px-6 py-4">{composerForm}</div>
          </>
        )}
      </main>
    </div>
  )
}
