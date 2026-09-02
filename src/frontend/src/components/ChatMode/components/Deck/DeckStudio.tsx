import React, {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { createPortal } from "react-dom";
import { Download, Loader2, Maximize2, Undo2, X } from "lucide-react";
import ScaledFrame from "../Chat/ScaledFrame";
import DeckPresentation from "../Chat/DeckPresentation";
import ThumbnailRail from "./ThumbnailRail";
import SlideInstructionBar from "./SlideInstructionBar";
import { DeckService } from "../../../../api/chat/DeckService";
import { useAppStore } from "../../store/appStore";
import { useSessionStore } from "../../store/sessionStore";
import { planSlideEdit, type SlideEdit } from "../../utils/slideRefine";
import {
  SLIDE_W,
  replaceDeckInContent,
  splitSlides,
  stageFor,
} from "../../utils/htmlDeck";
import { downloadDeckPdf, downloadDeckPptx } from "../../utils/deckExport";

/**
 * The deck studio: a deck opened for editing, one slide at a time.
 *
 * Thumbnails on the left (select, drag to reorder, duplicate, delete, add
 * between), the selected slide large on a dark stage, and under it the
 * instruction bar. An instruction sends ONLY that slide through the one-slide
 * generation call; the thumbnail pulses while it works, the slide swaps in
 * place, and Undo puts the previous deck back. Structural edits are instant.
 *
 * Every change is written straight back into the message the deck lives in,
 * so the chat's deck card is always current and nothing is lost if the studio
 * is simply closed.
 */

interface DeckStudioProps {
  /** The deck's HTML (without the ``` fences). */
  code: string;
  /** The chat message the deck lives in — where edits are written back. */
  messageId?: string;
  initialIndex?: number;
  onClose: () => void;
}

interface HistoryEntry {
  label: string;
  /** The deck before this edit. */
  prev: string;
}

const DeckStudio: React.FC<DeckStudioProps> = ({
  code,
  messageId,
  initialIndex = 0,
  onClose,
}) => {
  const [deck, setDeck] = useState(code);
  const slides = useMemo(() => splitSlides(deck), [deck]);
  const count = slides.length;
  const [selected, setSelected] = useState(() =>
    Math.max(0, Math.min(initialIndex, count - 1)),
  );
  const [history, setHistory] = useState<HistoryEntry[]>([]);
  const [working, setWorking] = useState<{ index: number } | null>(null);
  const [error, setError] = useState<string | null>(null);
  // The bar edits the selected slide, or writes a new one at a position.
  const [barMode, setBarMode] = useState<
    { kind: "refine" } | { kind: "add"; at: number }
  >({ kind: "refine" });
  const [present, setPresent] = useState(false);
  const [menu, setMenu] = useState(false);
  const [busy, setBusy] = useState<"" | "pdf" | "pptx">("");
  const menuRef = useRef<HTMLDivElement>(null);
  const selectedModel = useAppStore((s) => s.selectedModel);

  const shown = Math.min(selected, Math.max(0, count - 1));
  const stage = useMemo(() => stageFor(slides[shown] ?? ""), [slides, shown]);

  // Write the deck back into its message, so the chat's card is current and a
  // closed studio loses nothing.
  const writeBack = useCallback(
    (next: string) => {
      if (!messageId) return;
      const store = useSessionStore.getState();
      const msg = store.messages.find((m) => m.id === messageId);
      if (msg)
        store.updateMessage(messageId, {
          content: replaceDeckInContent(msg.content, next),
        });
    },
    [messageId],
  );
  const commit = useCallback(
    (next: string, label: string) => {
      setHistory((h) => [...h, { label, prev: deck }]);
      setDeck(next);
      writeBack(next);
    },
    [deck, writeBack],
  );
  const undo = useCallback(() => {
    setHistory((h) => {
      const last = h[h.length - 1];
      if (!last) return h;
      setDeck(last.prev);
      writeBack(last.prev);
      return h.slice(0, -1);
    });
  }, [writeBack]);

  const instant = (edit: SlideEdit) => {
    const plan = planSlideEdit(edit, deck);
    if (plan.kind !== "instant") return;
    commit(plan.deck, plan.done);
    setSelected(plan.focus);
    setError(null);
  };

  const apply = async (instruction: string) => {
    const edit: SlideEdit =
      barMode.kind === "add"
        ? { kind: "add", index: barMode.at, instruction }
        : { kind: "refine", index: shown, instruction };
    const plan = planSlideEdit(edit, deck);
    if (plan.kind !== "call") return;
    setError(null);
    setWorking({
      index: barMode.kind === "add" ? Math.min(barMode.at, count - 1) : shown,
    });
    try {
      const res = await DeckService.refineSlide({
        ...plan.request,
        model: selectedModel || null,
      });
      if (!res.section)
        throw new Error(res.error || "The model did not return a slide.");
      commit(plan.apply(res.section), plan.done);
      setSelected(plan.focus);
      setBarMode({ kind: "refine" });
    } catch (e) {
      const detail = (e as { response?: { data?: { detail?: unknown } } })
        ?.response?.data?.detail;
      setError(
        typeof detail === "string"
          ? detail
          : e instanceof Error
            ? e.message
            : "The edit failed.",
      );
    } finally {
      setWorking(null);
    }
  };

  // Keys, window-wide, except while typing in the instruction bar.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (present) return; // the presentation owns the keys
      const target = e.target as HTMLElement | null;
      const typing =
        target && (target.tagName === "TEXTAREA" || target.tagName === "INPUT");
      if (e.key === "Escape") {
        e.preventDefault();
        onClose();
        return;
      }
      if (typing) return;
      const k = e.key;
      let next: number | null = null;
      if (k === "ArrowLeft" || k === "ArrowUp" || k === "PageUp")
        next = Math.max(0, shown - 1);
      else if (k === "ArrowRight" || k === "ArrowDown" || k === "PageDown")
        next = Math.min(count - 1, shown + 1);
      else if (k === "Home") next = 0;
      else if (k === "End") next = count - 1;
      if (next !== null) {
        e.preventDefault();
        setSelected(next);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [shown, count, onClose, present]);

  useEffect(() => {
    if (!menu) return;
    const onDoc = (e: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(e.target as Node))
        setMenu(false);
    };
    document.addEventListener("mousedown", onDoc);
    return () => document.removeEventListener("mousedown", onDoc);
  }, [menu]);

  const runExport = async (kind: "pdf" | "pptx") => {
    setMenu(false);
    setBusy(kind);
    try {
      if (kind === "pdf") await downloadDeckPdf(slides);
      else await downloadDeckPptx(slides);
    } catch (err) {
      console.error("[deck] export failed", err);
    } finally {
      setBusy("");
    }
  };

  const btn =
    "inline-flex items-center gap-1.5 rounded-md !px-2.5 !py-1.5 text-xs font-medium transition-colors hover:bg-white/10 disabled:opacity-40 disabled:hover:bg-transparent";

  // Portaled to <body> so its z-index counts in the ROOT stacking context —
  // inside the chat column, the app bar (z 1001, at the root) paints over it
  // whatever z-index the studio has. The chat's utility classes are scoped to
  // `.kasal-chat-root` (tailwind.config `important`), so the portal carries
  // that class on a wrapper; without it `fixed inset-0` would not apply and
  // the studio would land as a giant static block at the end of the page.
  return createPortal(
    <div className="kasal-chat-root">
      <div
        role="dialog"
        aria-label="Deck studio"
        aria-modal="true"
        className="kasal-deck-studio fixed inset-0 z-[1200] flex flex-col"
        style={{ background: "#111", color: "#e5e5e5" }}
      >
        <div
          className="flex items-center gap-2 px-4 py-2"
          style={{ borderBottom: "1px solid #222" }}
        >
          <span className="text-sm font-medium">Deck</span>
          <span className="text-xs" style={{ color: "#8a8a8a" }}>
            {count} slide{count === 1 ? "" : "s"}
            {history.length > 0
              ? ` · ${history.length} edit${history.length === 1 ? "" : "s"}`
              : ""}
          </span>
          <div className="ml-auto flex items-center gap-1">
            <button
              type="button"
              className={btn}
              disabled={history.length === 0 || !!working}
              onClick={undo}
              title={
                history[history.length - 1]
                  ? `Undo: ${history[history.length - 1].label}`
                  : "Undo"
              }
            >
              <Undo2 size={14} /> Undo
            </button>
            <button
              type="button"
              className={btn}
              onClick={() => setPresent(true)}
              title="Present"
            >
              <Maximize2 size={14} /> Present
            </button>
            <div ref={menuRef} className="relative">
              <button
                type="button"
                className={btn}
                disabled={!!busy}
                onClick={() => setMenu((m) => !m)}
                title="Download"
              >
                {busy ? (
                  <Loader2 size={14} className="animate-spin" />
                ) : (
                  <Download size={14} />
                )}{" "}
                Download
              </button>
              {menu && (
                <div
                  className="absolute right-0 z-10 mt-1 min-w-[10rem] overflow-hidden rounded-md border py-1 shadow-lg"
                  style={{ background: "#1c1c1c", borderColor: "#333" }}
                >
                  <button
                    type="button"
                    className="block w-full !px-3 !py-1.5 text-left text-xs hover:bg-white/10"
                    onClick={() => runExport("pdf")}
                  >
                    Download PDF
                  </button>
                  <button
                    type="button"
                    className="block w-full !px-3 !py-1.5 text-left text-xs hover:bg-white/10"
                    onClick={() => runExport("pptx")}
                  >
                    Download PowerPoint
                  </button>
                </div>
              )}
            </div>
            <button
              type="button"
              className={btn}
              onClick={onClose}
              title="Done (Esc)"
              style={{ background: "#2a2a2a" }}
            >
              <X size={14} /> Done
            </button>
          </div>
        </div>
        <div className="flex min-h-0 flex-1">
          <ThumbnailRail
            slides={slides}
            selected={shown}
            working={working?.index ?? null}
            onSelect={(i) => {
              setSelected(i);
              setBarMode({ kind: "refine" });
            }}
            onMove={(from, to) => instant({ kind: "move", from, to })}
            onDuplicate={(i) => instant({ kind: "duplicate", index: i })}
            onRemove={(i) => instant({ kind: "remove", index: i })}
            onAddAt={(at) => {
              setBarMode({ kind: "add", at });
              setError(null);
            }}
          />
          <div className="flex min-w-0 flex-1 flex-col">
            <div className="min-h-0 flex-1 p-6">
              <div className="h-full w-full">
                <ScaledFrame
                  html={stage}
                  baseWidth={SLIDE_W}
                  contain
                  upscale
                  pad={0}
                  background="#000"
                  title={`Slide ${shown + 1}`}
                />
              </div>
            </div>
            <SlideInstructionBar
              slideNumber={barMode.kind === "add" ? barMode.at + 1 : shown + 1}
              mode={barMode.kind}
              working={!!working}
              error={error}
              onApply={apply}
              onCancelAdd={() => setBarMode({ kind: "refine" })}
            />
          </div>
        </div>
        {present && (
          <DeckPresentation
            stage={stage}
            index={shown}
            count={count}
            onPrev={() => setSelected((i) => Math.max(0, i - 1))}
            onNext={() => setSelected((i) => Math.min(count - 1, i + 1))}
            onFirst={() => setSelected(0)}
            onLast={() => setSelected(Math.max(0, count - 1))}
            onClose={() => setPresent(false)}
          />
        )}
      </div>
    </div>,
    document.body,
  );
};

export default DeckStudio;
