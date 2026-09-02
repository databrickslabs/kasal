/**
 * Refining a deck one slide at a time.
 *
 * "Make the chart bigger on slide 3" used to mean: send the whole deck to the
 * editor, get the whole deck back. Ten slides regenerated for one change, and
 * the nine untouched ones drifting. A deck is slide-addressable by contract
 * (one `<section class="slide">` per slide), so an edit can name its slide:
 * the model writes ONE section and the frontend splices it in.
 *
 * Three kinds of edit come out of a sentence (or of the deck studio's controls):
 *   - `refine` / `add` — one generation call sized to one slide, then a splice;
 *   - `remove` / `move` / `duplicate` — pure splices, no model at all.
 *
 * Everything here is pure: parsing the sentence, finding the deck, and planning
 * the edit — the request for the one-slide call and the splice that puts its
 * answer back. The studio and the chat hook make the call.
 */
import type { ChatMessage } from '../types/chat';
import type { SlideRefineRequest } from '../../../api/chat/DeckService';
import { splitDiagramSegments } from './mdSandboxDiagram';
import {
  clearRefined,
  duplicateSlide,
  insertSlide,
  isDeck,
  markRefined,
  mergeDeckSegments,
  moveSlide,
  removeSlide,
  replaceSlide,
  splitSlides,
} from './htmlDeck';

export type SlideEdit =
  | { kind: 'refine'; index: number; instruction: string }
  | { kind: 'add'; index: number; instruction: string }
  | { kind: 'remove'; index: number }
  | { kind: 'move'; from: number; to: number }
  | { kind: 'duplicate'; index: number };

const ORDINALS: Record<string, number> = {
  first: 1, second: 2, third: 3, fourth: 4, fifth: 5,
  sixth: 6, seventh: 7, eighth: 8, ninth: 9, tenth: 10,
};

/** A slide reference — "3", "3rd", "third", "last" — as a 0-based index. */
function slideIndex(token: string | undefined, count: number): number | null {
  if (!token) return null;
  const t = token.toLowerCase().trim();
  let n: number;
  if (t === 'last' || t === 'final') n = count;
  else if (ORDINALS[t] !== undefined) n = ORDINALS[t];
  else n = parseInt(t, 10);
  if (!Number.isFinite(n) || n < 1 || n > count) return null;
  return n - 1;
}

// One slide reference, in the ways people write it.
const REF = String.raw`(?:slide\s*#?(\d+)|(?:the\s+)?(\d+)(?:st|nd|rd|th)\s+slide|(?:the\s+)?(first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth|last|final)\s+slide)`;
const ref = (m: RegExpMatchArray, at: number) => m[at] ?? m[at + 1] ?? m[at + 2];
const LEAD = String.raw`^(?:please\s+|can\s+you\s+|could\s+you\s+)?`;
const TAIL = String.raw`\s*[.!]?\s*$`;
// What separates the target from the instruction: punctuation or a joining word.
const SEP = String.raw`\s*(?:[:,\-–—]\s*|\b(?:so\s+that|so|to|and|by|about|on|that|with|showing|covering)\s+)?`;

const RE_REMOVE = new RegExp(`${LEAD}(?:delete|remove|drop|cut)\\s+${REF}${TAIL}`, 'i');
const RE_DUP = new RegExp(`${LEAD}(?:duplicate|copy|clone)\\s+${REF}${TAIL}`, 'i');
const RE_MOVE = new RegExp(
  `${LEAD}move\\s+${REF}\\s+(after|before|to)\\s+(?:slide\\s*#?|position\\s*)?(\\d+|last|the\\s+end|the\\s+start|the\\s+beginning)${TAIL}`,
  'i',
);
const RE_ADD_AT = new RegExp(
  `${LEAD}(?:add|insert|create|write)\\s+(?:a\\s+|an\\s+|one\\s+)?(?:new\\s+)?(?:final\\s+|closing\\s+|last\\s+)?slide\\s+(after|before)\\s+${REF}${SEP}(.*)$`,
  'i',
);
const RE_ADD_END = new RegExp(
  `${LEAD}(?:add|insert|create|write)\\s+(?:a\\s+|an\\s+|one\\s+)?(?:new\\s+)?(?:final\\s+|closing\\s+|last\\s+)?slide\\s+(?:at|to)\\s+the\\s+(end|start|beginning)${SEP}(.*)$`,
  'i',
);
// "slide 3: make the chart bigger" / "on slide 3, drop the footer"
const RE_REFINE_LEAD = new RegExp(`${LEAD}(?:on|in|for)?\\s*${REF}\\s*[:,\\-–—]\\s*(.+)$`, 'i');
// "refine slide 3 so it has less text" / "update the 3rd slide: bigger title"
const RE_REFINE_VERB = new RegExp(
  `${LEAD}(?:refine|update|change|edit|fix|improve|rework|redo|rewrite|tweak|polish|adjust|revise|simplify|shorten)\\s+${REF}${SEP}(.*)$`,
  'i',
);
// "make the chart bigger on slide 3" / "add a footer to the last slide"
const RE_REFINE_TRAIL = new RegExp(`${LEAD}(.+?)\\s+(?:on|in|for|of|to|into)\\s+${REF}${TAIL}`, 'i');

/**
 * The slide edit a sentence asks for, given how many slides the deck has —
 * or null when the sentence does not name a slide (a normal turn). Indices
 * are 0-based; an out-of-range slide number is not an edit.
 */
export function parseSlideEdit(message: string, count: number): SlideEdit | null {
  const text = (message || '').trim();
  if (!text || count <= 0) return null;

  let m = text.match(RE_REMOVE);
  if (m) {
    const index = slideIndex(ref(m, 1), count);
    return index === null ? null : { kind: 'remove', index };
  }
  m = text.match(RE_DUP);
  if (m) {
    const index = slideIndex(ref(m, 1), count);
    return index === null ? null : { kind: 'duplicate', index };
  }
  m = text.match(RE_MOVE);
  if (m) {
    const from = slideIndex(ref(m, 1), count);
    if (from === null) return null;
    const rel = m[4].toLowerCase();
    const target = m[5].toLowerCase().replace(/\s+/g, ' ');
    let to: number;
    if (target === 'the end' || target === 'last') to = count - 1;
    else if (target === 'the start' || target === 'the beginning') to = 0;
    else {
      const n = slideIndex(target, count);
      if (n === null) return null;
      // "after N": land after N once N has shifted; "before N"/"to N": at N.
      to = rel === 'after' ? (n > from ? n : n + 1) : n;
      if (rel === 'before' && n > from) to = n - 1;
    }
    to = Math.max(0, Math.min(count - 1, to));
    return { kind: 'move', from, to };
  }
  m = text.match(RE_ADD_AT);
  if (m) {
    const n = slideIndex(ref(m, 2), count);
    if (n === null) return null;
    const index = m[1].toLowerCase() === 'after' ? n + 1 : n;
    return { kind: 'add', index, instruction: (m[5] || '').trim() };
  }
  m = text.match(RE_ADD_END);
  if (m) {
    const index = m[1].toLowerCase() === 'end' ? count : 0;
    return { kind: 'add', index, instruction: (m[2] || '').trim() };
  }
  m = text.match(RE_REFINE_LEAD);
  if (m) {
    const index = slideIndex(ref(m, 1), count);
    return index === null ? null : { kind: 'refine', index, instruction: m[4].trim() };
  }
  m = text.match(RE_REFINE_VERB);
  if (m) {
    const index = slideIndex(ref(m, 1), count);
    return index === null ? null : { kind: 'refine', index, instruction: m[4].trim() || 'improve it' };
  }
  m = text.match(RE_REFINE_TRAIL);
  if (m) {
    const index = slideIndex(ref(m, 2), count);
    return index === null ? null : { kind: 'refine', index, instruction: m[1].trim() };
  }
  return null;
}

/** The most recent finished deck in a conversation, and the message holding it. */
export function latestDeck(messages: ChatMessage[]): { messageId: string; code: string } | null {
  for (let i = messages.length - 1; i >= 0; i--) {
    const m = messages[i];
    if (m.role !== 'assistant' || !m.content) continue;
    const seg = mergeDeckSegments(splitDiagramSegments(m.content)).find(
      (s) => s.type === 'diagram' && s.lang === 'html' && s.closed && isDeck(s.code),
    );
    if (seg && seg.type === 'diagram') return { messageId: m.id, code: seg.code };
  }
  return null;
}

/** The single-pass editor agent every refine runs — one LLM pass, no memory. */
export function refinerAgent(model?: string) {
  return {
    id: 'refiner',
    role: 'Content Editor',
    goal: 'Revise the provided artifact according to the user instruction, preserving correctness and returning the complete updated artifact.',
    backstory:
      'You are an expert editor and front-end developer who refines documents and HTML, keeping the output valid, self-contained and ready to render.',
    tools: [],
    // Pin the editor to the user-selected model. Without an explicit llm the
    // backend defaults this hand-built agent to gpt-4o, which fails in
    // Databricks environments with no OpenAI key.
    ...(model ? { llm: model } : {}),
    // A refine is a single-shot edit, not a research crew. Disabling memory
    // (the only agent → disables crew memory entirely) skips the memory
    // search/save flow; no delegation keeps it to one LLM pass.
    memory: false,
    allow_delegation: false,
  };
}

export type SlideEditPlan =
  | { kind: 'instant'; deck: string; focus: number; summary: string; done: string }
  | {
      kind: 'call';
      /** The one-slide generation call (the caller adds the model). */
      request: SlideRefineRequest;
      /** The model's slide, spliced into the deck (marked as the changed one). */
      apply: (section: string) => string;
      focus: number;
      /** Progressive and past-tense labels for the activity step. */
      summary: string;
      done: string;
    };

/**
 * What to do for a slide edit: an instant splice for structural edits, or a
 * one-slide generation call plus the splice that puts its answer back.
 */
export function planSlideEdit(edit: SlideEdit, deck: string): SlideEditPlan {
  const base = clearRefined(deck);
  const slides = splitSlides(base);
  const n = slides.length;
  const human = (i: number) => `slide ${i + 1}`;

  if (edit.kind === 'remove') {
    const next = removeSlide(base, edit.index);
    const label = `Removed ${human(edit.index)}`;
    return { kind: 'instant', deck: next, focus: Math.max(0, Math.min(edit.index, n - 2)), summary: label, done: label };
  }
  if (edit.kind === 'duplicate') {
    const next = replaceSlide(duplicateSlide(base, edit.index), edit.index + 1, markRefined(slides[edit.index]));
    const label = `Duplicated ${human(edit.index)}`;
    return { kind: 'instant', deck: next, focus: edit.index + 1, summary: label, done: label };
  }
  if (edit.kind === 'move') {
    const moved = moveSlide(base, edit.from, edit.to);
    const next = replaceSlide(moved, edit.to, markRefined(slides[edit.from]));
    const label = `Moved ${human(edit.from)} to position ${edit.to + 1}`;
    return { kind: 'instant', deck: next, focus: edit.to, summary: label, done: label };
  }

  if (edit.kind === 'refine') {
    const i = edit.index;
    // The cover shows the deck's design; when the cover itself is the target,
    // the next slide stands in.
    const reference = slides[i === 0 ? 1 : 0];
    return {
      kind: 'call',
      request: {
        mode: 'refine',
        instruction: edit.instruction,
        slide: slides[i],
        reference: reference || undefined,
        position: `${i + 1} of ${n}`,
      },
      apply: (section) => replaceSlide(base, i, markRefined(section)),
      focus: i,
      summary: `Refining ${human(i)}`,
      done: `Refined ${human(i)}`,
    };
  }

  // add
  const at = Math.max(0, Math.min(edit.index, n));
  return {
    kind: 'call',
    request: {
      mode: 'add',
      instruction: edit.instruction,
      before: slides[at - 1] || undefined,
      after: slides[at] || undefined,
      position: `${at + 1} of ${n + 1}`,
    },
    apply: (section) => insertSlide(base, at, markRefined(section)),
    focus: at,
    summary: `Adding ${human(at)}`,
    done: `Added ${human(at)}`,
  };
}
