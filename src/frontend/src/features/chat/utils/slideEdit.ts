/**
 * Editing a slide directly on the deck studio's stage.
 *
 * The stage is a sandboxed frame, so the studio cannot reach into the slide's
 * DOM. It renders a marked copy instead: every element carries a number
 * (`data-kid`), text blocks are editable (`data-kedit`) and images selectable
 * (`data-kimg`). A script in the frame (slideEditFrame) reports what the person
 * did as operations on those numbers, and the studio replays the operations on
 * the STORED slide. The stored slide keeps its `asset:<id>` image references
 * where the rendered copy has data URLs; the swap only rewrites attribute
 * text, so both parse to the same elements and the numbers agree.
 *
 * The frame's own document is never saved: a slide may run scripts that build
 * DOM, and saving their output would duplicate it on the next render.
 */
import { sanitizeDeckDocument } from './deckExport';
import { stageFor } from './htmlDeck';
import { frameEditorScript } from './slideEditFrame';

/** The message type the editing frame posts to the studio. */
export const SLIDE_EDIT_MSG = 'kasal-slide-edit';

/** What the frame reports. `id` is an element's number, or `n<k>` for one the frame created. */
export type SlideOp =
  /** The element's content is now `html` (a text block as typed, or a list after a bullet was added). */
  | { op: 'html'; id: string; html: string }
  | { op: 'remove'; id: string }
  /** An image now shows `src` (an `asset:<id>` reference). */
  | { op: 'src'; id: string; src: string };

const KID = 'data-kid';
const KEDIT = 'data-kedit';
const KIMG = 'data-kimg';
// Every attribute the editor adds, removed again before a slide is stored.
const EDITOR_ATTRS = [KID, KEDIT, KIMG, 'data-ksel', 'contenteditable'];

// Numbered, but not looked inside: their content is not edited by hand.
const OPAQUE = new Set(['script', 'style', 'template', 'noscript', 'svg', 'math']);

// Never text blocks, and nothing inside them is either.
const SKIP = new Set([
  'script', 'style', 'template', 'noscript', 'svg', 'math', 'textarea', 'select',
  'iframe', 'object', 'embed', 'canvas', 'video', 'audio',
]);

// A block holding any of these is not edited as a whole: its inner HTML would
// carry an image's data URL (or an active element) back. Its children are tried instead.
const NOT_TEXT =
  'img,picture,svg,math,video,audio,canvas,iframe,object,embed,input,select,textarea,script,style,template';

/** One slide's outer HTML as an element, or null when it is not exactly one element. */
function parseSlide(slide: string): Element | null {
  const t = document.createElement('template');
  t.innerHTML = slide || '';
  return t.content.children.length === 1 ? t.content.firstElementChild : null;
}

/** Every element of the slide in document order, the slide itself first. */
function numbered(root: Element): Element[] {
  const out: Element[] = [root];
  const walk = (el: Element) => {
    for (const child of Array.from(el.children)) {
      out.push(child);
      if (!OPAQUE.has(child.localName)) walk(child);
    }
  };
  if (!OPAQUE.has(root.localName)) walk(root);
  return out;
}

const hasOwnText = (el: Element): boolean =>
  Array.from(el.childNodes).some((n) => n.nodeType === Node.TEXT_NODE && (n.textContent || '').trim() !== '');

const isTextBlock = (el: Element): boolean => hasOwnText(el) && !el.querySelector(NOT_TEXT);

/**
 * The slide's text blocks: the outermost elements that hold text of their own
 * and no media. Inline formatting inside a block belongs to that block.
 */
function textBlocks(root: Element): Element[] {
  if (isTextBlock(root)) return [root];
  const out: Element[] = [];
  const collect = (el: Element) => {
    for (const child of Array.from(el.children)) {
      if (SKIP.has(child.localName)) continue;
      if (isTextBlock(child)) out.push(child);
      else collect(child);
    }
  };
  collect(root);
  return out;
}

/** The slide marked for the editing frame: numbered, text blocks editable, images selectable. */
export function markSlide(slide: string): string {
  const root = parseSlide(slide);
  if (!root) return slide;
  const all = numbered(root);
  all.forEach((el, i) => el.setAttribute(KID, String(i)));
  textBlocks(root).forEach((el) => el.setAttribute(KEDIT, ''));
  all.filter((el) => el.localName === 'img').forEach((el) => el.setAttribute(KIMG, ''));
  return root.outerHTML;
}

/** `html` set as `el`'s content, keeping the stored source of every image it already had. */
function setContent(el: Element, html: string, byId: Map<string, Element>): void {
  const sources = new Map<string, string | null>();
  el.querySelectorAll(`img[${KID}]`).forEach((img) => sources.set(img.getAttribute(KID) || '', img.getAttribute('src')));
  // The frame runs the slide's own scripts, so what it posts is untrusted.
  el.innerHTML = sanitizeDeckDocument(html);
  el.querySelectorAll(`[${KID}]`).forEach((node) => {
    const id = node.getAttribute(KID) || '';
    byId.set(id, node);
    // The frame showed this image from a data URL; the store keeps the reference.
    const src = sources.get(id);
    if (node.localName === 'img' && src != null) node.setAttribute('src', src);
  });
}

/**
 * The stored slide with `ops` replayed in order. Operations on numbers the
 * slide does not have (or no longer has) are ignored, and the slide element
 * itself is never removed. Everything else is kept, re-serialized by the
 * browser's parser.
 */
export function applySlideOps(slide: string, ops: SlideOp[]): string {
  const root = parseSlide(slide);
  if (!root) return slide;
  const byId = new Map<string, Element>();
  numbered(root).forEach((el, i) => {
    el.setAttribute(KID, String(i));
    byId.set(String(i), el);
  });
  for (const o of ops) {
    const el = byId.get(o.id);
    if (!el) continue;
    if (o.op === 'remove') {
      if (el !== root) el.remove();
    } else if (o.op === 'src') {
      if (el.localName === 'img') {
        el.setAttribute('src', o.src);
        el.removeAttribute('srcset');
      }
    } else {
      setContent(el, o.html, byId);
    }
  }
  for (const el of [root, ...Array.from(root.querySelectorAll('*'))]) {
    for (const attr of EDITOR_ATTRS) el.removeAttribute(attr);
  }
  return root.outerHTML;
}

const EDIT_STYLE =
  '<style>[' + KEDIT + ']{cursor:text;}' +
  '[' + KEDIT + ']:hover,[' + KIMG + ']:hover{outline:2px dashed rgba(47,111,235,.55);outline-offset:3px;}' +
  '[' + KEDIT + ']:focus{outline:2px solid #2f6feb;outline-offset:3px;}' +
  '[' + KIMG + ']{cursor:pointer;}' +
  '[data-ksel]{outline:3px solid #2f6feb !important;outline-offset:3px;}</style>';

/** The editing frame's content: the marked slide on the fixed stage, reporting under `key`. */
export function editableStage(slide: string, key: string): string {
  return stageFor(markSlide(slide)) + EDIT_STYLE + frameEditorScript(key, SLIDE_EDIT_MSG);
}
