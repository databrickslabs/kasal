/**
 * The script that runs inside the deck studio's editing frame (see slideEdit).
 *
 * It is written as an ordinary, type-checked function and shipped into the
 * frame as source text (`Function.prototype.toString`), so it must stay
 * SELF-CONTAINED: no imports, no module-level names — everything it needs is
 * inside the function or passed as an argument.
 *
 * What it does, all inside the slide:
 *  - text blocks are typed into (plain text, so a paste brings no foreign
 *    styling; existing formatting stays); every keystroke reports the block;
 *  - Enter in a bullet starts a new bullet;
 *  - Backspace (or Delete) in an empty block removes it, and a block left
 *    empty is removed when the caret leaves it — that is how a text box goes;
 *  - clicking an image selects it, with Replace / Remove on a small bar;
 *  - leaving a block (or the frame) asks the studio to save.
 *
 * Returns a teardown (tests use it; the frame never does).
 */
export function frameEditor(key: string, type: string): () => void {
  const life = new AbortController();
  const on = { signal: life.signal };
  const capture = { signal: life.signal, capture: true };
  const KID = 'data-kid';
  const KEDIT = 'data-kedit';
  const KIMG = 'data-kimg';
  const KSEL = 'data-ksel';
  const MEDIA = 'img,svg,video,canvas,picture,iframe,object,embed';
  let fresh = 0;
  let quiet = false;
  // The selected image, which the bar acts on.
  let target: HTMLElement | null = null;

  const post = (m: Record<string, unknown>) => parent.postMessage({ ...m, t: type, id: key }, '*');
  const idOf = (el: Element) => el.getAttribute(KID) || '';
  const blockOf = (n: EventTarget | null): HTMLElement | null =>
    n instanceof Element ? (n.closest(`[${KEDIT}]`) as HTMLElement | null) : null;
  const isEmpty = (el: Element) => !(el.textContent || '').trim() && !el.querySelector(MEDIA);
  const makeEditable = (el: HTMLElement) => {
    try {
      el.contentEditable = 'plaintext-only';
    } catch {
      // Not supported: fall back to rich editing below.
    }
    if (el.contentEditable !== 'plaintext-only') el.contentEditable = 'true';
  };

  /** Remove `el` and every ancestor that leaves empty, up to the slide; the outermost removed. */
  const removeUp = (el: Element): Element => {
    let top = el;
    let up = el.parentElement;
    el.remove();
    while (up && up.hasAttribute(KID) && idOf(up) !== '0' && up.children.length === 0 && !(up.textContent || '').trim()) {
      top = up;
      const next: HTMLElement | null = up.parentElement;
      up.remove();
      up = next;
    }
    return top;
  };

  const bar = document.createElement('div');
  bar.style.cssText =
    'position:fixed;z-index:2147483647;display:none;gap:2px;padding:3px;border-radius:8px;background:#111827;' +
    'box-shadow:0 4px 12px rgba(0,0,0,.25);font:500 12px -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;';

  const hide = () => {
    if (target) target.removeAttribute(KSEL);
    target = null;
    bar.style.display = 'none';
  };
  const place = () => {
    if (!target || !target.isConnected) {
      hide();
      return;
    }
    const r = target.getBoundingClientRect();
    const w = bar.offsetWidth;
    const h = bar.offsetHeight;
    // Inside the image's top-left corner, covering nothing but the image; above
    // (or below) an image too small to hold it.
    const inside = r.width > w + 16 && r.height > h + 16;
    const left = inside ? r.left + 8 : r.left;
    let top = inside ? r.top + 8 : r.top - h - 6;
    if (top < 4) top = r.bottom + 6;
    bar.style.top = `${top}px`;
    bar.style.left = `${Math.max(4, Math.min(left, window.innerWidth - w - 4))}px`;
  };
  const removeImage = (img: Element) => {
    hide();
    post({ op: 'remove', k: idOf(removeUp(img)) });
    post({ op: 'commit' });
  };
  const button = (label: string, run: () => void) => {
    const b = document.createElement('button');
    b.type = 'button';
    b.textContent = label;
    b.style.cssText = 'all:unset;cursor:pointer;color:#f9fafb;padding:4px 9px;border-radius:6px;';
    b.addEventListener('mouseenter', () => { b.style.background = '#374151'; });
    b.addEventListener('mouseleave', () => { b.style.background = ''; });
    b.addEventListener('click', (e) => {
      e.preventDefault();
      e.stopPropagation();
      run();
    });
    return b;
  };
  const select = (img: HTMLElement) => {
    hide();
    target = img;
    img.setAttribute(KSEL, '');
    bar.replaceChildren(
      button('Replace image', () => post({ op: 'pick', k: idOf(img) })),
      button('Remove image', () => removeImage(img)),
    );
    bar.style.display = 'flex';
    place();
  };

  const caretAt = (el: HTMLElement, atEnd: boolean) => {
    const sel = window.getSelection();
    if (!sel) return;
    const r = document.createRange();
    r.selectNodeContents(el);
    r.collapse(!atEnd);
    sel.removeAllRanges();
    sel.addRange(r);
  };
  // Enter in a bullet: what follows the caret moves into a new bullet below.
  const splitItem = (li: HTMLElement) => {
    const sel = window.getSelection();
    if (!sel || !sel.rangeCount) return;
    const range = sel.getRangeAt(0);
    range.deleteContents();
    const tail = document.createRange();
    tail.setStart(range.endContainer, range.endOffset);
    tail.setEnd(li, li.childNodes.length);
    const item = li.cloneNode(false) as HTMLElement;
    item.setAttribute(KID, `n${++fresh}`);
    item.append(tail.extractContents());
    for (const node of [li, item]) if (!node.textContent) node.innerHTML = '<br>';
    li.after(item);
    makeEditable(item);
    quiet = true;
    item.focus();
    quiet = false;
    caretAt(item, false);
    const list = item.parentElement;
    if (list) post({ op: 'html', k: idOf(list), html: list.innerHTML });
  };
  // Backspace in an empty block: remove it and continue at the end of the block before.
  const dropBlock = (el: HTMLElement) => {
    const blocks = Array.from(document.querySelectorAll<HTMLElement>(`[${KEDIT}]`));
    const prev = blocks[blocks.indexOf(el) - 1];
    quiet = true;
    const top = removeUp(el);
    quiet = false;
    post({ op: 'remove', k: idOf(top) });
    post({ op: 'commit' });
    if (prev && prev.isConnected) {
      prev.focus();
      caretAt(prev, true);
    }
  };

  document.addEventListener('input', (e) => {
    const el = blockOf(e.target);
    if (el) post({ op: 'html', k: idOf(el), html: el.innerHTML });
  }, capture);
  document.addEventListener('focusout', (e) => {
    const el = blockOf(e.target);
    if (!el || quiet) return;
    // A block left empty goes, with whatever held only it.
    if (el.isConnected && isEmpty(el)) post({ op: 'remove', k: idOf(removeUp(el)) });
    post({ op: 'commit' });
  }, on);
  window.addEventListener('blur', () => post({ op: 'commit' }), on);
  document.addEventListener('keydown', (e) => {
    const el = blockOf(e.target);
    if (el) {
      if (e.key === 'Escape') {
        e.preventDefault();
        el.blur();
      } else if (el.localName === 'li' && e.key === 'Enter' && !e.shiftKey && !e.isComposing) {
        e.preventDefault();
        splitItem(el);
      } else if ((e.key === 'Backspace' || e.key === 'Delete') && isEmpty(el)) {
        e.preventDefault();
        dropBlock(el);
      }
      return;
    }
    if (target && (e.key === 'Delete' || e.key === 'Backspace')) {
      e.preventDefault();
      removeImage(target);
    } else if (target && e.key === 'Escape') {
      hide();
    }
  }, capture);
  document.addEventListener('click', (e) => {
    const t = e.target instanceof Element ? e.target : null;
    if (!t || bar.contains(t)) return;
    // The stage is for editing: a link on the slide must not navigate the frame.
    if (t.closest('a')) e.preventDefault();
    const img = t.closest(`img[${KIMG}]`) as HTMLElement | null;
    if (img) select(img);
    else if (target) hide();
  }, capture);
  window.addEventListener('resize', () => requestAnimationFrame(place), on);

  const arm = () => {
    document.querySelectorAll<HTMLElement>(`[${KEDIT}]`).forEach(makeEditable);
    document.body.append(bar);
  };
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', arm, on);
  else arm();
  return () => {
    life.abort();
    bar.remove();
  };
}

/** `frameEditor` as a `<script>` for the frame, reporting under `key` with message type `type`. */
export function frameEditorScript(key: string, type: string): string {
  const call = `(${frameEditor.toString()})(${JSON.stringify(key)},${JSON.stringify(type)});`;
  // `</` would end the script element early if it ever appeared in the source.
  return '<' + 'script>' + call.replace(/<\//g, '<\\/') + '<' + '/script>';
}
