import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { frameEditor } from './slideEditFrame';
import { markSlide } from './slideEdit';

// The editor script, run against a marked slide in this document (in the app
// it runs inside the stage frame; here `parent` is this window).
const SLIDE =
  '<section class="slide">' +                                   // 0
  '<div class="card"><h1>Title</h1></div>' +                    // 1 (h1 2)
  '<ul><li>One</li><li>Two</li></ul>' +                         // 3 (li 4, 5)
  '<p>Body</p>' +                                               // 6
  '<img src="data:image/gif;base64,R0lGODlhAQABAAAAACw=">' +     // 7
  '</section>';

const byKid = (id: string) => document.querySelector(`[data-kid="${id}"]`) as HTMLElement;
const caretAtEnd = (el: HTMLElement) => {
  const r = document.createRange();
  r.selectNodeContents(el);
  r.collapse(false);
  const sel = window.getSelection()!;
  sel.removeAllRanges();
  sel.addRange(r);
};
const key = (el: HTMLElement, k: string) => el.dispatchEvent(new KeyboardEvent('keydown', { key: k, bubbles: true, cancelable: true }));

describe('frameEditor', () => {
  let posts: Record<string, unknown>[];
  let dispose: () => void;

  beforeEach(() => {
    document.body.innerHTML = `<div id="kft">${markSlide(SLIDE)}</div>`;
    posts = [];
    vi.spyOn(window, 'postMessage').mockImplementation((m: unknown) => { posts.push(m as Record<string, unknown>); });
    dispose = frameEditor('session-key', 'msg-type');
  });
  afterEach(() => {
    dispose();
    vi.restoreAllMocks();
  });
  const ops = () => posts.map(({ op, k }) => (k === undefined ? op : `${op}:${k}`));

  it('reports what is typed into a block, tagged with the session', () => {
    const h1 = byKid('2');
    h1.textContent = 'New title';
    h1.dispatchEvent(new Event('input', { bubbles: true }));
    expect(posts).toEqual([{ op: 'html', k: '2', html: 'New title', t: 'msg-type', id: 'session-key' }]);
  });

  it('starts a new bullet on Enter and reports the list', () => {
    const li = byKid('4');
    caretAtEnd(li);
    key(li, 'Enter');
    const items = Array.from(byKid('3').children).map((x) => [x.getAttribute('data-kid'), x.textContent]);
    expect(items).toEqual([['4', 'One'], ['n1', ''], ['5', 'Two']]);
    expect(ops()).toEqual(['html:3']);
    expect(String(posts[0].html)).toContain('data-kid="n1"');
  });

  it('removes an empty bullet on Backspace, and the list with its last one', () => {
    const two = byKid('5');
    two.textContent = '';
    key(two, 'Backspace');
    expect(byKid('5')).toBeNull();
    const one = byKid('4');
    one.textContent = '';
    key(one, 'Backspace');
    expect(byKid('3')).toBeNull();
    expect(ops()).toEqual(['remove:5', 'commit', 'remove:3', 'commit']);
  });

  it('removes a block left empty, with the wrapper it was alone in, and asks for a save', () => {
    const h1 = byKid('2');
    h1.textContent = '';
    h1.dispatchEvent(new FocusEvent('focusout', { bubbles: true }));
    expect(byKid('1')).toBeNull();
    expect(ops()).toEqual(['remove:1', 'commit']);
  });

  it('removes an empty text box on Backspace and offers no delete button', () => {
    const p = byKid('6');
    p.dispatchEvent(new FocusEvent('focusin', { bubbles: true }));
    expect(document.querySelectorAll('button')).toHaveLength(0);
    key(p, 'Backspace');
    // Not empty yet: Backspace edits the text as usual.
    expect(byKid('6')).not.toBeNull();
    p.textContent = '';
    key(p, 'Backspace');
    expect(byKid('6')).toBeNull();
    expect(ops()).toEqual(['remove:6', 'commit']);
  });

  it('selects an image on click and offers to replace or remove it', () => {
    byKid('7').click();
    expect(byKid('7').hasAttribute('data-ksel')).toBe(true);
    const buttons = () => Array.from(document.querySelectorAll('button'));
    buttons().find((b) => b.textContent === 'Replace image')!.click();
    expect(ops()).toEqual(['pick:7']);
    buttons().find((b) => b.textContent === 'Remove image')!.click();
    expect(byKid('7')).toBeNull();
    expect(ops()).toEqual(['pick:7', 'remove:7', 'commit']);
  });

  it('stops reporting after teardown', () => {
    dispose();
    byKid('2').dispatchEvent(new Event('input', { bubbles: true }));
    expect(posts).toEqual([]);
    dispose = () => {};
  });
});
