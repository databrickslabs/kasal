import { describe, expect, it } from 'vitest';
import { SLIDE_EDIT_MSG, applySlideOps, editableStage, markSlide } from './slideEdit';

const SLIDE =
  '<section class="slide" style="padding:40px">' +            // 0
  '<style>.x{color:red}</style>' +                             // 1
  '<h1 class="x">Title</h1>' +                                 // 2
  '<div>' +                                                    // 3
  '<p>Revenue grew <strong>23%</strong></p>' +                 // 4 (strong 5)
  '<ul style="font-size:28px"><li>One</li><li>Two</li></ul>' + // 6 (li 7, 8)
  '</div>' +
  '<figure><img src="asset:abc123def">Logo<figcaption>Caption</figcaption></figure>' + // 9 (img 10, figcaption 11)
  '<svg><text>Chart label</text></svg>' +                      // 12
  '</section>';

const marked = (html: string, attr: string) =>
  Array.from(html.matchAll(new RegExp(`<(\\w+)[^>]*data-kid="(\\d+)"[^>]*${attr}[^>]*>`, 'g'))).map((m) => `${m[2]}:${m[1]}`);

describe('markSlide', () => {
  it('numbers every element, marks text blocks editable and images selectable', () => {
    const html = markSlide(SLIDE);
    expect(html).toMatch(/^<section[^>]*data-kid="0"/);
    expect(marked(html, 'data-kedit')).toEqual(['2:h1', '4:p', '7:li', '8:li', '11:figcaption']);
    expect(marked(html, 'data-kimg')).toEqual(['10:img']);
    // SVG internals are not numbered.
    expect(html).toContain('<svg data-kid="12"><text>Chart label</text></svg>');
  });

  it('leaves a string that is not one element alone', () => {
    expect(markSlide('<p>a</p><p>b</p>')).toBe('<p>a</p><p>b</p>');
  });
});

describe('applySlideOps', () => {
  it('sets a block, keeps everything else and strips every editor attribute', () => {
    const out = applySlideOps(SLIDE, [
      { op: 'html', id: '2', html: 'New title' },
      { op: 'html', id: '4', html: 'Revenue grew <strong>31%</strong>' },
    ]);
    expect(out).toContain('<h1 class="x">New title</h1>');
    expect(out).toContain('<p>Revenue grew <strong>31%</strong></p>');
    expect(out).toContain('<li>One</li><li>Two</li>');
    expect(out).toContain('src="asset:abc123def"');
    expect(out).toContain('<style>.x{color:red}</style>');
    expect(out).toMatch(/^<section class="slide" style="padding:40px">/);
    expect(out).not.toMatch(/data-k|contenteditable/);
  });

  it('adds a bullet the frame created, then the text typed into it', () => {
    const out = applySlideOps(SLIDE, [
      { op: 'html', id: '6', html: '<li data-kid="7" contenteditable="plaintext-only">One</li><li data-kid="n1"><br></li><li data-kid="8">Two</li>' },
      { op: 'html', id: 'n1', html: 'One and a half' },
    ]);
    expect(out).toContain('<ul style="font-size:28px"><li>One</li><li>One and a half</li><li>Two</li></ul>');
  });

  it('removes elements, but never the slide itself', () => {
    const out = applySlideOps(SLIDE, [
      { op: 'remove', id: '8' },
      { op: 'remove', id: '9' },
      { op: 'remove', id: '0' },
    ]);
    expect(out).toContain('<ul style="font-size:28px"><li>One</li></ul>');
    expect(out).not.toContain('<figure');
    expect(out).toMatch(/^<section class="slide"/);
  });

  it('replaces an image source, and keeps the stored reference when a snapshot carried a data URL', () => {
    expect(applySlideOps(SLIDE, [{ op: 'src', id: '10', src: 'asset:newimage99' }])).toContain('src="asset:newimage99"');
    const out = applySlideOps(SLIDE, [
      { op: 'html', id: '9', html: '<img data-kid="10" src="data:image/png;base64,AAAA">Logo<figcaption data-kid="11">Caption</figcaption>' },
    ]);
    expect(out).toContain('src="asset:abc123def"');
    expect(out).not.toContain('data:image');
  });

  it('ignores numbers the slide does not have', () => {
    expect(applySlideOps(SLIDE, [{ op: 'html', id: '99', html: 'nope' }, { op: 'remove', id: 'n7' }]))
      .toBe(applySlideOps(SLIDE, []));
  });

  it('strips active content from what the frame posts', () => {
    const out = applySlideOps(SLIDE, [{ op: 'html', id: '2', html: 'Hi<script>alert(1)</script><img src="x" onerror="alert(1)">' }]);
    const title = out.match(/<h1[^>]*>(.*?)<\/h1>/)?.[1] ?? '';
    expect(title).toContain('Hi');
    expect(title).not.toContain('<script');
    expect(title).not.toContain('onerror');
  });
});

describe('editableStage', () => {
  it('renders the marked slide with the editor script reporting under its key', () => {
    const stage = editableStage(SLIDE, 'key-1');
    expect(stage).toContain('data-kedit');
    expect(stage).toContain('"key-1"');
    expect(stage).toContain(JSON.stringify(SLIDE_EDIT_MSG));
    // The script's source never closes its own element early.
    expect(stage.match(/<\/script>/g)).toHaveLength(1);
  });
});
