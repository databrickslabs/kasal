import { describe, it, expect } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import UiRenderer from './UiRenderer';
import { UiSurface } from '../../utils/uiDocument';

const surface = (components: Record<string, unknown>, rootId = 'root', data = {}): UiSurface =>
  ({ rootId, components: components as never, data }) as UiSurface;

// Quiz option labels render as "A" + ". " + text across separate text nodes, so
// match on the button element's combined textContent instead of a literal string.
const clickButton = (full: string) =>
  fireEvent.click(
    screen.getByText((_, node) => node?.tagName === 'BUTTON' && node.textContent === full),
  );

describe('UiRenderer', () => {
  it('renders a Column root with Text, a Row, a TextField and a Button', () => {
    render(
      <UiRenderer
        surface={surface(
          {
            root: { id: 'root', component: 'Column', children: ['title', 'row'], justify: 'spaceBetween', align: 'center' },
            title: { id: 'title', component: 'Text', text: 'Dashboard', variant: 'h1' },
            row: { id: 'row', component: 'Row', children: ['field', 'btn'], justify: 'end', align: 'end' },
            field: { id: 'field', component: 'TextField', label: 'Name', value: { path: '/name' }, weight: 1 },
            btn: { id: 'btn', component: 'Button', child: 'btnlabel' },
            btnlabel: { id: 'btnlabel', component: 'Text', text: 'Go' },
          },
          'root',
          { name: 'Ada' },
        )}
      />,
    );
    expect(screen.getByText('Dashboard')).toBeInTheDocument();
    expect(screen.getByText('Name')).toBeInTheDocument();
    expect(screen.getByText('Go')).toBeInTheDocument();
    expect(screen.getByDisplayValue('Ada')).toBeInTheDocument();
  });

  it('edits a TextField bound value into a nested local data-model path', () => {
    render(
      <UiRenderer
        surface={surface({
          // nested path exercises the multi-segment write in setData
          root: { id: 'root', component: 'TextField', label: 'Email', value: { path: '/contact/email' } },
        })}
      />,
    );
    const input = screen.getByRole('textbox');
    fireEvent.change(input, { target: { value: 'a@b.com' } });
    expect(screen.getByDisplayValue('a@b.com')).toBeInTheDocument();
  });

  it('renders an unbound TextField (no path) and ignores edits to it', () => {
    render(
      <UiRenderer
        surface={surface({ root: { id: 'root', component: 'TextField', label: 'Freeform' } })}
      />,
    );
    const input = screen.getByRole('textbox');
    expect(input).toHaveValue('');
    // no path → onChange is a no-op (value stays controlled at '')
    fireEvent.change(input, { target: { value: 'x' } });
    expect(input).toHaveValue('');
  });

  it('renders a Button with a fallback label when it has no child', () => {
    render(
      <UiRenderer
        surface={surface({ root: { id: 'root', component: 'Button' } })}
      />,
    );
    expect(screen.getByRole('button')).toHaveTextContent('Action');
  });

  it('uses default text variant and ignores missing child references', () => {
    render(
      <UiRenderer
        surface={surface({
          root: { id: 'root', component: 'Column', children: ['t', 'ghost'] },
          t: { id: 't', component: 'Text', text: 'plain' }, // no variant → body
          // 'ghost' is referenced but not defined → rendered as nothing
        })}
      />,
    );
    expect(screen.getByText('plain')).toBeInTheDocument();
  });

  it('guards against cyclic references without infinite recursion', () => {
    const { container } = render(
      <UiRenderer
        surface={surface({
          root: { id: 'root', component: 'Column', children: ['child'] },
          child: { id: 'child', component: 'Column', children: ['root'] }, // cycle
        })}
      />,
    );
    // Renders without hanging; the cyclic re-entry is dropped.
    expect(container.querySelector('div')).toBeInTheDocument();
  });

  it('renders nothing when the root id is unknown', () => {
    const { container } = render(<UiRenderer surface={surface({}, 'missing')} />);
    // Only the outer padding wrapper, no node content.
    expect(container.textContent).toBe('');
  });

  it('handles a Row with no children and a TextField without a path binding', () => {
    render(
      <UiRenderer
        surface={surface({
          root: { id: 'root', component: 'Row' }, // no children array
          // a standalone unbound text field is also fine
        })}
      />,
    );
    // no throw; nothing to assert beyond successful render
    expect(screen.queryByRole('textbox')).not.toBeInTheDocument();
  });
});

describe('UiRenderer — rich components', () => {
  it('renders Card (title + children), List, Divider and Slide', () => {
    const { container } = render(
      <UiRenderer surface={surface({
        root: { id: 'root', component: 'Column', children: ['card', 'list', 'div', 'slide'] },
        card: { id: 'card', component: 'Card', title: 'Panel', children: ['ct'], weight: 1 },
        ct: { id: 'ct', component: 'Text', text: 'card body' },
        list: { id: 'list', component: 'List', children: ['li1', 'li2'] },
        li1: { id: 'li1', component: 'Text', text: 'item one' },
        li2: { id: 'li2', component: 'Text', text: 'item two' },
        div: { id: 'div', component: 'Divider' },
        slide: { id: 'slide', component: 'Slide', title: 'Slide title', children: ['st'] },
        st: { id: 'st', component: 'Text', text: 'slide body' },
      })} />,
    );
    expect(screen.getByText('Panel')).toBeInTheDocument();
    expect(screen.getByText('card body')).toBeInTheDocument();
    expect(screen.getByText('item one')).toBeInTheDocument();
    expect(screen.getByText('Slide title')).toBeInTheDocument();
    expect(container.querySelectorAll('table').length).toBe(0);
  });

  it('renders Image (with and without url) and Icon (known, alias, unknown)', () => {
    const { container, rerender } = render(
      <UiRenderer surface={surface({
        root: { id: 'root', component: 'Column', children: ['img', 'noimg', 'ic1', 'ic2', 'ic3'] },
        img: { id: 'img', component: 'Image', url: 'https://example.com/x.png', alt: 'pic' },
        noimg: { id: 'noimg', component: 'Image' }, // no url → null
        ic1: { id: 'ic1', component: 'Icon', name: 'chart' },       // known
        ic2: { id: 'ic2', component: 'Icon', name: 'trending-up' }, // alias → trending
        ic3: { id: 'ic3', component: 'Icon', name: 'totally-unknown' }, // fallback dot
      })} />,
    );
    expect(container.querySelector('img')).toHaveAttribute('alt', 'pic');
    expect(container.querySelectorAll('img').length).toBe(1); // the no-url image is skipped
    expect(container.querySelectorAll('svg').length).toBeGreaterThanOrEqual(2);
    rerender(<UiRenderer surface={surface({ root: { id: 'root', component: 'Icon' } })} />);
    expect(container).toBeTruthy();
  });

  it('renders Badge in each tone', () => {
    render(<UiRenderer surface={surface({
      root: { id: 'root', component: 'Column', children: ['b1', 'b2', 'b3', 'b4'] },
      b1: { id: 'b1', component: 'Badge', text: 'good', tone: 'good' },
      b2: { id: 'b2', component: 'Badge', text: 'warn', tone: 'warn' },
      b3: { id: 'b3', component: 'Badge', text: 'bad', tone: 'bad' },
      b4: { id: 'b4', component: 'Badge', text: 'plain' }, // default neutral
    })} />);
    ['good', 'warn', 'bad', 'plain'].forEach((t) => expect(screen.getByText(t)).toBeInTheDocument());
  });

  it('toggles a bound CheckBox and ignores an unbound one', () => {
    render(<UiRenderer surface={surface({
      root: { id: 'root', component: 'Column', children: ['c1', 'c2'] },
      c1: { id: 'c1', component: 'CheckBox', label: 'agree', value: { path: '/agree' } },
      c2: { id: 'c2', component: 'CheckBox', label: 'nopath' },
    })} />);
    const boxes = screen.getAllByRole('checkbox');
    expect(boxes[0]).not.toBeChecked();
    fireEvent.click(boxes[0]);
    expect(boxes[0]).toBeChecked();
    fireEvent.click(boxes[1]); // unbound → no path → no-op, no throw
  });

  it('renders ChoicePicker (object + string options), selects, and handles unbound', () => {
    render(<UiRenderer surface={surface({
      root: { id: 'root', component: 'Column', children: ['cp1', 'cp2'] },
      cp1: { id: 'cp1', component: 'ChoicePicker', label: 'Pick', value: { path: '/pick' }, options: [{ label: 'One', value: '1' }, { label: 'Two', value: '2' }] },
      cp2: { id: 'cp2', component: 'ChoicePicker', options: ['a', 'b'] }, // string options, unbound
    })} />);
    expect(screen.getByText('One')).toBeInTheDocument();
    const radios = screen.getAllByRole('radio');
    fireEvent.click(radios[1]);
    expect(radios[1]).toBeChecked();
    fireEvent.click(screen.getByText('a')); // unbound string option → no-op
  });

  it('renders Dashboard + Stat (with/without delta)', () => {
    render(<UiRenderer surface={surface({
      root: { id: 'root', component: 'Dashboard', children: ['s1', 's2'] },
      s1: { id: 's1', component: 'Stat', label: 'Revenue', value: '$1M', delta: '+5%', tone: 'good' },
      s2: { id: 's2', component: 'Stat', label: 'Users', value: '42' }, // no delta
    })} />);
    expect(screen.getByText('Revenue')).toBeInTheDocument();
    expect(screen.getByText('$1M')).toBeInTheDocument();
    expect(screen.getByText('+5%')).toBeInTheDocument();
    expect(screen.getByText('42')).toBeInTheDocument();
  });

  it('renders bar, line and pie Charts and skips empty data', () => {
    const { container } = render(<UiRenderer surface={surface({
      root: { id: 'root', component: 'Column', children: ['bar', 'line', 'pie', 'empty'] },
      bar: { id: 'bar', component: 'Chart', chartType: 'bar', title: 'Bars', data: [{ label: 'A', value: 3 }, { label: 'B', value: 7 }] },
      line: { id: 'line', component: 'Chart', chartType: 'line', data: [{ label: 'X', value: 1 }, { label: 'Y', value: 5 }] },
      pie: { id: 'pie', component: 'Chart', chartType: 'pie', data: [{ label: 'P', value: 2 }, { label: 'Q', value: 8 }] },
      empty: { id: 'empty', component: 'Chart', chartType: 'bar', data: [] }, // → null
    })} />);
    expect(screen.getByText('Bars')).toBeInTheDocument();
    expect(container.querySelectorAll('svg').length).toBe(3); // empty chart renders nothing
  });

  it('reads Chart data from the bound data model', () => {
    const { container } = render(<UiRenderer surface={surface({
      root: { id: 'root', component: 'Chart', chartType: 'bar', data: { path: '/series' } },
    }, 'root', { series: [{ label: 'Z', value: 9 }] })} />);
    expect(container.querySelector('svg')).toBeInTheDocument();
  });

  it('renders a Table from array rows and object rows', () => {
    const { container, rerender } = render(<UiRenderer surface={surface({
      root: { id: 'root', component: 'Table', columns: ['Name', 'Age'], rows: [['Ada', 36], ['Bo', 22]] },
    })} />);
    expect(screen.getByText('Name')).toBeInTheDocument();
    expect(screen.getByText('Ada')).toBeInTheDocument();
    expect(container.querySelectorAll('tbody tr').length).toBe(2);
    // object rows mapped by column header (both literal, so rerender is fine)
    rerender(<UiRenderer surface={surface({
      root: { id: 'root', component: 'Table', columns: ['Name', 'Age'], rows: [{ Name: 'Cy', Age: 40 }] },
    })} />);
    expect(screen.getByText('Cy')).toBeInTheDocument();
  });

  it('reads Table rows from the bound data model, with no columns and object cells', () => {
    render(<UiRenderer surface={surface({
      root: { id: 'root', component: 'Table', rows: { path: '/rows' } },
    }, 'root', { rows: [['solo'], [{ nested: true }]] })} />);
    expect(screen.getByText('solo')).toBeInTheDocument();
  });

  it('renders { values: [...] } positional rows (the data_array shape models emit)', () => {
    const { container } = render(<UiRenderer surface={surface({
      root: {
        id: 'root', component: 'Table', columns: ['Event ID', 'Event Name'],
        rows: [
          { values: ['EVT001', 'AI Day 2026 Paris'] },
          { values: [{ string_value: 'EVT002' }, { string_value: 'VivaTech 2026' }] },
        ],
      },
    })} />);
    expect(container.querySelectorAll('tbody tr').length).toBe(2);
    expect(screen.getByText('AI Day 2026 Paris')).toBeInTheDocument();
    // Databricks {string_value: …} cell wrappers are unwrapped to the scalar.
    expect(screen.getByText('VivaTech 2026')).toBeInTheDocument();
    expect(screen.getByText('EVT002')).toBeInTheDocument();
  });

  it('renders {label,key} object columns with rows keyed by key', () => {
    render(<UiRenderer surface={surface({
      root: {
        id: 'root', component: 'Table',
        columns: [
          { label: 'Event ID', key: 'event_id' },
          { label: 'Event Name', key: 'event_name' },
        ],
        rows: [
          { event_id: 'EVT001', event_name: 'PCAIDE 2026' },
          { event_id: 'EVT002', event_name: 'AI Day 2026 Paris' },
        ],
      },
    })} />);
    // Header shows the LABEL, not "[object Object]".
    expect(screen.getByText('Event Name')).toBeInTheDocument();
    expect(screen.queryByText(/object Object/i)).not.toBeInTheDocument();
    // Cells resolve via each column's KEY.
    expect(screen.getByText('PCAIDE 2026')).toBeInTheDocument();
    expect(screen.getByText('AI Day 2026 Paris')).toBeInTheDocument();
  });
});

describe('UiRenderer — branch coverage', () => {
  it('Text falls back to body for a non-string / unknown variant and empty text', () => {
    render(<UiRenderer surface={surface({
      root: { id: 'root', component: 'Column', children: ['t1', 't2', 't3'] },
      t1: { id: 't1', component: 'Text', text: 'plain' }, // no variant → body
      t2: { id: 't2', component: 'Text', text: 'odd', variant: 42 }, // non-string variant
      t3: { id: 't3', component: 'Text', variant: 'nope' }, // unknown variant + null text
    })} />);
    expect(screen.getByText('plain')).toBeInTheDocument();
    expect(screen.getByText('odd')).toBeInTheDocument();
  });

  it('Row/Column use default justify/align, and Button without a child shows "Action"', () => {
    render(<UiRenderer surface={surface({
      root: { id: 'root', component: 'Row', children: ['btn'] }, // no justify/align → fallbacks
      btn: { id: 'btn', component: 'Button' }, // no child → "Action"
    })} />);
    expect(screen.getByText('Action')).toBeInTheDocument();
  });

  it('TextField without a label or path binding renders and is editable without writing data', () => {
    render(<UiRenderer surface={surface({
      root: { id: 'root', component: 'TextField' }, // no label, no value/path
    })} />);
    const input = screen.getByRole('textbox');
    fireEvent.change(input, { target: { value: 'x' } }); // path === '' → setData skipped
    expect(input).toBeInTheDocument();
  });

  it('Card without a title, and a List with a single child (no top border)', () => {
    render(<UiRenderer surface={surface({
      root: { id: 'root', component: 'Column', children: ['card', 'list'] },
      card: { id: 'card', component: 'Card', children: ['ct'] }, // no title
      ct: { id: 'ct', component: 'Text', text: 'body' },
      list: { id: 'list', component: 'List', children: ['only'] }, // single child
      only: { id: 'only', component: 'Text', text: 'one' },
    })} />);
    expect(screen.getByText('one')).toBeInTheDocument();
  });

  it('Badge with an unknown tone falls back to neutral and empty text', () => {
    render(<UiRenderer surface={surface({
      root: { id: 'root', component: 'Badge', tone: 'mystery' }, // unknown tone, no text
    })} />);
    expect(document.querySelector('span')).toBeInTheDocument();
  });

  it('CheckBox reflects a bound true value', () => {
    render(<UiRenderer surface={surface({
      root: { id: 'root', component: 'CheckBox', value: { path: '/on' } }, // no label
    }, 'root', { on: true })} />);
    expect(screen.getByRole('checkbox')).toBeChecked();
  });

  it('ChoicePicker derives label/value from partial option objects and reflects selection', () => {
    render(<UiRenderer surface={surface({
      root: { id: 'root', component: 'ChoicePicker', value: { path: '/pick' }, options: [
        { value: 'v-only' },        // label derives from value
        { label: 'l-only' },        // value derives from label
      ] },
    }, 'root', { pick: 'v-only' })} />);
    const radios = screen.getAllByRole('radio');
    expect(radios[0]).toBeChecked(); // selected === 'v-only'
    expect(screen.getByText('v-only')).toBeInTheDocument();
    expect(screen.getByText('l-only')).toBeInTheDocument();
  });

  it('Stat with an unknown tone and an empty-string delta hides the delta', () => {
    render(<UiRenderer surface={surface({
      root: { id: 'root', component: 'Stat', label: 'L', value: 'V', tone: 'xx', delta: '' },
    })} />);
    expect(screen.getByText('V')).toBeInTheDocument();
  });

  it('Chart defaults to a bar type, drops non-finite values, and renders nothing for unresolved bound data', () => {
    const { container } = render(<UiRenderer surface={surface({
      root: { id: 'root', component: 'Column', children: ['c1', 'c2'] },
      c1: { id: 'c1', component: 'Chart', data: [{ label: 'ok', value: 5 }, { label: 'bad', value: 'NaNy' }] }, // no chartType → bar; NaN filtered
      c2: { id: 'c2', component: 'Chart', chartType: 'bar', data: { path: '/missing' } }, // unresolved → []
    })} />);
    expect(container.querySelectorAll('svg').length).toBe(1); // only c1 has a finite point
  });

  it('Quiz resolves an object answer to no-match, reads the "prompt" alias, and drops optionless questions', () => {
    render(<UiRenderer surface={surface({
      root: { id: 'root', component: 'Quiz', questions: [
        { prompt: 'Aliased?', options: ['a', 'b'], answer: {}, explanation: 'because' }, // object answer → -1
        { question: 'no options here' }, // dropped
      ] },
    })} />);
    expect(screen.getByText('Aliased?')).toBeInTheDocument();
    clickButton('A. a');
    fireEvent.click(screen.getByText('Submit Quiz'));
    expect(screen.getByText('0 / 1')).toBeInTheDocument(); // object answer never matches
  });

  it('guards against missing children and cyclic references, and renders nothing for an unknown component', () => {
    const { container } = render(<UiRenderer surface={surface({
      root: { id: 'root', component: 'Column', children: ['ghost', 'root', 'weird'] }, // missing + self-cycle
      weird: { id: 'weird', component: 'Bogus' }, // unknown → default null
    })} />);
    expect(container.querySelector('div')).toBeInTheDocument(); // rendered without throwing
  });

  it('returns nothing when the root id is absent from the component map', () => {
    const { container } = render(<UiRenderer surface={surface({}, 'nope')} />);
    expect(container.querySelector('input')).not.toBeInTheDocument();
  });

  it('writes a bound value into an existing nested object and ignores a root-only path', () => {
    const { rerender } = render(<UiRenderer surface={surface({
      root: { id: 'root', component: 'TextField', value: { path: '/a/b' } },
    }, 'root', { a: { keep: 1 } })} />);
    fireEvent.change(screen.getByRole('textbox'), { target: { value: 'deep' } }); // spreads existing a
    expect(screen.getByDisplayValue('deep')).toBeInTheDocument();
    // a value bound to just "/" yields an empty segment list → setData is a no-op
    rerender(<UiRenderer surface={surface({
      root: { id: 'root', component: 'TextField', value: { path: '/' } },
    })} />);
  });
});

describe('UiRenderer — exhaustive fallbacks', () => {
  it('pie chart with all-zero values, weighted chart, label-less point and label-less Image', () => {
    const { container } = render(<UiRenderer surface={surface({
      root: { id: 'root', component: 'Column', children: ['pie', 'wchart', 'img'] },
      pie: { id: 'pie', component: 'Chart', chartType: 'pie', data: [{ label: 'A', value: 0 }, { label: 'B', value: 0 }] }, // total || 1
      wchart: { id: 'wchart', component: 'Chart', chartType: 'bar', weight: 2, data: [{ value: 4 }] }, // weighted + missing label
      img: { id: 'img', component: 'Image', url: 'https://example.com/y.png' }, // no alt → ''
    })} />);
    expect(container.querySelectorAll('svg').length).toBe(2);
    expect(container.querySelector('img')).toHaveAttribute('alt', '');
  });

  it('empty-children containers (List, Dashboard, Slide, Slides) and a missing-options ChoicePicker', () => {
    const { container } = render(<UiRenderer surface={surface({
      root: { id: 'root', component: 'Column', children: ['list', 'dash', 'slide', 'slides', 'cp', 'stat'] },
      list: { id: 'list', component: 'List' },           // no children
      dash: { id: 'dash', component: 'Dashboard' },       // no children
      slide: { id: 'slide', component: 'Slide' },         // no title, no children
      slides: { id: 'slides', component: 'Slides' },      // no children → SlidesNode([])
      cp: { id: 'cp', component: 'ChoicePicker', options: [{}] }, // option with neither label nor value → ''
      stat: { id: 'stat', component: 'Stat' },            // no label / value → ''
    })} />);
    expect(container).toBeTruthy();
    expect(screen.getAllByRole('radio').length).toBe(1);
  });

  it('Table with a primitive row and a null cell', () => {
    render(<UiRenderer surface={surface({
      root: { id: 'root', component: 'Table', columns: ['C'], rows: [42, [null]] },
    })} />);
    expect(screen.getByText('42')).toBeInTheDocument();
  });

  it('Quiz reads questions from bound data, keeps an explanation, and shows "—" plus the correct answer for unanswered/out-of-range questions', () => {
    // questions bound to a path that does NOT resolve → falls back to node.questions
    render(<UiRenderer surface={surface({
      root: { id: 'root', component: 'Quiz', questions: { path: '/missing' } },
    })} />);
    expect(screen.queryByText('Submit Quiz')).not.toBeInTheDocument(); // empty → renders nothing

    // a real quiz: answer Q0 wrong (valid correct option shown), leave Q1 unanswered (answer out of range → "—")
    render(<UiRenderer surface={surface({
      root: { id: 'root', component: 'Quiz', questions: [
        { question: 'Q1', options: ['a', 'b'], answer: 1, explanation: 'note' },
        { question: 'Q2', options: ['c', 'd'], answer: 9 }, // out-of-range correct index
      ] },
    })} />);
    clickButton('A. a'); // Q1 wrong (correct is b)
    fireEvent.click(screen.getByText('Next')); // advance to Q2, leave it unanswered
    fireEvent.click(screen.getByText('Submit Quiz'));
    expect(screen.getByText(/Your answer: a/)).toBeInTheDocument();
    expect(screen.getByText(/correct: b/)).toBeInTheDocument(); // valid correct option
    expect(screen.getByText(/Your answer: —/)).toBeInTheDocument(); // unanswered Q2
  });

  it('childless Card, optionless ChoicePicker, rowless Table, and Quiz with a null / title-less entry', () => {
    render(<UiRenderer surface={surface({
      root: { id: 'root', component: 'Column', children: ['card', 'cp', 'table', 'quiz'] },
      card: { id: 'card', component: 'Card', title: 'Empty' },     // no children → []
      cp: { id: 'cp', component: 'ChoicePicker', label: 'Pick' },  // no options → []
      table: { id: 'table', component: 'Table', columns: ['C'] },  // no rows → []
      quiz: { id: 'quiz', component: 'Quiz', questions: [
        null,                              // falsy entry → {}
        { options: ['a'] },                // no question/prompt → '' → filtered out
        { question: 'Real', options: ['a', 'b'], answer: 0 },
      ] },
    })} />);
    expect(screen.getByText('Empty')).toBeInTheDocument();
    expect(screen.getByText('Real')).toBeInTheDocument();
  });

  it('falls back to an empty object when surface.data is undefined, and ignores a root-only "/" write', () => {
    const noData = { rootId: 'root', components: { root: { id: 'root', component: 'TextField', value: { path: '/' } } } as never, data: undefined as never } as UiSurface;
    render(<UiRenderer surface={noData} />);
    fireEvent.change(screen.getByRole('textbox'), { target: { value: 'ignored' } }); // path "/" → no segments → no-op
    expect(screen.getByRole('textbox')).toBeInTheDocument();
  });
});

describe('UiRenderer — Slides deck', () => {
  const deck = (n: number) => {
    const components: Record<string, unknown> = {
      root: { id: 'root', component: 'Slides', children: Array.from({ length: n }, (_, i) => `s${i}`) },
    };
    for (let i = 0; i < n; i++) {
      components[`s${i}`] = { id: `s${i}`, component: 'Slide', title: `Slide ${i}`, children: [`t${i}`] };
      components[`t${i}`] = { id: `t${i}`, component: 'Text', text: `body ${i}` };
    }
    return surface(components);
  };

  it('shows the first slide, navigates via dots and arrow keys', () => {
    render(<UiRenderer surface={deck(3)} />);
    expect(screen.getByText('Slide 0')).toBeInTheDocument();
    // dots are buttons labelled "Go to slide N"
    fireEvent.click(screen.getByLabelText('Go to slide 3'));
    expect(screen.getByText('Slide 2')).toBeInTheDocument();
    fireEvent.keyDown(window, { key: 'ArrowLeft' });
    expect(screen.getByText('Slide 1')).toBeInTheDocument();
    fireEvent.keyDown(window, { key: 'ArrowRight' });
    expect(screen.getByText('Slide 2')).toBeInTheDocument();
    fireEvent.keyDown(window, { key: 'a' }); // ignored
  });

  it('renders nothing for an empty deck and skips keyboard for a single slide', () => {
    const { container } = render(<UiRenderer surface={surface({ root: { id: 'root', component: 'Slides', children: [] } })} />);
    expect(container.textContent).toBe('');
    render(<UiRenderer surface={deck(1)} />);
    expect(screen.getByText('Slide 0')).toBeInTheDocument();
    fireEvent.keyDown(window, { key: 'ArrowRight' }); // single slide → no-op
  });
});

describe('UiRenderer — Quiz', () => {
  const quiz = (questions: unknown[], title = 'Quiz') =>
    surface({ root: { id: 'root', component: 'Quiz', title, questions } });

  it('renders a question, updates score on selection, navigates and submits to a passing breakdown', () => {
    render(<UiRenderer surface={quiz([
      { question: 'Q1', options: ['a', 'b'], answer: 0 },
      { question: 'Q2', options: ['c', 'd'], answer: 1 },
    ])} />);
    expect(screen.getByText('Q1')).toBeInTheDocument();
    clickButton('A. a'); // correct
    fireEvent.click(screen.getByText('Next'));
    expect(screen.getByText('Q2')).toBeInTheDocument();
    clickButton('B. d'); // correct
    fireEvent.click(screen.getByText('Submit Quiz'));
    expect(screen.getByText('Final Score')).toBeInTheDocument();
    expect(screen.getByText('2 / 2')).toBeInTheDocument();
    expect(screen.getByText('100%')).toBeInTheDocument();
    // Previous works after navigation, and Retake resets
    fireEvent.click(screen.getByText('Retake quiz'));
    expect(screen.getByText('Q1')).toBeInTheDocument();
    clickButton('A. a');
    fireEvent.click(screen.getByText('Next'));
    fireEvent.click(screen.getByText('Previous'));
    expect(screen.getByText('Q1')).toBeInTheDocument();
  });

  it('scores a partial (warn) result and resolves answers given as text', () => {
    render(<UiRenderer surface={quiz([
      { question: 'Q1', options: ['a', 'b'], answer: 'a' }, // answer by text
      { question: 'Q2', options: ['c', 'd'], answer: 0 },
    ])} />);
    clickButton('A. a'); // correct
    fireEvent.click(screen.getByText('Next'));
    clickButton('B. d'); // wrong (correct is c)
    fireEvent.click(screen.getByText('Submit Quiz'));
    expect(screen.getByText('1 / 2')).toBeInTheDocument();
    expect(screen.getByText('50%')).toBeInTheDocument();
  });

  it('scores a failing (bad) result and resolves a numeric-string answer', () => {
    render(<UiRenderer surface={quiz([
      { question: 'Q1', options: ['a', 'b'], answer: '1' }, // numeric string → index 1
    ])} />);
    clickButton('A. a'); // wrong (correct is index 1)
    fireEvent.click(screen.getByText('Submit Quiz'));
    expect(screen.getByText('0 / 1')).toBeInTheDocument();
    expect(screen.getByText('0%')).toBeInTheDocument();
  });

  it('keeps Submit disabled until an answer is chosen, and drops malformed questions / empty quiz', () => {
    const { rerender, container } = render(<UiRenderer surface={quiz([
      { question: 'Only Q', options: ['x', 'y'], answer: 'not-an-option' }, // unresolvable answer
    ])} />);
    const submit = screen.getByText('Submit Quiz');
    expect(submit).toBeDisabled();
    clickButton('B. y');
    fireEvent.click(submit);
    expect(screen.getByText('0 / 1')).toBeInTheDocument(); // unresolved answer → never correct
    // malformed (no options) is filtered out → empty quiz renders nothing
    rerender(<UiRenderer surface={quiz([{ question: 'no options' }])} />);
    expect(container.textContent).toBe('');
  });
});

describe('UiRenderer — surface theme', () => {
  const themed = (theme: object, comps?: Record<string, unknown>): UiSurface =>
    ({
      ...surface(comps || { root: { id: 'root', component: 'Text', text: 'Hi', variant: 'h1' } }),
      theme,
    }) as UiSurface;

  it('applies a full theme as CSS vars, font and compact padding on the stage', () => {
    const { container } = render(
      <UiRenderer
        surface={themed({
          accent: '#ff0000', background: '#101010', surface: '#202020',
          text: '#fefefe', heading: '#00ff00', muted: '#888888',
          font: 'serif', density: 'compact',
        })}
      />,
    );
    const styleAttr = (container.firstChild as HTMLElement).getAttribute('style') || '';
    expect(styleAttr).toContain('--ui-accent: #ff0000');
    expect(styleAttr).toContain('--ui-stage: #101010');
    expect(styleAttr).toContain('--ui-surface: #202020');
    expect(styleAttr).toContain('--ui-surface-strong: #202020');
    expect(styleAttr).toContain('--ui-text: #fefefe');
    expect(styleAttr).toContain('--ui-heading: #00ff00');
    expect(styleAttr).toContain('--ui-muted: #888888');
    expect(styleAttr).toContain('--ui-border: rgba(128,128,128,0.30)');
    expect(styleAttr).toContain('Georgia');
    expect(styleAttr).toContain('padding: 22px 30px');
  });

  it('renders un-themed with no css vars and the default font/padding', () => {
    const { container } = render(
      <UiRenderer surface={surface({ root: { id: 'root', component: 'Text', text: 'Hi' } })} />,
    );
    const styleAttr = (container.firstChild as HTMLElement).getAttribute('style') || '';
    // No custom-property *declarations* (var() *references* like `var(--ui-stage,…)`
    // still appear in background/color — those carry the built-in fallbacks).
    expect(styleAttr).not.toMatch(/--ui-[\w-]+:/);
    expect(styleAttr).toContain('Inter');
    expect(styleAttr).toContain('padding: 36px 48px');
  });

  it('sets only the accent var (no border) and falls back on an unknown font', () => {
    const { container } = render(<UiRenderer surface={themed({ accent: '#abcabc', font: 'bogus' })} />);
    const styleAttr = (container.firstChild as HTMLElement).getAttribute('style') || '';
    expect(styleAttr).toContain('--ui-accent: #abcabc');
    expect(styleAttr).not.toMatch(/--ui-border:/);
    expect(styleAttr).not.toMatch(/--ui-stage:/);
    expect(styleAttr).toContain('Inter'); // unknown font → sans fallback
  });

  it('sets the border var when only the background is themed', () => {
    const { container } = render(<UiRenderer surface={themed({ background: '#ffffff' })} />);
    const styleAttr = (container.firstChild as HTMLElement).getAttribute('style') || '';
    expect(styleAttr).toContain('--ui-stage: #ffffff');
    expect(styleAttr).toContain('--ui-border: rgba(128,128,128,0.30)');
  });

  it('body text inherits the themed text color (no hardcoded light color)', () => {
    render(<UiRenderer surface={surface({ root: { id: 'root', component: 'Text', text: 'Body copy', variant: 'body' } })} />);
    const style = screen.getByText('Body copy').getAttribute('style') || '';
    // Must NOT carry the old dark-stage literal that overrode light themes.
    expect(style).not.toContain('#dbe3ff');
    // Resolves through the theme token so a light theme makes it dark/readable.
    expect(style).toContain('var(--ui-text');
  });

  it('Table body cells use the themed text color, not a hardcoded light color', () => {
    // Regression: cells were hardcoded #dbe3ff (a dark-theme light color), so on a
    // light theme they washed out to near-invisible. They must follow --ui-text.
    const { container } = render(<UiRenderer surface={surface({
      root: { id: 'root', component: 'Table', columns: ['ZIP'], rows: [['1123']] },
    })} />);
    const td = container.querySelector('tbody td') as HTMLElement;
    const style = td.getAttribute('style') || '';
    expect(style).not.toContain('#dbe3ff');
    expect(style).toContain('var(--ui-text');
  });

  it('hyperlinks the first column to its row source url (http only, new tab)', () => {
    const { container } = render(<UiRenderer surface={surface({
      root: { id: 'root', component: 'Table', columns: ['Title', 'Page Age'], rows: [{ Title: 'Swiss News', 'Page Age': '2026-06-22' }], links: ['https://swissinfo.ch/x'] },
    })} />);
    const a = container.querySelector('tbody a') as HTMLAnchorElement;
    expect(a).not.toBeNull();
    expect(a.getAttribute('href')).toBe('https://swissinfo.ch/x');
    expect(a.getAttribute('target')).toBe('_blank');
    expect(a.getAttribute('rel') || '').toContain('noopener');
    expect(a.textContent).toBe('Swiss News');
  });

  it('does not hyperlink a non-http row link (no javascript: urls)', () => {
    const { container } = render(<UiRenderer surface={surface({
      root: { id: 'root', component: 'Table', columns: ['Title'], rows: [{ Title: 'X' }], links: ['javascript:alert(1)'] },
    })} />);
    expect(container.querySelector('tbody a')).toBeNull();
  });

  it('sorts rows numerically when a column header is clicked (asc → desc → off)', () => {
    const { container } = render(<UiRenderer surface={surface({
      root: { id: 'root', component: 'Table', columns: ['Name', 'Score'], rows: [{ Name: 'A', Score: 30 }, { Name: 'B', Score: 10 }, { Name: 'C', Score: 20 }] },
    })} />);
    const scoreHeader = container.querySelectorAll('thead th')[1] as HTMLElement;
    const scores = () => Array.from(container.querySelectorAll('tbody tr')).map((tr) => tr.querySelectorAll('td')[1].textContent);
    expect(scores()).toEqual(['30', '10', '20']); // original order
    fireEvent.click(scoreHeader);
    expect(scores()).toEqual(['10', '20', '30']); // ascending (numeric, not lexicographic)
    fireEvent.click(scoreHeader);
    expect(scores()).toEqual(['30', '20', '10']); // descending
    fireEvent.click(scoreHeader);
    expect(scores()).toEqual(['30', '10', '20']); // back to original
  });
});

describe('UiRenderer — Album', () => {
  const album = (extra: Record<string, unknown>): UiSurface =>
    surface({ root: { id: 'root', component: 'Album', ...extra } });

  it('renders a grid of images (string + object urls, captions, source links)', () => {
    const { container } = render(
      <UiRenderer
        surface={album({
          title: 'Trip',
          images: [
            { url: 'https://ex.com/a.jpg', caption: 'A', alt: 'alt-a' },
            'https://ex.com/b.jpg', // plain string
            { src: 'https://ex.com/c.jpg' }, // src alias
            { link: 'https://ex.com/d.jpg', title: 'D' }, // link alias + title→caption
            { nope: 1 }, // no url → filtered out
          ],
        })}
      />,
    );
    const imgs = Array.from(container.querySelectorAll('img'));
    expect(imgs.map((i) => i.getAttribute('src'))).toEqual([
      'https://ex.com/a.jpg',
      'https://ex.com/b.jpg',
      'https://ex.com/c.jpg',
      'https://ex.com/d.jpg',
    ]);
    expect(screen.getByText('Trip')).toBeInTheDocument();
    expect(screen.getByText('A')).toBeInTheDocument();
    const wrap = imgs[0].closest('a')!.parentElement as HTMLElement;
    expect(wrap.style.display).toBe('grid'); // grid, not a scroller
  });

  it('carousel: one image per screen, navigable by ←/→ keys, buttons and dots', () => {
    const { container } = render(
      <UiRenderer
        surface={album({
          layout: 'carousel',
          title: 'Alps',
          images: [
            { url: 'https://ex.com/1.jpg', caption: 'First' },
            { url: 'https://ex.com/2.jpg', caption: 'Second' },
            { url: 'https://ex.com/3.jpg' }, // no caption → caption branch false
          ],
        })}
      />,
    );
    // one image at a time, with title + caption + counter
    expect(container.querySelectorAll('img')).toHaveLength(1);
    expect(screen.getByText('Alps')).toBeInTheDocument();
    expect(screen.getByText('First')).toBeInTheDocument();
    expect(screen.getByText('1 / 3')).toBeInTheDocument();
    // prev disabled at the start, 3 dots
    expect(screen.getByLabelText('Previous image')).toBeDisabled();
    expect(screen.getByLabelText('Next image')).not.toBeDisabled();
    expect(screen.getAllByLabelText(/Go to image/)).toHaveLength(3);

    // → advances; ← goes back; a non-arrow key is ignored
    fireEvent.keyDown(window, { key: 'ArrowRight' });
    expect(screen.getByText('2 / 3')).toBeInTheDocument();
    expect(screen.getByText('Second')).toBeInTheDocument();
    fireEvent.keyDown(window, { key: 'ArrowLeft' });
    expect(screen.getByText('1 / 3')).toBeInTheDocument();
    fireEvent.keyDown(window, { key: 'a' });
    expect(screen.getByText('1 / 3')).toBeInTheDocument();

    // Next button, then jump to the last image via a dot → Next disabled at the end
    fireEvent.click(screen.getByLabelText('Next image'));
    expect(screen.getByText('2 / 3')).toBeInTheDocument();
    fireEvent.click(screen.getAllByLabelText(/Go to image/)[2]);
    expect(screen.getByText('3 / 3')).toBeInTheDocument();
    expect(screen.getByLabelText('Next image')).toBeDisabled();
    fireEvent.click(screen.getByLabelText('Previous image'));
    expect(screen.getByText('2 / 3')).toBeInTheDocument();
  });

  it('carousel with a single image hides all navigation', () => {
    render(<UiRenderer surface={album({ layout: 'carousel', urls: ['https://ex.com/only.jpg'] })} />);
    expect(screen.queryByLabelText('Previous image')).toBeNull();
    expect(screen.queryByLabelText('Next image')).toBeNull();
    expect(screen.queryByLabelText(/Go to image/)).toBeNull();
    expect(screen.queryByText('1 / 1')).toBeNull();
  });

  it('treats layout "slideshow" as a navigable carousel', () => {
    render(<UiRenderer surface={album({ layout: 'slideshow', images: [{ url: 'https://ex.com/1.jpg' }, { url: 'https://ex.com/2.jpg' }] })} />);
    expect(screen.getByText('1 / 2')).toBeInTheDocument();
    expect(screen.getByLabelText('Next image')).toBeInTheDocument();
  });

  it('renders a grid without a title', () => {
    render(<UiRenderer surface={album({ images: [{ url: 'https://ex.com/x.jpg', caption: 'cap' }] })} />);
    expect(screen.getByText('cap')).toBeInTheDocument();
    expect(screen.queryByText('1 / 1')).toBeNull(); // grid, not a carousel
  });

  it('renders nothing when there are no usable images', () => {
    const { container, rerender } = render(<UiRenderer surface={album({ images: [{ nope: 1 }, 5] })} />);
    expect(container.querySelector('img')).toBeNull();
    // neither images nor urls present
    rerender(<UiRenderer surface={album({})} />);
    expect(container.querySelector('img')).toBeNull();
  });
});

describe('UiRenderer — Mindmap', () => {
  // A tree that exercises every layout branch:
  //  - root (depth 0) with 4 usable children → first / middle / last connectors
  //  - Branch A → single child (only-child connector), whose child A1 is at depth 2
  //    (collapsed by default) and itself has a child A1a
  //  - Branch B → leaf, supplied via `text` (not `label`)
  //  - Branch C → two children with a null mixed in (filtered out)
  //  - an empty-label branch (toggle aria falls back to "node")
  //  - a null and a string child on root (both filtered out by mindmapChildren)
  const tree = {
    label: 'Center',
    children: [
      { label: 'Branch A', children: [{ label: 'A1', children: [{ label: 'A1a' }] }] },
      { text: 'Branch B' },
      // `{}` is a usable object node with neither label nor text → renders empty
      { label: 'Branch C', children: [{ label: 'C1' }, null, { label: 'C2' }, {}] },
      { label: '', children: [{ label: 'E1' }] },
      null,
      'junk',
    ],
  };
  const mindmap = (extra: Record<string, unknown> = {}) =>
    surface({ root: { id: 'root', component: 'Mindmap', ...extra } });

  it('renders the title, the colourful tree, and collapses levels below depth 1 by default', () => {
    render(<UiRenderer surface={mindmap({ title: 'My Map', root: tree })} />);
    expect(screen.getByText('My Map')).toBeInTheDocument(); // title
    expect(screen.getByText('Center')).toBeInTheDocument(); // root
    expect(screen.getByText('Branch A')).toBeInTheDocument();
    expect(screen.getByText('Branch B')).toBeInTheDocument(); // supplied via `text`
    expect(screen.getByText('Branch C')).toBeInTheDocument();
    expect(screen.getByText('A1')).toBeInTheDocument(); // depth 2 node shows (parent expanded)
    expect(screen.getByText('C1')).toBeInTheDocument();
    expect(screen.getByText('C2')).toBeInTheDocument();
    expect(screen.getByText('E1')).toBeInTheDocument();
    // A1 is at depth 2 → its OWN children start collapsed
    expect(screen.queryByText('A1a')).toBeNull();
    expect(screen.getByLabelText('Expand A1')).toHaveTextContent('+1'); // collapsed → count badge
    // toggles present for nodes-with-children; an empty label falls back to "node"
    expect(screen.getByLabelText('Collapse Center')).toBeInTheDocument();
    expect(screen.getByLabelText('Collapse Branch A')).toBeInTheDocument();
    expect(screen.getByLabelText('Collapse node')).toBeInTheDocument();
    // a leaf has no toggle
    expect(screen.queryByLabelText('Collapse Branch B')).toBeNull();
    expect(screen.queryByLabelText('Expand Branch B')).toBeNull();
  });

  it('expands a deep node to reveal its hidden children', () => {
    render(<UiRenderer surface={mindmap({ title: 'My Map', root: tree })} />);
    fireEvent.click(screen.getByLabelText('Expand A1'));
    expect(screen.getByText('A1a')).toBeInTheDocument();
    expect(screen.getByLabelText('Collapse A1')).toBeInTheDocument(); // now expanded
  });

  it('collapsing the root hides the whole tree (and shows the hidden-child count)', () => {
    render(<UiRenderer surface={mindmap({ title: 'My Map', root: tree })} />);
    fireEvent.click(screen.getByLabelText('Collapse Center'));
    expect(screen.queryByText('Branch A')).toBeNull();
    expect(screen.queryByText('Branch C')).toBeNull();
    expect(screen.getByText('Center')).toBeInTheDocument(); // root itself stays
    expect(screen.getByLabelText('Expand Center')).toHaveTextContent('+4'); // 4 usable children
    fireEvent.click(screen.getByLabelText('Expand Center'));
    expect(screen.getByText('Branch A')).toBeInTheDocument();
  });

  it('reads the tree from `data` or `tree` and renders without a title', () => {
    const { rerender } = render(<UiRenderer surface={mindmap({ data: { label: 'FromData' } })} />);
    expect(screen.getByText('FromData')).toBeInTheDocument();
    rerender(<UiRenderer surface={mindmap({ tree: { label: 'FromTree' } })} />);
    expect(screen.getByText('FromTree')).toBeInTheDocument();
  });

  it('renders nothing when the tree is missing or not an object', () => {
    const { container, rerender } = render(<UiRenderer surface={mindmap()} />); // no root/data/tree
    expect(container.querySelector('button')).toBeNull();
    rerender(<UiRenderer surface={mindmap({ root: 'not-an-object' })} />);
    expect(container.querySelector('button')).toBeNull();
  });
});

describe('UiRenderer — Mindmap canvas interactions (pan + drag)', () => {
  // Root → A (→ A1 leaf) + B leaf. Root & A have children (toggles); A1/B are leaves.
  const tree = { label: 'Root', children: [{ label: 'A', children: [{ label: 'A1' }] }, { label: 'B' }] };
  const mindmap = () => surface({ root: { id: 'root', component: 'Mindmap', root: tree } });
  const px = (v: string) => parseFloat(v || '0');
  const node = (c: HTMLElement, id: string) => c.querySelector(`[data-mm-node="${id}"]`) as HTMLElement;
  const canvasEl = (c: HTMLElement) => c.querySelector('[data-mm-canvas]') as HTMLElement;
  const worldEl = (c: HTMLElement) => c.querySelector('[data-mm-world]') as HTMLElement;

  it('draws one curved edge per visible non-root node', () => {
    const { container } = render(<UiRenderer surface={mindmap()} />);
    const nodeCount = container.querySelectorAll('[data-mm-node]').length;
    expect(nodeCount).toBe(4); // Root, A, A1, B
    expect(container.querySelectorAll('path').length).toBe(nodeCount - 1); // edges = nodes − root
  });

  it('drags a node and carries its whole subtree, toggling the grab cursor', () => {
    const { container } = render(<UiRenderer surface={mindmap()} />);
    const root = node(container, 'r');
    const childA = node(container, 'r.0');
    const rootLeft0 = px(root.style.left);
    const rootTop0 = px(root.style.top);
    const aLeft0 = px(childA.style.left);

    fireEvent.pointerDown(root, { clientX: 200, clientY: 200 });
    expect(canvasEl(container).style.cursor).toBe('grabbing');
    fireEvent.pointerMove(canvasEl(container), { clientX: 260, clientY: 250 }); // delta (60, 50)
    fireEvent.pointerUp(canvasEl(container));
    expect(canvasEl(container).style.cursor).toBe('grab');

    expect(px(node(container, 'r').style.left)).toBe(rootLeft0 + 60);
    expect(px(node(container, 'r').style.top)).toBe(rootTop0 + 50);
    // subtree moved with the root
    expect(px(node(container, 'r.0').style.left)).toBe(aLeft0 + 60);
  });

  it('dragging one node leaves unrelated nodes where they were', () => {
    const { container } = render(<UiRenderer surface={mindmap()} />);
    const bLeft0 = px(node(container, 'r.1').style.left); // sibling B
    const childA = node(container, 'r.0');
    fireEvent.pointerDown(childA, { clientX: 0, clientY: 0 });
    fireEvent.pointerMove(canvasEl(container), { clientX: 25, clientY: 0 });
    fireEvent.pointerUp(canvasEl(container));
    expect(px(node(container, 'r.0').style.left)).toBe(px(childA.style.left)); // A unchanged ref reads post-move value
    expect(px(node(container, 'r.1').style.left)).toBe(bLeft0); // B untouched
  });

  // The initial view centers the ROOT node in the viewport (jsdom reports a
  // 0×0 canvas, so the centered pan is exactly the negated root position).
  const centeredTransform = (c: HTMLElement) => {
    const root = node(c, 'r');
    return `translate(${-px(root.style.left)}px, ${-px(root.style.top)}px) scale(1)`;
  };

  it('opens centered on the root node', () => {
    const { container } = render(<UiRenderer surface={mindmap()} />);
    expect(worldEl(container).style.transform).toBe(centeredTransform(container));
  });

  it('pans the whole canvas when dragging empty space', () => {
    const { container } = render(<UiRenderer surface={mindmap()} />);
    const t0 = centeredTransform(container);
    expect(worldEl(container).style.transform).toBe(t0);
    fireEvent.pointerDown(canvasEl(container), { clientX: 10, clientY: 10 });
    fireEvent.pointerMove(canvasEl(container), { clientX: 40, clientY: 22 }); // delta (30, 12)
    fireEvent.pointerUp(canvasEl(container));
    const m = t0.match(/translate\((-?[\d.]+)px, (-?[\d.]+)px\)/) as RegExpMatchArray;
    expect(worldEl(container).style.transform).toBe(
      `translate(${parseFloat(m[1]) + 30}px, ${parseFloat(m[2]) + 12}px) scale(1)`,
    );
  });

  it('ignores pointer moves when nothing is being dragged', () => {
    const { container } = render(<UiRenderer surface={mindmap()} />);
    fireEvent.pointerMove(canvasEl(container), { clientX: 99, clientY: 99 });
    expect(worldEl(container).style.transform).toBe(centeredTransform(container)); // unchanged
  });

  it('pressing a node toggle does not start a drag (stops propagation)', () => {
    const { container } = render(<UiRenderer surface={mindmap()} />);
    fireEvent.pointerDown(screen.getByLabelText('Collapse A'));
    expect(canvasEl(container).style.cursor).toBe('grab'); // no drag started
  });

  const scaleOf = (t: string) => parseFloat((t.match(/scale\(([^)]+)\)/) || [])[1] || '1');

  it('zooms with the mouse wheel (in on scroll-up, out on scroll-down)', () => {
    const { container } = render(<UiRenderer surface={mindmap()} />);
    fireEvent.wheel(canvasEl(container), { deltaY: -100, clientX: 0, clientY: 0 });
    expect(scaleOf(worldEl(container).style.transform)).toBeGreaterThan(1);
    fireEvent.wheel(canvasEl(container), { deltaY: 100, clientX: 0, clientY: 0 });
    fireEvent.wheel(canvasEl(container), { deltaY: 100, clientX: 0, clientY: 0 });
    expect(scaleOf(worldEl(container).style.transform)).toBeLessThan(1);
  });

  it('zooms via the +/− buttons and restores with reset (re-centered on the root)', () => {
    const { container } = render(<UiRenderer surface={mindmap()} />);
    // pressing a zoom button must not start a canvas pan
    fireEvent.pointerDown(screen.getByLabelText('Zoom in'));
    expect(canvasEl(container).style.cursor).toBe('grab');
    fireEvent.click(screen.getByLabelText('Zoom in'));
    expect(scaleOf(worldEl(container).style.transform)).toBeGreaterThan(1);
    fireEvent.click(screen.getByLabelText('Reset view'));
    expect(worldEl(container).style.transform).toBe(centeredTransform(container));
    fireEvent.click(screen.getByLabelText('Zoom out'));
    expect(scaleOf(worldEl(container).style.transform)).toBeLessThan(1);
  });

  it('clamps zoom to the min/max bounds', () => {
    const { container } = render(<UiRenderer surface={mindmap()} />);
    for (let i = 0; i < 15; i += 1) fireEvent.click(screen.getByLabelText('Zoom out'));
    expect(scaleOf(worldEl(container).style.transform)).toBe(0.3); // MM_MIN_ZOOM
    for (let i = 0; i < 25; i += 1) fireEvent.click(screen.getByLabelText('Zoom in'));
    expect(scaleOf(worldEl(container).style.transform)).toBe(2.5); // MM_MAX_ZOOM
  });

  it('detaches the wheel listener on unmount without error', () => {
    const { unmount } = render(<UiRenderer surface={mindmap()} />);
    expect(() => unmount()).not.toThrow(); // ref callback runs with null → cleanup
  });
});

describe('UiRenderer — Mindmap bilateral layout', () => {
  const mindmap = (tree: Record<string, unknown>) =>
    surface({ root: { id: 'root', component: 'Mindmap', root: tree } });
  const px = (v: string) => parseFloat(v || '0');
  const node = (c: HTMLElement, id: string) => c.querySelector(`[data-mm-node="${id}"]`) as HTMLElement;

  it('splits top-level branches symmetrically left and right of the central node', () => {
    const { container } = render(
      <UiRenderer
        surface={mindmap({
          label: 'Center',
          children: [{ label: 'B1' }, { label: 'B2' }, { label: 'B3' }, { label: 'B4' }],
        })}
      />,
    );
    const rootX = px(node(container, 'r').style.left);
    // Greedy balance, ties to the right: B1/B3 right, B2/B4 left…
    expect(px(node(container, 'r.0').style.left)).toBeGreaterThan(rootX);
    expect(px(node(container, 'r.2').style.left)).toBeGreaterThan(rootX);
    expect(px(node(container, 'r.1').style.left)).toBeLessThan(rootX);
    expect(px(node(container, 'r.3').style.left)).toBeLessThan(rootX);
    // …at the same distance on both sides.
    expect(rootX - px(node(container, 'r.1').style.left)).toBe(px(node(container, 'r.0').style.left) - rootX);
    // Both sides share the same rows and the root sits on the vertical midline.
    expect(px(node(container, 'r.0').style.top)).toBe(px(node(container, 'r.1').style.top));
    const topRow = px(node(container, 'r.0').style.top);
    const bottomRow = px(node(container, 'r.2').style.top);
    expect(px(node(container, 'r').style.top)).toBe((topRow + bottomRow) / 2);
  });

  it('keeps a single branch on the right of the root (no lonely left side)', () => {
    const { container } = render(
      <UiRenderer surface={mindmap({ label: 'C', children: [{ label: 'Only' }] })} />,
    );
    expect(px(node(container, 'r.0').style.left)).toBeGreaterThan(px(node(container, 'r').style.left));
  });

  it('grows deeper levels outward on their own side', () => {
    // B1 (leaf) → right; B2 (subtree) → left; B2's child must be FURTHER left.
    const { container } = render(
      <UiRenderer
        surface={mindmap({
          label: 'C',
          children: [{ label: 'B1' }, { label: 'B2', children: [{ label: 'B2a' }] }],
        })}
      />,
    );
    const rootX = px(node(container, 'r').style.left);
    const b2 = px(node(container, 'r.1').style.left);
    expect(b2).toBeLessThan(rootX);
    expect(px(node(container, 'r.1.0').style.left)).toBeLessThan(b2);
    expect(px(node(container, 'r.0').style.left)).toBeGreaterThan(rootX);
  });

  it('vertically centers the lighter side against the heavier one', () => {
    // Heavy (2 leaves) → right; Light (1 leaf) → left, centered on the midline.
    const { container } = render(
      <UiRenderer
        surface={mindmap({
          label: 'C',
          children: [{ label: 'Heavy', children: [{ label: 'H1' }, { label: 'H2' }] }, { label: 'Light' }],
        })}
      />,
    );
    const rootTop = px(node(container, 'r').style.top);
    expect(px(node(container, 'r.1').style.top)).toBe(rootTop); // lighter side on the midline
    expect(px(node(container, 'r.0.0').style.top)).toBeLessThan(rootTop);
    expect(px(node(container, 'r.0.1').style.top)).toBeGreaterThan(rootTop);
  });

  it('mirrors node chrome on the left side (outer accent bar, reversed row)', () => {
    const { container } = render(
      <UiRenderer surface={mindmap({ label: 'C', children: [{ label: 'R' }, { label: 'L' }] })} />,
    );
    const rightNode = node(container, 'r.0');
    const leftNode = node(container, 'r.1');
    expect(rightNode.style.flexDirection).toBe('row');
    expect(rightNode.style.borderLeftWidth).toBe('3px');
    expect(leftNode.style.flexDirection).toBe('row-reverse');
    expect(leftNode.style.borderRightWidth).toBe('3px');
  });
});

describe('UiRenderer — remaining visual branches', () => {
  it('passes through a node style override and skips non-primitive style values', () => {
    render(<UiRenderer surface={surface({
      root: { id: 'root', component: 'Text', text: 'Styled', style: { color: '#ff0000', opacity: {} } },
    })} />);
    const el = screen.getByText('Styled');
    expect(el).toBeInTheDocument();
    expect(el.style.color).toBe('rgb(255, 0, 0)'); // string passthrough applied
  });

  it('rotates and truncates bar-chart labels when there are many bars', () => {
    render(<UiRenderer surface={surface({
      root: {
        id: 'root', component: 'Chart', chartType: 'bar',
        data: [
          { label: 'Very Long City Name', value: 9 },
          { label: 'B', value: 3 }, { label: 'C', value: 4 }, { label: 'D', value: 2 },
          { label: 'E', value: 5 }, { label: 'F', value: 1 }, { label: 'G', value: 6 },
        ],
      },
    })} />);
    expect(screen.getByText('Very Long C…')).toBeInTheDocument(); // >6 bars → rotate + truncate
  });

  it('applies the theme font stack (and falls back to sans for an unknown font)', () => {
    const themed = (font: string) =>
      ({ rootId: 'root', components: { root: { id: 'root', component: 'Text', text: 'Fonted' } }, data: {}, theme: { font } }) as unknown as UiSurface;
    const { container, rerender } = render(<UiRenderer surface={themed('serif')} />);
    expect((container.firstChild as HTMLElement).style.fontFamily).toContain('Georgia');
    rerender(<UiRenderer surface={themed('weird')} />); // unknown font → sans fallback
    expect((container.firstChild as HTMLElement).style.fontFamily).toContain('Inter');
  });
});

describe('UiRenderer — Mindmap rendering: lines behind nodes + no overlap', () => {
  const tree = {
    label: 'Root',
    children: [
      { label: 'A very long branch label that would otherwise overflow its column', children: [{ label: 'A1' }] },
      { label: 'B' },
    ],
  };
  const mindmap = () => surface({ root: { id: 'root', component: 'Mindmap', root: tree } });

  it('paints the connector svg BEHIND the nodes (svg zIndex 0, nodes zIndex 1)', () => {
    const { container } = render(<UiRenderer surface={mindmap()} />);
    const svg = container.querySelector('[data-mm-world] svg') as SVGElement;
    expect(svg.style.zIndex).toBe('0');
    container.querySelectorAll('[data-mm-node]').forEach((n) => {
      expect((n as HTMLElement).style.zIndex).toBe('1');
    });
  });

  it('gives non-root nodes an OPAQUE fill so connector lines do not show through', () => {
    const { container } = render(<UiRenderer surface={mindmap()} />);
    const child = container.querySelector('[data-mm-node="r.0"]') as HTMLElement;
    // Composited glass-over-solid: an opaque solid base occludes the line.
    expect(child.style.background).toContain('--ui-surface-solid');
  });

  it('gives nodes a fixed width and wraps long labels to two lines by word', () => {
    const { container } = render(<UiRenderer surface={mindmap()} />);
    const child = container.querySelector('[data-mm-node="r.0"]') as HTMLElement;
    expect(child.style.width).toBe('220px'); // MM_NODE_W — fixed width < MM_COL avoids overlap
    const root = container.querySelector('[data-mm-node="r"]') as HTMLElement;
    expect(root.style.width).toBe('240px'); // root a touch wider
    // The long label wraps by word (whiteSpace normal, overflowWrap break-word —
    // NOT 'anywhere', which broke mid-word) and is clamped to two lines.
    const labelSpan = Array.from(child.querySelectorAll('span')).find(
      (s) => s.textContent?.startsWith('A very long branch'),
    ) as HTMLElement;
    expect(labelSpan).toBeTruthy();
    expect(labelSpan.style.whiteSpace).toBe('normal');
    expect(labelSpan.style.overflowWrap).toBe('break-word');
    expect(labelSpan.style.overflow).toBe('hidden');
    expect(labelSpan.style.webkitLineClamp).toBe('2');
  });

  it('reveals a clamped label in full via a hover tooltip; short nodes get none', () => {
    const { container } = render(<UiRenderer surface={mindmap()} />);
    const longNode = container.querySelector('[data-mm-node="r.0"]') as HTMLElement;
    expect(container.querySelector('[data-mm-tooltip]')).toBeNull(); // nothing until hover
    fireEvent.mouseEnter(longNode);
    const tip = container.querySelector('[data-mm-tooltip]') as HTMLElement;
    expect(tip).toBeTruthy();
    expect(tip.textContent).toContain('A very long branch label that would otherwise overflow its column');
    fireEvent.mouseLeave(longNode);
    expect(container.querySelector('[data-mm-tooltip]')).toBeNull();
    // 'B' is short with no detail → nothing extra to show, so no tooltip.
    const shortNode = container.querySelector('[data-mm-node="r.1"]') as HTMLElement;
    fireEvent.mouseEnter(shortNode);
    expect(container.querySelector('[data-mm-tooltip]')).toBeNull();
  });

  it('shows a node’s description (label heading + detail body) in the hover tooltip', () => {
    const withDetail = surface({
      root: {
        id: 'root',
        component: 'Mindmap',
        root: {
          label: 'Topic',
          children: [{ label: 'Revenue', description: 'Total booked revenue across all regions for FY24, net of refunds.' }],
        },
      },
    });
    const { container } = render(<UiRenderer surface={withDetail} />);
    const child = container.querySelector('[data-mm-node="r.0"]') as HTMLElement;
    fireEvent.mouseEnter(child);
    const tip = container.querySelector('[data-mm-tooltip]') as HTMLElement;
    expect(tip).toBeTruthy();
    expect(tip.textContent).toContain('Revenue'); // short label as the heading
    expect(tip.textContent).toContain('Total booked revenue'); // the detail body
  });

  it('spaces depth columns wider than the node width so a fixed-width node clears the next column', () => {
    const { container } = render(<UiRenderer surface={mindmap()} />);
    const px = (v: string) => parseFloat(v || '0');
    const root = container.querySelector('[data-mm-node="r"]') as HTMLElement;
    const child = container.querySelector('[data-mm-node="r.0"]') as HTMLElement;
    // Adjacent depths are MM_COL (260) apart > the widest node (root 240), so the
    // node footprints (centered, ≤240 wide) never overlap horizontally.
    expect(Math.abs(px(child.style.left) - px(root.style.left))).toBe(260);
  });
});

describe('UiRenderer — Flashcards (Anki flip cards)', () => {
  const deck = (cards: unknown[], title?: string) =>
    surface({ root: { id: 'root', component: 'Flashcards', title, cards } });

  it('renders a card grid and flips a card front↔back on click', () => {
    const { container } = render(
      <UiRenderer surface={deck(
        [
          { front: 'What is Spark?', back: 'A distributed compute engine' },
          { question: 'RDD?', answer: 'Resilient Distributed Dataset' }, // alt key synonyms
        ],
        'Study',
      )} />,
    );
    expect(screen.getByText('Study')).toBeInTheDocument(); // title
    expect(screen.getByText('What is Spark?')).toBeInTheDocument(); // front face
    // alt keys (question/answer) are parsed into front/back too
    expect(screen.getByText('Resilient Distributed Dataset')).toBeInTheDocument();

    const inners = container.querySelectorAll('[data-fc-inner]');
    expect(inners).toHaveLength(2);
    const firstBtn = inners[0].closest('button') as HTMLElement;
    // Starts on the question side (not flipped).
    expect(firstBtn.getAttribute('aria-pressed')).toBe('false');
    expect((inners[0] as HTMLElement).style.transform).toBe('none');
    // Click flips to the answer; the second card is unaffected.
    fireEvent.click(firstBtn);
    expect(firstBtn.getAttribute('aria-pressed')).toBe('true');
    expect((inners[0] as HTMLElement).style.transform).toBe('rotateY(180deg)');
    expect(inners[1].closest('button')!.getAttribute('aria-pressed')).toBe('false');
    // Click again flips back.
    fireEvent.click(firstBtn);
    expect(firstBtn.getAttribute('aria-pressed')).toBe('false');
  });

  it('renders nothing when there are no usable cards', () => {
    const { container } = render(<UiRenderer surface={deck([{}, 'junk', null])} />);
    expect(container.querySelector('[data-fc-inner]')).toBeNull();
  });
});

describe('UiRenderer — URL sanitization (security)', () => {
  it('sanitizes javascript: URLs in Album anchors and keeps safe http(s) links', () => {
    const { container } = render(<UiRenderer surface={surface({
      root: {
        id: 'root',
        component: 'Album',
        images: [
          { url: 'javascript:alert(document.cookie)', alt: 'evil' },
          { url: 'https://example.com/safe.png', alt: 'safe' },
        ],
      },
    })} />);
    const hrefs = Array.from(container.querySelectorAll('a')).map((a) => a.getAttribute('href') ?? '');
    // No anchor may carry a javascript: scheme.
    expect(hrefs.some((h) => h.toLowerCase().startsWith('javascript:'))).toBe(false);
    // The safe https link is preserved.
    expect(hrefs).toContain('https://example.com/safe.png');
  });

  it('blocks a javascript: URL on a single Image (rendered as empty src, not executed)', () => {
    const { container } = render(<UiRenderer surface={surface({
      root: { id: 'root', component: 'Image', url: 'javascript:alert(1)' },
    })} />);
    const img = container.querySelector('img');
    // sanitizeImageSrc strips script schemes -> empty -> node returns null (no img) or empty src.
    expect(img?.getAttribute('src') ?? '').not.toContain('javascript:');
  });
});

describe('UiRenderer — presentation deck caliber (Databricks defaults)', () => {
  const deckSurface = (theme?: Record<string, unknown>) =>
    ({
      rootId: 'root',
      components: {
        root: { id: 'root', component: 'Slides', children: ['s0', 's1'] },
        s0: { id: 's0', component: 'Slide', title: 'First', children: ['t0'] },
        t0: { id: 't0', component: 'Text', text: 'alpha' },
        s1: { id: 's1', component: 'Slide', title: 'Second', children: ['t1'] },
        t1: { id: 't1', component: 'Text', text: 'beta' },
      } as never,
      data: {},
      ...(theme ? { theme } : {}),
    }) as unknown as UiSurface;

  it('a Slides root gets the deck default tokens (orange accent, teal stage)', () => {
    const { container } = render(<UiRenderer surface={deckSurface()} />);
    const stage = container.firstElementChild as HTMLElement;
    expect(stage.style.getPropertyValue('--ui-accent')).toBe('#FF3621');
    expect(stage.style.getPropertyValue('--ui-stage')).toContain('#162A34');
  });

  it('a UI-Configurator palette still overrides the deck defaults', () => {
    const { container } = render(
      <UiRenderer surface={deckSurface({ accent: '#00A972', background: '#101010' })} />,
    );
    const stage = container.firstElementChild as HTMLElement;
    expect(stage.style.getPropertyValue('--ui-accent')).toBe('#00A972');
    expect(stage.style.getPropertyValue('--ui-stage')).toBe('#101010');
  });

  it('non-deck roots keep the generic premium theme (no deck tokens)', () => {
    const { container } = render(
      <UiRenderer
        surface={
          {
            rootId: 'root',
            components: { root: { id: 'root', component: 'Text', text: 'hi' } } as never,
            data: {},
          } as unknown as UiSurface
        }
      />,
    );
    const stage = container.firstElementChild as HTMLElement;
    expect(stage.style.getPropertyValue('--ui-accent')).toBe('');
  });

  it('shows a deck-style slide counter and replays the entrance animation per slide', () => {
    const { container } = render(<UiRenderer surface={deckSurface()} />);
    expect(screen.getByText('01 / 02')).toBeInTheDocument();
    const firstWrapper = container.querySelector('.ui-slide-enter');
    expect(firstWrapper).not.toBeNull();

    fireEvent.click(screen.getByLabelText('Go to slide 2'));
    expect(screen.getByText('02 / 02')).toBeInTheDocument();
    // keyed remount: the wrapper is a NEW element, so the CSS animation replays
    const secondWrapper = container.querySelector('.ui-slide-enter');
    expect(secondWrapper).not.toBe(firstWrapper);
    expect(screen.getByText('Second')).toBeInTheDocument();
  });
});
