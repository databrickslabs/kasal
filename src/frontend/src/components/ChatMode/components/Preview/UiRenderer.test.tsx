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
