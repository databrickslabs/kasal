import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import PreviewPanel, { parsePreviewContent, PreviewContent } from './PreviewPanel';

// react-markdown / remark-gfm are ESM-only; stub them.
vi.mock('react-markdown', () => ({
  default: ({ children }: { children: string }) => <div data-testid="md">{children}</div>,
}));
vi.mock('remark-gfm', () => ({ default: () => {} }));

describe('parsePreviewContent', () => {
  it('returns null for empty or too-short input', () => {
    expect(parsePreviewContent('')).toBeNull();
    expect(parsePreviewContent('short')).toBeNull();
  });

  it('detects full HTML documents (DOCTYPE, html tag, HTML upper)', () => {
    expect(parsePreviewContent('<!DOCTYPE html><html><body>hi</body></html>')?.type).toBe('html');
    expect(parsePreviewContent('<!doctype html><html></html>')?.type).toBe('html');
    expect(parsePreviewContent('<html lang="en"><body>x</body></html>')?.type).toBe('html');
    expect(parsePreviewContent('<HTML><body>x</body></HTML>')?.type).toBe('html');
  });

  it('detects HTML via <script> and generic tag heuristics', () => {
    expect(parsePreviewContent('<div><script>var x=1;</script></div>')?.type).toBe('html');
    expect(parsePreviewContent('<section><p>hello world</p></section>')?.type).toBe('html');
  });

  it('detects JSON objects and arrays', () => {
    expect(parsePreviewContent('{"a":1,"b":2}')?.type).toBe('json');
    expect(parsePreviewContent('[{"a":1},{"a":2}]')?.type).toBe('json');
  });

  it('falls through invalid JSON-looking content to text/null', () => {
    // looks like json (starts { ends }) but invalid -> not json; also not html/markdown -> null
    expect(parsePreviewContent('{not valid json at all}')).toBeNull();
  });

  it('detects markdown via headers + structure and headers + length', () => {
    expect(parsePreviewContent('# Title\n\n- a\n- b')?.type).toBe('markdown');
    expect(parsePreviewContent('## Title\n\n```\ncode\n```')?.type).toBe('markdown');
    expect(parsePreviewContent('### T\n\n| a | b |')?.type).toBe('markdown');
    const long = '# Big\n\n' + 'x'.repeat(600);
    expect(parsePreviewContent(long)?.type).toBe('markdown');
  });

  it('returns null for a header with no structure and short length (plain text)', () => {
    expect(parsePreviewContent('# Just a heading line only')).toBeNull();
  });

  it('strips a leading **Task title** prefix before parsing', () => {
    const out = parsePreviewContent('**My Task**\n\n{"a":1,"b":2}');
    expect(out?.type).toBe('json');
    expect(out?.data).toBe('{"a":1,"b":2}');
  });

  it('strips full and inner code fences', () => {
    const full = parsePreviewContent('```html\n<div><p>hi there</p></div>\n```');
    expect(full?.type).toBe('html');
    expect(full?.data).toContain('<div>');
    const inner = parsePreviewContent('Here is output:\n```json\n{"a":1,"b":2}\n```\nthanks');
    expect(inner?.type).toBe('json');
  });

  it('extracts the largest embedded DOCTYPE html document (reduce both arms)', () => {
    // No standalone <html> opening tag -> detectContentType stays "text", so the
    // DOCTYPE branch of extractEmbeddedHtml runs. Order small,big,smaller forces
    // both arms of the `a.length >= b.length` reducer.
    const small = '<!DOCTYPE html><body>' + 'a'.repeat(40) + '</body></html>';
    const big = '<!DOCTYPE html><body>' + 'b'.repeat(160) + '</body></html>';
    const smaller = '<!DOCTYPE html><body>' + 'c'.repeat(20) + '</body></html>';
    const out = parsePreviewContent(`prefix filler text here\n${small}\n${big}\n${smaller}`);
    expect(out?.type).toBe('html');
    expect(out?.data).toContain('b'.repeat(160));
  });

  it('extracts embedded HTML via the <html> fallback (reduce both arms)', () => {
    // A ```json fence makes the *cleaned* content "text" while the *body* still
    // holds <html> docs with no DOCTYPE -> hits the htmlTag fallback + reduce.
    const small = '<html><body>' + 'a'.repeat(40) + '</body></html>';
    const big = '<html><body>' + 'b'.repeat(160) + '</body></html>';
    const smaller = '<html><body>' + 'c'.repeat(20) + '</body></html>';
    const raw = 'lead ```json\n{bad}\n``` ' + small + ' ' + big + ' ' + smaller;
    const out = parsePreviewContent(raw);
    expect(out?.type).toBe('html');
    expect(out?.data).toContain('b'.repeat(160));
  });

  it('returns null when embedded html is too short (<=100 chars)', () => {
    // No standalone <html> opening tag, so detectContentType stays "text" and
    // extractEmbeddedHtml's DOCTYPE match (<=100 chars) hits the length guard.
    const mixed = 'plain words with filler text to pass the length gate here ok\n<!DOCTYPE html><body>x</body></html>';
    expect(parsePreviewContent(mixed)).toBeNull();
  });

  it('returns null for genuinely plain text', () => {
    expect(parsePreviewContent('just a normal sentence with nothing special here at all')).toBeNull();
  });
});

const baseProps = {
  onClose: vi.fn(),
  chatCollapsed: false,
  onToggleChat: vi.fn(),
};

function renderPanel(content: PreviewContent, props: Partial<typeof baseProps> = {}) {
  return render(<PreviewPanel content={content} {...baseProps} {...props} />);
}

describe('PreviewPanel component', () => {
  it('renders an HTML preview into a sandboxed iframe (doc.write)', () => {
    const { container } = renderPanel({ type: 'html', data: '<p>hello iframe</p>' });
    const iframe = container.querySelector('iframe');
    expect(iframe).toBeInTheDocument();
    expect(iframe?.getAttribute('title')).toBe('Execution result preview');
    expect(screen.getByText('Preview')).toBeInTheDocument(); // default html title
    expect(screen.getByText('HTML')).toBeInTheDocument(); // type badge
  });

  it('renders a JSON array-of-objects as a table with unioned keys and value formatting', () => {
    renderPanel({
      type: 'json',
      data: JSON.stringify([
        { a: 1, b: { nested: true } },
        { a: null, c: 'x' },
      ]),
    });
    // header keys union: a, b, c
    expect(screen.getByText('a')).toBeInTheDocument();
    expect(screen.getByText('b')).toBeInTheDocument();
    expect(screen.getByText('c')).toBeInTheDocument();
    // object value JSON.stringify'd
    expect(screen.getByText('{"nested":true}')).toBeInTheDocument();
  });

  it('handles array-of-objects rows that are null or non-object', () => {
    // data[0] is an object so the table branch runs; later null/primitive rows
    // exercise the `row && typeof row === 'object'` guard false arm + empty cell.
    renderPanel({ type: 'json', data: JSON.stringify([{ a: 1 }, null, 'str']) });
    expect(screen.getByText('a')).toBeInTheDocument();
  });

  it('renders a single JSON object as a key/value table', () => {
    renderPanel({ type: 'json', data: JSON.stringify({ name: 'Kasal', meta: { x: 1 }, empty: null }) });
    expect(screen.getByText('Key')).toBeInTheDocument();
    expect(screen.getByText('Value')).toBeInTheDocument();
    expect(screen.getByText('name')).toBeInTheDocument();
    expect(screen.getByText('Kasal')).toBeInTheDocument();
  });

  it('renders a primitive JSON array via the <pre> fallback', () => {
    const { container } = renderPanel({ type: 'json', data: JSON.stringify([1, 2, 3]) });
    expect(container.querySelector('pre')).toBeInTheDocument();
  });

  it('renders nothing in body for invalid JSON (jsonData null)', () => {
    const { container } = renderPanel({ type: 'json', data: '{invalid' });
    expect(container.querySelector('table')).toBeNull();
    expect(container.querySelector('pre')).toBeNull();
  });

  it('renders markdown via ReactMarkdown with Report default title', () => {
    renderPanel({ type: 'markdown', data: '# Report body' });
    expect(screen.getByTestId('md')).toHaveTextContent('# Report body');
    expect(screen.getByText('Report')).toBeInTheDocument();
  });

  it('renders text type with the Result default title and no body content', () => {
    const { container } = renderPanel({ type: 'text', data: 'plain' });
    expect(screen.getByText('Result')).toBeInTheDocument();
    expect(container.querySelector('iframe')).toBeNull();
    expect(container.querySelector('table')).toBeNull();
  });

  it('uses a provided title over the default', () => {
    renderPanel({ type: 'html', data: '<p>x</p>', title: 'Custom Title' });
    expect(screen.getByText('Custom Title')).toBeInTheDocument();
  });

  it('heals a stored **Title** prefix in displayData (markdown)', () => {
    renderPanel({ type: 'markdown', data: '**Task X**\n\n# Body' });
    expect(screen.getByTestId('md')).toHaveTextContent('# Body');
    expect(screen.getByTestId('md')).not.toHaveTextContent('Task X');
  });

  it('close and toggle-chat buttons fire their callbacks', () => {
    const onClose = vi.fn();
    const onToggleChat = vi.fn();
    renderPanel({ type: 'text', data: 'x' }, { onClose, onToggleChat });
    fireEvent.click(screen.getByTitle('Close preview'));
    expect(onClose).toHaveBeenCalled();
    fireEvent.click(screen.getByTitle('Hide chat'));
    expect(onToggleChat).toHaveBeenCalled();
  });

  it('shows "Show chat" affordance when chatCollapsed is true', () => {
    renderPanel({ type: 'text', data: 'x' }, { chatCollapsed: true });
    expect(screen.getByTitle('Show chat')).toBeInTheDocument();
  });

  it('does not render the Refine button when onRefine is not provided', () => {
    renderPanel({ type: 'html', data: '<p>x</p>' });
    expect(screen.queryByText('Refine')).not.toBeInTheDocument();
  });

  it('opens the refine bar and submits an instruction via the button', () => {
    const onRefine = vi.fn();
    render(<PreviewPanel content={{ type: 'html', data: '<p>x</p>' }} {...baseProps} onRefine={onRefine} />);
    // toggle open
    fireEvent.click(screen.getByText('Refine'));
    const input = screen.getByPlaceholderText('Describe how to improve this result…');
    fireEvent.change(input, { target: { value: 'make it blue' } });
    // the submit button is the second "Refine" (in the bar)
    const buttons = screen.getAllByText('Refine');
    fireEvent.click(buttons[buttons.length - 1]);
    expect(onRefine).toHaveBeenCalledWith('make it blue');
    // bar closes after submit
    expect(screen.queryByPlaceholderText('Describe how to improve this result…')).not.toBeInTheDocument();
  });

  it('submits the refine instruction on Enter and ignores empty submits', () => {
    const onRefine = vi.fn();
    render(<PreviewPanel content={{ type: 'html', data: '<p>x</p>' }} {...baseProps} onRefine={onRefine} />);
    fireEvent.click(screen.getByText('Refine'));
    const input = screen.getByPlaceholderText('Describe how to improve this result…');
    // empty Enter -> no-op
    fireEvent.keyDown(input, { key: 'Enter' });
    expect(onRefine).not.toHaveBeenCalled();
    // typed Enter -> submits
    fireEvent.change(input, { target: { value: 'add a chart' } });
    fireEvent.keyDown(input, { key: 'Enter' });
    expect(onRefine).toHaveBeenCalledWith('add a chart');
  });

  it('closes the refine bar on Escape without submitting', () => {
    const onRefine = vi.fn();
    render(<PreviewPanel content={{ type: 'html', data: '<p>x</p>' }} {...baseProps} onRefine={onRefine} />);
    fireEvent.click(screen.getByText('Refine'));
    const input = screen.getByPlaceholderText('Describe how to improve this result…');
    fireEvent.change(input, { target: { value: 'discard me' } });
    fireEvent.keyDown(input, { key: 'Escape' });
    expect(onRefine).not.toHaveBeenCalled();
    expect(screen.queryByPlaceholderText('Describe how to improve this result…')).not.toBeInTheDocument();
  });

  it('does not render the history nav when history has one or zero entries', () => {
    const onNavigate = vi.fn();
    const single = [{ type: 'html' as const, data: '<p>x</p>' }];
    renderPanel({ type: 'html', data: '<p>x</p>' }, {});
    expect(screen.queryByLabelText('Previous output')).not.toBeInTheDocument();
    // explicit single-entry history + onNavigate still hides the nav
    render(
      <PreviewPanel
        content={single[0]}
        {...baseProps}
        history={single}
        index={0}
        onNavigate={onNavigate}
      />,
    );
    expect(screen.queryByLabelText('Previous output')).not.toBeInTheDocument();
  });

  it('renders history nav, shows position, and navigates older/newer', () => {
    const onNavigate = vi.fn();
    const history = [
      { type: 'markdown' as const, data: '# first' },
      { type: 'html' as const, data: '<p>second</p>' },
      { type: 'json' as const, data: '{"a":1}' },
    ];
    render(
      <PreviewPanel
        content={history[2]}
        {...baseProps}
        history={history}
        index={2}
        onNavigate={onNavigate}
      />,
    );
    // position label: latest shown first => 3/3
    expect(screen.getByText('3/3')).toBeInTheDocument();
    // next disabled at the latest, prev enabled
    const prev = screen.getByLabelText('Previous output');
    const next = screen.getByLabelText('Next output');
    expect(next).toBeDisabled();
    expect(prev).not.toBeDisabled();
    fireEvent.click(prev);
    expect(onNavigate).toHaveBeenCalledWith(1);
  });

  it('defaults the position to the latest when index is omitted and disables prev at the oldest', () => {
    const onNavigate = vi.fn();
    const history = [
      { type: 'markdown' as const, data: '# first' },
      { type: 'html' as const, data: '<p>second</p>' },
    ];
    // index omitted -> defaults to history.length - 1 (latest)
    render(
      <PreviewPanel
        content={history[1]}
        {...baseProps}
        history={history}
        onNavigate={onNavigate}
      />,
    );
    expect(screen.getByText('2/2')).toBeInTheDocument();
    fireEvent.click(screen.getByLabelText('Next output')); // disabled, no-op visually but click guarded
    // navigate to the oldest then assert prev disabled there
    render(
      <PreviewPanel
        content={history[0]}
        {...baseProps}
        history={history}
        index={0}
        onNavigate={onNavigate}
      />,
    );
    const prevs = screen.getAllByLabelText('Previous output');
    expect(prevs[prevs.length - 1]).toBeDisabled();
    const next = screen.getAllByLabelText('Next output');
    fireEvent.click(next[next.length - 1]);
    expect(onNavigate).toHaveBeenCalledWith(1);
  });

  it('safely no-ops the iframe write when contentDocument is null (defensive branch)', () => {
    const proto = window.HTMLIFrameElement.prototype;
    const original = Object.getOwnPropertyDescriptor(proto, 'contentDocument');
    Object.defineProperty(proto, 'contentDocument', { configurable: true, get: () => null });
    try {
      const { container } = renderPanel({ type: 'html', data: '<p>x</p>' });
      // Renders without throwing even though doc is null
      expect(container.querySelector('iframe')).toBeInTheDocument();
    } finally {
      if (original) Object.defineProperty(proto, 'contentDocument', original);
    }
  });
});
