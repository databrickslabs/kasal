import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest';
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import DeckStudio from './DeckStudio';
import HtmlDeckBlock from '../Chat/HtmlDeckBlock';
import { useAppStore } from '../../store/appStore';
import { fetchEnabledModels } from '../../api/models';
import { useSessionStore } from '../../../../app/sessions/sessionStore';
import { splitSlides } from '../../utils/htmlDeck';
import { downloadDeckHtml } from '../../utils/deckExport';
import { useThemeStore } from '../../../../store/theme';

vi.mock('../../api/models', () => ({ fetchEnabledModels: vi.fn() }));

const refineSlide = vi.fn();
vi.mock('../../../../api/chat/DeckService', () => ({
  DeckService: { refineSlide: (...args: unknown[]) => refineSlide(...args) },
}));
vi.mock('../../persistence/sessionApi', () => ({
  addMessageToSession: vi.fn().mockResolvedValue(undefined),
  updateMessageInSession: vi.fn().mockResolvedValue(undefined),
}));
vi.mock('../../utils/deckExport', () => ({
  downloadDeckHtml: vi.fn(),
  downloadDeckPdf: vi.fn(),
  downloadDeckPptx: vi.fn(),
  sanitizeDeckDocument: (html: string) => html,
}));
vi.mock('../Chat/RunProgress', () => ({
  default: ({ jobId, running, onSelectStep }: { jobId?: string; running: boolean; onSelectStep: (step: unknown) => void }) =>
    <div data-testid="slide-run" data-job-id={jobId} data-running={String(running)}>
      <button onClick={() => onSelectStep({ id: 'call', label: 'Model request', detail: 'The slide-edit prompt' })}>Open model request</button>
    </div>,
}));
vi.mock('../Preview/StepContent', () => ({
  default: ({ step }: { step: { detail: string } }) => <div>{step.detail}</div>,
}));

const slide = (t: string) => `<section class="slide"><h1>${t}</h1></section>`;
const DECK = [slide('Cover'), slide('Two'), slide('Three')].join('\n');
const titles = (html: string) => splitSlides(html).map((s) => s.match(/<h1>(.*?)<\/h1>/)?.[1]);
const deckInMessage = () => {
  const m = useSessionStore.getState().messages.find((x) => x.id === 'm1');
  return m ? titles(m.content) : null;
};

describe('DeckStudio', () => {
  afterEach(() => vi.restoreAllMocks());
  beforeEach(() => {
    useThemeStore.setState({ isDarkMode: false });
    refineSlide.mockReset();
    useAppStore.setState({ selectedModel: 'chat-model', models: [] });
    vi.mocked(fetchEnabledModels).mockResolvedValue([
      { key: 'chat-model', name: 'Chat model' },
      { key: 'edit-model', name: 'Editing model' },
    ] as Awaited<ReturnType<typeof fetchEnabledModels>>);
    useSessionStore.setState({
      messages: [{ id: 'm1', role: 'assistant', content: 'Deck:\n```html\n' + DECK + '\n```', timestamp: new Date() } as never],
      currentSessionId: 'owner',
    } as never);
  });

  it('chooses an edit-local model and keeps picker keys from navigating slides', async () => {
    const close = vi.fn();
    refineSlide.mockResolvedValue({ section: slide('Updated') });
    render(<DeckStudio code={DECK} messageId="m1" model="builder-model" onClose={close} />);
    const picker = screen.getByRole('combobox', { name: 'Slide edit model' });
    expect(picker).toHaveValue('builder-model');
    await screen.findByRole('option', { name: 'Editing model' });
    fireEvent.keyDown(picker, { key: 'ArrowDown' });
    fireEvent.keyDown(picker, { key: 'Escape' });
    expect(close).not.toHaveBeenCalled();
    expect(screen.getByRole('listitem', { name: 'Slide 1' })).toHaveAttribute('aria-current', 'true');
    fireEvent.change(picker, { target: { value: 'edit-model' } });
    fireEvent.change(screen.getByLabelText('Slide instruction'), { target: { value: 'Improve' } });
    fireEvent.click(screen.getByText('Apply'));
    expect(picker).toBeDisabled();
    await waitFor(() => expect(refineSlide).toHaveBeenCalledWith(
      expect.objectContaining({ model: 'edit-model' }), expect.any(Function),
    ));
    await waitFor(() => expect(picker).not.toBeDisabled());
    expect(picker).toHaveValue('edit-model');
    expect(useAppStore.getState().selectedModel).toBe('chat-model');
  });

  it('allows the workspace default when writing a new slide', async () => {
    refineSlide.mockResolvedValue({ section: slide('New') });
    render(<DeckStudio code={DECK} messageId="m1" onClose={() => {}} />);
    expect(screen.getByLabelText('Slide edit model')).toHaveValue('chat-model');
    fireEvent.change(screen.getByLabelText('Slide edit model'), { target: { value: '' } });
    fireEvent.click(screen.getByLabelText('Add a slide at position 2'));
    fireEvent.change(screen.getByLabelText('New slide instruction'), { target: { value: 'Explain tokens' } });
    fireEvent.click(screen.getByText('Apply'));
    await waitFor(() => expect(refineSlide).toHaveBeenCalledWith(
      expect.objectContaining({ mode: 'add', model: null }), expect.any(Function),
    ));
  });

  it('keeps the inherited model usable when loading models fails', async () => {
    vi.mocked(fetchEnabledModels).mockRejectedValue(new Error('offline'));
    render(<DeckStudio code={DECK} messageId="m1" model="builder-model" onClose={() => {}} />);
    expect(await screen.findByRole('status')).toHaveTextContent('Could not refresh models.');
    expect(screen.getByLabelText('Slide edit model')).toHaveValue('builder-model');
    expect(screen.getByLabelText('Slide edit model')).not.toBeDisabled();
  });

  it('shows every slide in the rail, selects on click, and pages with the keys', () => {
    render(<DeckStudio code={DECK} messageId="m1" onClose={() => {}} />);
    expect(screen.getAllByRole('listitem')).toHaveLength(3);
    expect(screen.getByRole('listitem', { name: 'Slide 1' })).toHaveAttribute('aria-current', 'true');
    fireEvent.click(screen.getByLabelText('Select slide 3'));
    expect(screen.getByRole('listitem', { name: 'Slide 3' })).toHaveAttribute('aria-current', 'true');
    // Keys are handled on the studio container (a window listener would be
    // defeated by the modal's own stopPropagation), so they must originate from
    // inside it — as a real keypress with focus in the studio does.
    fireEvent.keyDown(screen.getByRole('dialog', { name: 'Deck studio' }), { key: 'ArrowLeft' });
    expect(screen.getByRole('listitem', { name: 'Slide 2' })).toHaveAttribute('aria-current', 'true');
  });

  it('reattaches to running edits after closing and reopening, including parallel saves', async () => {
    const runs = [deferred<{ section: string; job_id: string }>(), deferred<{ section: string; job_id: string }>()];
    refineSlide.mockImplementation((_request, started) => {
      const index = refineSlide.mock.calls.length - 1;
      started(`job-${index}`);
      return runs[index].promise;
    });
    render(<HtmlDeckBlock code={DECK} messageId="m1" />);
    fireEvent.click(screen.getByTitle('Edit deck'));
    submitSlide(1, 'Improve cover');
    fireEvent.click(screen.getByTitle('Done (Esc)'));
    fireEvent.click(screen.getByTitle('Edit deck'));
    expect(screen.getByLabelText('Slide instruction')).toBeDisabled();
    expect(screen.getByTestId('slide-run')).toHaveAttribute('data-job-id', 'job-0');
    expect(screen.getByTestId('slide-run')).toHaveAttribute('data-running', 'true');
    submitSlide(2, 'Improve second');
    await act(async () => runs[0].resolve({ section: slide('New cover'), job_id: 'job-0' }));
    await act(async () => runs[1].resolve({ section: slide('New second'), job_id: 'job-1' }));
    expect(refineSlide).toHaveBeenCalledTimes(2);
    expect(deckInMessage()).toEqual(['New cover', 'New second', 'Three']);
    expect(screen.getByLabelText('Slide instruction')).toBeEnabled();
    expect(screen.getByText('3 slides · 2 edits')).toBeInTheDocument();
  });

  it('keeps completed results and undo history when a run finishes while closed', async () => {
    const run = deferred<{ section: string }>();
    refineSlide.mockReturnValue(run.promise);
    render(<HtmlDeckBlock code={DECK} messageId="m1" />);
    fireEvent.click(screen.getByTitle('Edit deck'));
    submitSlide(1, 'Improve cover');
    fireEvent.click(screen.getByTitle('Done (Esc)'));
    await act(async () => run.resolve({ section: slide('Updated while closed') }));
    fireEvent.click(screen.getByTitle('Edit deck'));
    expect(screen.getByText('3 slides · 1 edit')).toBeInTheDocument();
    expect(screen.getByTestId('slide-run')).toHaveAttribute('data-running', 'false');
    expect(screen.getByLabelText('Slide instruction')).toBeEnabled();
    fireEvent.click(screen.getByLabelText('Move slide 1 down'));
    expect(deckInMessage()).toEqual(['Two', 'Updated while closed', 'Three']);
  });

  it('shares the pending save queue across editor mounts in builder mode', async () => {
    const save = deferred<void>();
    const onDeckChange = vi.fn().mockReturnValueOnce(save.promise).mockResolvedValue(undefined);
    refineSlide.mockResolvedValueOnce({ section: slide('New cover') }).mockResolvedValueOnce({ section: slide('New second') });
    render(<HtmlDeckBlock code={DECK} onDeckChange={onDeckChange} />);
    fireEvent.click(screen.getByTitle('Edit deck'));
    submitSlide(1);
    await waitFor(() => expect(onDeckChange).toHaveBeenCalledTimes(1));
    fireEvent.click(screen.getByTitle('Done (Esc)'));
    fireEvent.click(screen.getByTitle('Edit deck'));
    expect(screen.getByLabelText('Slide instruction')).toBeDisabled();
    expect(screen.getByLabelText('Move slide 2 up')).toBeDisabled();
    submitSlide(2);
    await waitFor(() => expect(refineSlide).toHaveBeenCalledTimes(2));
    expect(onDeckChange).toHaveBeenCalledTimes(1);
    await act(async () => save.resolve());
    await waitFor(() => expect(onDeckChange).toHaveBeenCalledTimes(2));
    expect(titles(onDeckChange.mock.calls[1][1])).toEqual(['New cover', 'Two', 'Three']);
    expect(titles(onDeckChange.mock.calls[1][0])).toEqual(['New cover', 'New second', 'Three']);
    expect(screen.getByLabelText('Move slide 2 up')).toBeEnabled();
  });

  it('keeps failures visible after reopening without leaking them into another deck', async () => {
    const run = deferred<{ section: string }>();
    refineSlide.mockReturnValue(run.promise);
    render(<><HtmlDeckBlock code={DECK} messageId="m1" /><HtmlDeckBlock code={slide('Other deck')} /></>);
    fireEvent.click(screen.getAllByTitle('Edit deck')[0]);
    submitSlide(1, 'Improve');
    fireEvent.click(screen.getByTitle('Done (Esc)'));
    await act(async () => run.reject(new Error('Provider unavailable')));
    fireEvent.click(screen.getAllByTitle('Edit deck')[1]);
    expect(screen.queryByText('Provider unavailable')).not.toBeInTheDocument();
    expect(screen.queryByTestId('slide-run')).not.toBeInTheDocument();
    fireEvent.click(screen.getByTitle('Done (Esc)'));
    fireEvent.click(screen.getAllByTitle('Edit deck')[0]);
    expect(screen.getByRole('alert')).toHaveTextContent('Provider unavailable');
    expect(screen.getByLabelText('Slide instruction')).toBeEnabled();
  });

  it('scrolls the thumbnail rail to keep keyboard selection visible without moving focus', () => {
    render(<DeckStudio code={DECK} messageId="m1" onClose={() => {}} />);
    const rail = screen.getByRole('list', { name: 'Slides' });
    const dialog = screen.getByRole('dialog', { name: 'Deck studio' });
    const rect = (top: number, bottom: number) => ({ top, bottom, left: 0, right: 200, width: 200, height: bottom - top, x: 0, y: top, toJSON() {} });
    vi.spyOn(rail, 'getBoundingClientRect').mockReturnValue(rect(100, 400));
    Object.defineProperty(rail, 'clientHeight', { configurable: true, value: 300 });
    vi.spyOn(screen.getByRole('listitem', { name: 'Slide 2' }), 'getBoundingClientRect').mockReturnValue(rect(220, 320));
    vi.spyOn(screen.getByRole('listitem', { name: 'Slide 3' }), 'getBoundingClientRect').mockReturnValue(rect(420, 520));
    vi.spyOn(screen.getByRole('listitem', { name: 'Slide 1' }), 'getBoundingClientRect').mockReturnValue(rect(-20, 80));
    fireEvent.keyDown(dialog, { key: 'ArrowDown' });
    expect(rail.scrollTop).toBe(0); // Already visible: no jumping.
    fireEvent.keyDown(dialog, { key: 'End' });
    expect(rail.scrollTop).toBe(120);
    expect(screen.getByLabelText('Slide instruction')).toHaveFocus();
    fireEvent.keyDown(dialog, { key: 'Home' });
    expect(rail.scrollTop).toBe(0);
  });

  it('downloads the presentation as HTML', () => {
    render(<DeckStudio code={DECK} messageId="m1" onClose={() => {}} />);
    fireEvent.click(screen.getByTitle('Download'));
    fireEvent.click(screen.getByRole('button', { name: 'Download HTML' }));
    expect(downloadDeckHtml).toHaveBeenCalledWith(DECK);
  });

  it('follows Kasal theme changes without changing the slide content', () => {
    render(<DeckStudio code={DECK} messageId="m1" onClose={() => {}} />);
    const root = screen.getByRole('dialog', { name: 'Deck studio' }).parentElement;
    expect(root).toHaveAttribute('data-theme', 'light');
    expect(root).toHaveStyle({ colorScheme: 'light' });
    act(() => useThemeStore.setState({ isDarkMode: true }));
    expect(root).toHaveAttribute('data-theme', 'dark');
    expect(root).toHaveStyle({ colorScheme: 'dark' });
    act(() => useThemeStore.setState({ isDarkMode: false }));
    expect(root).toHaveAttribute('data-theme', 'light');
    expect(deckInMessage()).toEqual(['Cover', 'Two', 'Three']);
  });

  it('accepts consecutive thumbnail and between-slide drops with a fresh native drag each time', () => {
    render(<DeckStudio code={DECK} messageId="m1" onClose={() => {}} />);
    const transfer = { setData: vi.fn(), effectAllowed: '', dropEffect: '' };
    const drag = (from: number, target: HTMLElement) => {
      const source = screen.getByLabelText(`Select slide ${from}`);
      expect(source).toHaveAttribute('draggable', 'true');
      fireEvent.dragStart(source, { dataTransfer: transfer });
      expect(transfer.setData).toHaveBeenLastCalledWith('text/plain', String(from - 1));
      expect(transfer.effectAllowed).toBe('move');
      fireEvent.dragOver(target, { dataTransfer: transfer });
      fireEvent.drop(target, { dataTransfer: transfer });
      fireEvent.dragEnd(source);
    };
    drag(3, screen.getByLabelText('Add a slide at position 1'));
    expect(deckInMessage()).toEqual(['Three', 'Cover', 'Two']);
    drag(1, screen.getByLabelText('Add a slide at position 4'));
    expect(deckInMessage()).toEqual(['Cover', 'Two', 'Three']);
    drag(2, screen.getByLabelText('Select slide 1'));
    expect(deckInMessage()).toEqual(['Two', 'Cover', 'Three']);
    // A cancelled drag or a foreign drop cannot reuse the last source index.
    fireEvent.dragStart(screen.getByLabelText('Select slide 1'), { dataTransfer: transfer });
    fireEvent.dragEnd(screen.getByLabelText('Select slide 1'));
    fireEvent.drop(screen.getByLabelText('Select slide 3'), { dataTransfer: transfer });
    expect(deckInMessage()).toEqual(['Two', 'Cover', 'Three']);
  });

  it('moves slides with the arrow buttons, preserves selection, and supports undo', async () => {
    render(<DeckStudio code={DECK} messageId="m1" onClose={() => {}} />);
    expect(screen.getByLabelText('Move slide 1 up')).toBeDisabled();
    expect(screen.getByLabelText('Move slide 3 down')).toBeDisabled();
    fireEvent.click(screen.getByLabelText('Move slide 2 up'));
    expect(deckInMessage()).toEqual(['Two', 'Cover', 'Three']);
    expect(screen.getByRole('listitem', { name: 'Slide 1' })).toHaveAttribute('aria-current', 'true');
    fireEvent.click(screen.getByLabelText('Move slide 1 down'));
    expect(deckInMessage()).toEqual(['Cover', 'Two', 'Three']);
    expect(screen.getByRole('listitem', { name: 'Slide 2' })).toHaveAttribute('aria-current', 'true');
    fireEvent.click(screen.getByRole('button', { name: 'Undo' }));
    await waitFor(() => expect(deckInMessage()).toEqual(['Two', 'Cover', 'Three']));
  });

  it('explains the reorder lock and unlocks the arrows after generation finishes', async () => {
    let resolve!: (result: { section: string }) => void;
    refineSlide.mockReturnValue(new Promise(done => { resolve = done; }));
    render(<DeckStudio code={DECK} messageId="m1" onClose={() => {}} />);
    fireEvent.change(screen.getByLabelText('Slide instruction'), { target: { value: 'Improve' } });
    fireEvent.click(screen.getByText('Apply'));
    expect(screen.getByLabelText('Move slide 2 up')).toBeDisabled();
    expect(screen.getByLabelText('Move slide 2 down')).toBeDisabled();
    expect(screen.getByText('Reordering available after slide edits finish saving')).toBeInTheDocument();
    await act(async () => resolve({ section: slide('Updated') }));
    expect(screen.getByLabelText('Move slide 2 up')).toBeEnabled();
    expect(screen.queryByText('Reordering available after slide edits finish saving')).not.toBeInTheDocument();
    fireEvent.click(screen.getByLabelText('Move slide 2 up'));
    expect(deckInMessage()).toEqual(['Two', 'Updated', 'Three']);
  });

  it('imports a standalone HTML presentation through the existing save path', async () => {
    const onDeckChange = vi.fn().mockResolvedValue(undefined);
    const imported = [slide('Imported cover'), slide('Imported detail')].join('\n');
    render(<DeckStudio code={DECK} messageId="m1" onDeckChange={onDeckChange} onClose={() => {}} />);

    const file = new File(
      [`<!doctype html><html><body><main id="deck-stage">${imported}</main></body></html>`],
      'imported.html',
      { type: 'text/html' },
    );
    fireEvent.change(screen.getByTestId('deck-html-input'), { target: { files: [file] } });

    await waitFor(() => expect(onDeckChange).toHaveBeenCalledWith(imported, DECK));
    expect(await screen.findByText('2 slides · 1 edit')).toBeInTheDocument();
    expect(screen.getByRole('listitem', { name: 'Slide 1' })).toHaveAttribute('aria-current', 'true');
  });

  it('rejects an HTML document without presentation slides', async () => {
    const onDeckChange = vi.fn().mockResolvedValue(undefined);
    render(<DeckStudio code={DECK} messageId="m1" onDeckChange={onDeckChange} onClose={() => {}} />);

    const file = new File(['<!doctype html><html><body><p>Not a deck</p></body></html>'], 'notes.html', { type: 'text/html' });
    fireEvent.change(screen.getByTestId('deck-html-input'), { target: { files: [file] } });

    expect(await screen.findByRole('alert')).toHaveTextContent('does not contain any presentation slides');
    expect(onDeckChange).not.toHaveBeenCalled();
  });

  it('selects a slide on pointer-down, so a draggable row eating the click still selects', () => {
    render(<DeckStudio code={DECK} messageId="m1" onClose={() => {}} />);
    fireEvent.pointerDown(screen.getByLabelText('Select slide 3'));
    expect(screen.getByRole('listitem', { name: 'Slide 3' })).toHaveAttribute('aria-current', 'true');
  });

  it('arrows page slides from the empty instruction bar, but edit the text once typed', () => {
    render(<DeckStudio code={DECK} messageId="m1" onClose={() => {}} />);
    const bar = screen.getByLabelText('Slide instruction');
    // The bar autofocuses; while it is empty the arrows still page slides.
    fireEvent.keyDown(bar, { key: 'ArrowRight' });
    expect(screen.getByRole('listitem', { name: 'Slide 2' })).toHaveAttribute('aria-current', 'true');
    // Once there is text to edit, the field keeps the arrows (no navigation).
    fireEvent.change(bar, { target: { value: 'make the title bold' } });
    fireEvent.keyDown(bar, { key: 'ArrowRight' });
    expect(screen.getByRole('listitem', { name: 'Slide 2' })).toHaveAttribute('aria-current', 'true');
  });

  it('an instruction sends ONLY the selected slide, swaps the answer in place, writes back, and undoes', async () => {
    refineSlide.mockResolvedValue({ section: slide('Two!'), model: 'm', attempts: 1, job_id: 'j' });
    render(<DeckStudio code={DECK} messageId="m1" initialIndex={1} onClose={() => {}} />);
    fireEvent.change(screen.getByLabelText('Slide instruction'), { target: { value: 'bigger title' } });
    fireEvent.click(screen.getByText('Apply'));
    await waitFor(() => expect(refineSlide).toHaveBeenCalledTimes(1));
    expect(refineSlide.mock.calls[0][0]).toMatchObject({
      mode: 'refine',
      instruction: 'bigger title',
      slide: slide('Two'),
      reference: slide('Cover'),
      position: '2 of 3',
    });
    await waitFor(() => expect(deckInMessage()).toEqual(['Cover', 'Two!', 'Three']));
    expect(screen.getByTestId('slide-run')).toHaveAttribute('data-job-id', 'j');
    expect(useSessionStore.getState().messages).toEqual(expect.arrayContaining([
      expect.objectContaining({ role: 'user', content: 'Slide 2: bigger title' }),
      expect.objectContaining({ executionId: 'j', resultType: 'trace' }),
    ]));
    fireEvent.click(screen.getByText('Open model request'));
    expect(screen.getByText('The slide-edit prompt')).toBeInTheDocument();
    fireEvent.click(screen.getByText('Back to run activity'));
    expect(screen.getByText('3 slides · 1 edit')).toBeInTheDocument();
    fireEvent.click(screen.getByTitle('Undo: Refined slide 2'));
    expect(deckInMessage()).toEqual(['Cover', 'Two', 'Three']);
  });

  it('shows pending activity and retains the run trace when no slide is returned', async () => {
    let resolve!: (result: unknown) => void;
    refineSlide.mockReturnValue(new Promise(done => { resolve = done; }));
    render(<DeckStudio code={DECK} messageId="m1" onClose={() => {}} />);
    fireEvent.change(screen.getByLabelText('Slide instruction'), { target: { value: 'bigger title' } });
    fireEvent.click(screen.getByText('Apply'));
    expect(screen.getByTestId('slide-run')).toHaveAttribute('data-running', 'true');
    act(() => refineSlide.mock.calls[0][1]('failed-edit'));
    expect(screen.getByTestId('slide-run')).toHaveAttribute('data-job-id', 'failed-edit');
    await act(async () => resolve({ section: null, error: 'No slide returned', job_id: 'failed-edit' }));
    expect(screen.getByTestId('slide-run')).toHaveAttribute('data-running', 'false');
    expect(screen.getByTestId('slide-run')).toHaveAttribute('data-job-id', 'failed-edit');
    expect(useSessionStore.getState().messages).toEqual(expect.arrayContaining([
      expect.objectContaining({ executionId: 'failed-edit', resultData: expect.objectContaining({ label: 'Slide edit failed' }) }),
    ]));
    expect(deckInMessage()).toEqual(['Cover', 'Two', 'Three']);
  });

  it('keeps the activity in the originating session after the studio closes', async () => {
    useSessionStore.setState({ currentSessionId: 'owner' });
    const post = vi.spyOn(useSessionStore.getState(), 'addMessageToTargetSession').mockReturnValue('edit-step');
    const update = vi.spyOn(useSessionStore.getState(), 'updateMessageInTargetSession').mockImplementation(() => {});
    let resolve!: (result: unknown) => void;
    refineSlide.mockReturnValue(new Promise(done => { resolve = done; }));
    const { unmount } = render(<DeckStudio code={DECK} messageId="m1" onDeckChange={async () => {}} onClose={() => {}} />);
    fireEvent.change(screen.getByLabelText('Slide instruction'), { target: { value: 'bigger title' } });
    fireEvent.click(screen.getByText('Apply'));
    expect(post).toHaveBeenCalledWith('owner', 'user', 'Slide 1: bigger title', undefined);
    unmount();
    useSessionStore.setState({ currentSessionId: 'other', messages: [] });
    await act(async () => resolve({ section: slide('Updated'), job_id: 'edit-job' }));
    expect(update).toHaveBeenCalledWith('owner', 'edit-step', expect.objectContaining({ executionId: 'edit-job' }));
    expect(useSessionStore.getState().messages).toEqual([]);
  });

  it('the studio follows the message: an edit written back from elsewhere shows up', () => {
    render(<DeckStudio code={DECK} messageId="m1" onClose={() => {}} />);
    expect(screen.getAllByRole('listitem')).toHaveLength(3);
    // Simulate the deck card re-rendering the studio with new content.
    const { rerender } = render(<DeckStudio code={[slide('A'), slide('B')].join('\n')} messageId="m1" onClose={() => {}} />);
    rerender(<DeckStudio code={[slide('A'), slide('B'), slide('C'), slide('D')].join('\n')} messageId="m1" onClose={() => {}} />);
    expect(screen.getAllByRole('dialog').length).toBeGreaterThan(0);
  });

  it('a slide handed back unchanged is said so, not silently kept', async () => {
    refineSlide.mockResolvedValue({ section: slide('Two'), model: 'm', attempts: 1 });
    render(<DeckStudio code={DECK} messageId="m1" initialIndex={1} onClose={() => {}} />);
    fireEvent.change(screen.getByLabelText('Slide instruction'), { target: { value: 'x' } });
    fireEvent.click(screen.getByText('Apply'));
    await waitFor(() => expect(screen.getByRole('alert')).toHaveTextContent('unchanged'));
    expect(screen.queryByText(/1 edit/)).toBeNull();
  });

  it('a reply without a slide is shown as an error and changes nothing', async () => {
    refineSlide.mockResolvedValue({ section: null, error: 'The model did not return a slide.' });
    render(<DeckStudio code={DECK} messageId="m1" onClose={() => {}} />);
    fireEvent.change(screen.getByLabelText('Slide instruction'), { target: { value: 'x' } });
    fireEvent.keyDown(screen.getByLabelText('Slide instruction'), { key: 'Enter' });
    await waitFor(() => expect(screen.getByRole('alert')).toHaveTextContent('did not return a slide'));
    expect(deckInMessage()).toEqual(['Cover', 'Two', 'Three']);
  });

  it('structural edits are instant: delete, duplicate, add-between, reorder', async () => {
    refineSlide.mockResolvedValue({ section: slide('New'), model: 'm', attempts: 1 });
    render(<DeckStudio code={DECK} messageId="m1" onClose={() => {}} />);
    fireEvent.click(screen.getByLabelText('Delete slide 3'));
    expect(deckInMessage()).toEqual(['Cover', 'Two']);
    fireEvent.click(screen.getByLabelText('Duplicate slide 1'));
    expect(deckInMessage()).toEqual(['Cover', 'Cover', 'Two']);
    // "+" inserts a blank slide RIGHT AWAY, then the bar offers to write it.
    fireEvent.click(screen.getByLabelText('Add a slide at position 2'));
    expect(deckInMessage()).toEqual(['Cover', undefined, 'Cover', 'Two']);
    expect(screen.getByText('New slide 2 — what should it cover?')).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText('New slide instruction'), { target: { value: 'agenda' } });
    fireEvent.click(screen.getByText('Apply'));
    await waitFor(() => expect(deckInMessage()).toEqual(['Cover', 'New', 'Cover', 'Two']));
    expect(refineSlide.mock.calls[0][0]).toMatchObject({ mode: 'add', before: slide('Cover'), after: slide('Cover'), position: '2 of 4' });
    // The always-visible button appends one at the end; "leave it blank" keeps it.
    fireEvent.click(screen.getByText('Add slide'));
    expect(deckInMessage()).toEqual(['Cover', 'New', 'Cover', 'Two', undefined]);
    fireEvent.click(screen.getByText('leave it blank'));
    // Drag slide 4 onto slide 1.
    const from = screen.getByRole('listitem', { name: 'Slide 4' });
    const to = screen.getByRole('listitem', { name: 'Slide 1' });
    fireEvent.dragStart(from);
    fireEvent.dragOver(to);
    fireEvent.drop(to);
    expect(deckInMessage()).toEqual(['Two', 'Cover', 'New', 'Cover', undefined]);
  });

  it('arrow keys page the deck from inside the studio, and while presenting', () => {
    render(<DeckStudio code={DECK} messageId="m1" onClose={() => {}} />);
    // Focus sits on a control inside the studio (not the textarea): the key
    // must still reach the window listener.
    const present = screen.getByTitle('Present');
    present.focus();
    fireEvent.keyDown(present, { key: 'ArrowRight', bubbles: true });
    expect(screen.getByText('Slide 2')).toBeInTheDocument();
    fireEvent.click(present);
    // Presenting: the presentation view owns the keys, on the same window.
    fireEvent.keyDown(document.activeElement || document.body, { key: 'ArrowRight', bubbles: true });
    expect(screen.getByText('Slide 3')).toBeInTheDocument();
    fireEvent.keyDown(document.activeElement || document.body, { key: 'ArrowLeft', bubbles: true });
    expect(screen.getByText('Slide 2')).toBeInTheDocument();
  });

  const deferred = <T,>() => {
    let resolve!: (value: T) => void;
    let reject!: (error: Error) => void;
    const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
    return { promise, resolve, reject };
  };
  const submitSlide = (number: number, instruction = 'Improve this slide') => {
    fireEvent.click(screen.getByLabelText(`Select slide ${number}`));
    fireEvent.change(screen.getByLabelText('Slide instruction'), { target: { value: instruction } });
    fireEvent.click(screen.getByText('Apply'));
  };

  it.each([[0, 1], [1, 0]])('merges parallel edits completing in order %s then %s, preserving focus and undo', async (first, second) => {
    const runs = [deferred<{ section: string; job_id: string }>(), deferred<{ section: string; job_id: string }>()];
    refineSlide.mockReturnValueOnce(runs[0].promise).mockReturnValueOnce(runs[1].promise);
    render(<DeckStudio code={DECK} messageId="m1" onClose={() => {}} />);
    submitSlide(1);
    expect(screen.getByLabelText('Slide instruction')).toBeDisabled();
    submitSlide(2);
    expect(refineSlide).toHaveBeenCalledTimes(2);
    expect(screen.getByRole('listitem', { name: 'Slide 1' })).toHaveAttribute('aria-busy', 'true');
    expect(screen.getByRole('listitem', { name: 'Slide 2' })).toHaveAttribute('aria-busy', 'true');
    expect(screen.getByLabelText('Delete slide 3')).toBeDisabled();
    expect(screen.getByLabelText('Duplicate slide 1')).toBeDisabled();
    expect(screen.getByText('Add slide')).toBeDisabled();
    act(() => {
      refineSlide.mock.calls[0][1]('job-1');
      refineSlide.mock.calls[1][1]('job-2');
    });
    expect(screen.getByTestId('slide-run')).toHaveAttribute('data-job-id', 'job-2');
    fireEvent.click(screen.getByLabelText('Select slide 1'));
    expect(screen.getByTestId('slide-run')).toHaveAttribute('data-job-id', 'job-1');
    fireEvent.click(screen.getByLabelText('Select slide 3'));
    expect(screen.getByLabelText('Slide instruction')).toBeEnabled();
    expect(screen.queryByTestId('slide-run')).not.toBeInTheDocument();
    const replacements = ['Cover updated', 'Two updated'];
    await act(async () => runs[first].resolve({ section: slide(replacements[first]), job_id: `job-${first + 1}` }));
    expect(screen.getByRole('listitem', { name: `Slide ${second + 1}` })).toHaveAttribute('aria-busy', 'true');
    const intermediate = deckInMessage();
    await act(async () => runs[second].resolve({ section: slide(replacements[second]), job_id: `job-${second + 1}` }));
    expect(deckInMessage()).toEqual(['Cover updated', 'Two updated', 'Three']);
    expect(screen.getByRole('listitem', { name: 'Slide 3' })).toHaveAttribute('aria-current', 'true');
    expect(screen.getByText('3 slides · 2 edits')).toBeInTheDocument();
    fireEvent.click(screen.getByTitle(`Undo: Refined slide ${second + 1}`));
    expect(deckInMessage()).toEqual(intermediate);
    await act(async () => {});
    fireEvent.click(screen.getByTitle(`Undo: Refined slide ${first + 1}`));
    expect(deckInMessage()).toEqual(['Cover', 'Two', 'Three']);
  });

  it('queues overlapping saves and rebases the second save onto the first saved deck', async () => {
    const runs = [deferred<{ section: string }>(), deferred<{ section: string }>()];
    const saves = [deferred<void>(), deferred<void>()];
    const onDeckChange = vi.fn().mockReturnValueOnce(saves[0].promise).mockReturnValueOnce(saves[1].promise);
    refineSlide.mockReturnValueOnce(runs[0].promise).mockReturnValueOnce(runs[1].promise);
    render(<DeckStudio code={DECK} onDeckChange={onDeckChange} onClose={() => {}} />);
    submitSlide(1);
    submitSlide(2);
    await act(async () => {
      runs[0].resolve({ section: slide('Cover updated') });
      runs[1].resolve({ section: slide('Two updated') });
    });
    expect(onDeckChange).toHaveBeenCalledTimes(1);
    expect(screen.getByLabelText('Slide instruction')).toBeDisabled();
    await act(async () => saves[0].resolve());
    expect(onDeckChange).toHaveBeenCalledTimes(2);
    const [next, previous] = onDeckChange.mock.calls[1];
    expect(titles(previous)).toEqual(['Cover updated', 'Two', 'Three']);
    expect(titles(next)).toEqual(['Cover updated', 'Two updated', 'Three']);
    await act(async () => saves[1].resolve());
    expect(screen.getByLabelText('Slide instruction')).toBeEnabled();
  });

  it('a failed save does not block another slide or appear in its activity', async () => {
    const runs = [deferred<{ section: string }>(), deferred<{ section: string }>()];
    const firstSave = deferred<void>();
    const onDeckChange = vi.fn().mockReturnValueOnce(firstSave.promise).mockResolvedValueOnce(undefined);
    refineSlide.mockReturnValueOnce(runs[0].promise).mockReturnValueOnce(runs[1].promise);
    render(<DeckStudio code={DECK} onDeckChange={onDeckChange} onClose={() => {}} />);
    submitSlide(1);
    submitSlide(2);
    await act(async () => {
      runs[0].resolve({ section: slide('Cover updated') });
      runs[1].resolve({ section: slide('Two updated') });
    });
    await act(async () => firstSave.reject(new Error('Save unavailable')));
    expect(titles(onDeckChange.mock.calls[1][0])).toEqual(['Cover', 'Two updated', 'Three']);
    expect(onDeckChange.mock.calls[1][1]).toBe(DECK);
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
    fireEvent.click(screen.getByLabelText('Select slide 1'));
    expect(screen.getByRole('alert')).toHaveTextContent('Save unavailable');
    expect(screen.getByLabelText('Slide instruction')).toBeEnabled();
    expect(screen.getByText('3 slides · 1 edit')).toBeInTheDocument();
  });

  it('rejects a stale result if the target changed externally during generation', async () => {
    const run = deferred<{ section: string }>();
    const onDeckChange = vi.fn().mockResolvedValue(undefined);
    refineSlide.mockReturnValue(run.promise);
    const { rerender } = render(<DeckStudio code={DECK} onDeckChange={onDeckChange} onClose={() => {}} />);
    submitSlide(1);
    rerender(<DeckStudio code={[slide('External edit'), slide('Two'), slide('Three')].join('\n')}
      onDeckChange={onDeckChange} onClose={() => {}} />);
    await act(async () => run.resolve({ section: slide('Stale edit') }));
    expect(onDeckChange).not.toHaveBeenCalled();
    expect(screen.getByRole('alert')).toHaveTextContent('changed while the edit was running');
  });

  it('Escape closes', () => {
    const onClose = vi.fn();
    render(<DeckStudio code={DECK} onClose={onClose} />);
    act(() => {
      fireEvent.keyDown(screen.getByRole('dialog', { name: 'Deck studio' }), { key: 'Escape' });
    });
    expect(onClose).toHaveBeenCalled();
  });
});
