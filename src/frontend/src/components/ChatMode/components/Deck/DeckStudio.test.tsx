import { describe, expect, it, vi, beforeEach } from 'vitest';
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import DeckStudio from './DeckStudio';
import { useSessionStore } from '../../store/sessionStore';
import { splitSlides } from '../../utils/htmlDeck';

const refineSlide = vi.fn();
vi.mock('../../../../api/chat/DeckService', () => ({
  DeckService: { refineSlide: (...args: unknown[]) => refineSlide(...args) },
}));
vi.mock('../../utils/deckExport', () => ({ downloadDeckPdf: vi.fn(), downloadDeckPptx: vi.fn() }));

const slide = (t: string) => `<section class="slide"><h1>${t}</h1></section>`;
const DECK = [slide('Cover'), slide('Two'), slide('Three')].join('\n');
const titles = (html: string) => splitSlides(html).map((s) => s.match(/<h1>(.*?)<\/h1>/)?.[1]);
const deckInMessage = () => {
  const m = useSessionStore.getState().messages.find((x) => x.id === 'm1');
  return m ? titles(m.content) : null;
};

describe('DeckStudio', () => {
  beforeEach(() => {
    refineSlide.mockReset();
    useSessionStore.setState({
      messages: [{ id: 'm1', role: 'assistant', content: 'Deck:\n```html\n' + DECK + '\n```', timestamp: new Date() } as never],
      currentSessionId: null,
    } as never);
  });

  it('shows every slide in the rail, selects on click, and pages with the keys', () => {
    render(<DeckStudio code={DECK} messageId="m1" onClose={() => {}} />);
    expect(screen.getAllByRole('listitem')).toHaveLength(3);
    expect(screen.getByRole('listitem', { name: 'Slide 1' })).toHaveAttribute('aria-current', 'true');
    fireEvent.click(screen.getByLabelText('Select slide 3'));
    expect(screen.getByRole('listitem', { name: 'Slide 3' })).toHaveAttribute('aria-current', 'true');
    fireEvent.keyDown(window, { key: 'ArrowLeft' });
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
    expect(screen.getByText('3 slides · 1 edit')).toBeInTheDocument();
    fireEvent.click(screen.getByTitle('Undo: Refined slide 2'));
    expect(deckInMessage()).toEqual(['Cover', 'Two', 'Three']);
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
    fireEvent.click(screen.getByLabelText('Add a slide at position 2'));
    expect(screen.getByText('New slide at position 2')).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText('New slide instruction'), { target: { value: 'agenda' } });
    fireEvent.click(screen.getByText('Apply'));
    await waitFor(() => expect(deckInMessage()).toEqual(['Cover', 'New', 'Cover', 'Two']));
    expect(refineSlide.mock.calls[0][0]).toMatchObject({ mode: 'add', before: slide('Cover'), after: slide('Cover'), position: '2 of 4' });
    // Drag slide 4 onto slide 1.
    const from = screen.getByRole('listitem', { name: 'Slide 4' });
    const to = screen.getByRole('listitem', { name: 'Slide 1' });
    fireEvent.dragStart(from);
    fireEvent.dragOver(to);
    fireEvent.drop(to);
    expect(deckInMessage()).toEqual(['Two', 'Cover', 'New', 'Cover']);
  });

  it('Escape closes', () => {
    const onClose = vi.fn();
    render(<DeckStudio code={DECK} onClose={onClose} />);
    act(() => {
      fireEvent.keyDown(window, { key: 'Escape' });
    });
    expect(onClose).toHaveBeenCalled();
  });
});
