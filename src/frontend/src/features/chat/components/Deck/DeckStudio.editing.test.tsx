import { describe, expect, it, vi, beforeEach } from 'vitest';
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import DeckStudio from './DeckStudio';
import HtmlDeckBlock from '../Chat/HtmlDeckBlock';
import { fetchEnabledModelRows as fetchEnabledModels } from '../../../../api/config/EnabledModelsService';
import { resetModelsStoreForTests } from '../../../../store/models';
import { useAppStore } from '../../store/appStore';
import { useSessionStore } from '../../../../app/sessions/sessionStore';
import { splitSlides } from '../../utils/htmlDeck';
import { SLIDE_EDIT_MSG } from '../../utils/slideEdit';

vi.mock('../../../../api/config/EnabledModelsService', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../../../../api/config/EnabledModelsService')>()),
  fetchEnabledModelRows: vi.fn(),
}));
const refineSlide = vi.fn();
vi.mock('../../../../api/chat/DeckService', () => ({
  DeckService: { refineSlide: (...args: unknown[]) => refineSlide(...args) },
}));
const upload = vi.fn();
vi.mock('../../../../api/chat/AssetService', () => ({
  AssetService: {
    upload: (...args: unknown[]) => upload(...args),
    dataUrl: () => Promise.resolve('data:image/gif;base64,R0lGODlhAQABAAAAACw='),
  },
}));
vi.mock('../../utils/imageFiles', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../../utils/imageFiles')>()),
  measureImage: () => Promise.resolve({ width: 10, height: 10 }),
}));
vi.mock('../../persistence/sessionApi', () => ({
  addMessageToSession: vi.fn().mockResolvedValue(undefined),
  updateMessageInSession: vi.fn().mockResolvedValue(undefined),
}));

// Element numbers: the slide is "0", its <h1> is "1".
const slide = (t: string) => `<section class="slide"><h1>${t}</h1></section>`;
const DECK = [slide('Cover'), slide('Two'), slide('Three')].join('\n');
const titles = (html: string) => splitSlides(html).map((s) => s.match(/<h1>(.*?)<\/h1>/)?.[1]);
const content = () => useSessionStore.getState().messages.find((x) => x.id === 'm1')?.content ?? '';
const deckInMessage = () => titles(content());
const setDeck = (deck: string) =>
  useSessionStore.setState({
    messages: [{ id: 'm1', role: 'assistant', content: 'Deck:\n```html\n' + deck + '\n```', timestamp: new Date() } as never],
    currentSessionId: 'owner',
  } as never);

const stageDoc = (n: number) => screen.getByTitle(`Slide ${n}`).getAttribute('srcdoc') || '';
// The key the stage's editor script reports under: its call's first argument.
const frameKey = (n: number): string => {
  const m = stageDoc(n).match(new RegExp(`\\)\\(("[^"]*"),${JSON.stringify(SLIDE_EDIT_MSG)}\\);`));
  if (!m) throw new Error(`slide ${n} is not editable`);
  return JSON.parse(m[1]) as string;
};
// What the stage's script posts (jsdom runs no frame scripts).
const post = (data: Record<string, unknown>) =>
  act(() => {
    window.dispatchEvent(new MessageEvent('message', { data: { t: SLIDE_EDIT_MSG, ...data } }));
  });
const type = (key: string, html: string) => post({ id: key, op: 'html', k: '1', html });
const save = (key: string) => post({ id: key, op: 'commit' });

describe('DeckStudio editing on the stage', () => {
  beforeEach(() => {
    refineSlide.mockReset();
    upload.mockReset();
    resetModelsStoreForTests();
    useAppStore.setState({ selectedModel: 'chat-model' });
    vi.mocked(fetchEnabledModels).mockResolvedValue([
      { key: 'chat-model', name: 'Chat model' },
    ] as Awaited<ReturnType<typeof fetchEnabledModels>>);
    setDeck(DECK);
  });

  it('makes the slide editable with no mode, and saves when the caret leaves', () => {
    render(<DeckStudio code={DECK} messageId="m1" onClose={() => {}} />);
    expect(screen.queryByText(/edit text/i)).not.toBeInTheDocument();
    expect(stageDoc(1)).toContain('data-kedit');
    expect(screen.getByLabelText('Slide instruction')).toBeInTheDocument();

    const key = frameKey(1);
    type(key, 'Renamed cover');
    // Nothing is written while typing.
    expect(deckInMessage()).toEqual(['Cover', 'Two', 'Three']);

    save(key);
    expect(deckInMessage()).toEqual(['Renamed cover', 'Two', 'Three']);
    expect(screen.getByText('3 slides · 1 edit')).toBeInTheDocument();
    // The frame keeps its document (and the caret) across the save.
    expect(frameKey(1)).toBe(key);
    // A second save with nothing new adds no edit.
    save(key);
    expect(screen.getByText('3 slides · 1 edit')).toBeInTheDocument();

    fireEvent.click(screen.getByTitle('Undo: Edited slide 1'));
    expect(deckInMessage()).toEqual(['Cover', 'Two', 'Three']);
    // Undo changed the slide, so the frame is rebuilt from the deck.
    expect(frameKey(1)).not.toBe(key);
  });

  it('ignores messages from other frames and malformed ones', () => {
    render(<DeckStudio code={DECK} messageId="m1" onClose={() => {}} />);
    const key = frameKey(1);
    post({ id: 'some-other-frame', op: 'html', k: '1', html: 'Hijacked' });
    post({ id: 'some-other-frame', op: 'commit' });
    post({ id: key, op: 'html', k: '1', html: 42 });
    post({ id: key, op: 'html', k: '../1', html: 'x' });
    post({ id: key, op: 'explode', k: '1' });
    save(key);
    expect(deckInMessage()).toEqual(['Cover', 'Two', 'Three']);
    expect(screen.getByText('3 slides')).toBeInTheDocument();
  });

  it('saves a removed text box', async () => {
    render(<DeckStudio code={DECK} messageId="m1" onClose={() => {}} />);
    const key = frameKey(1);
    post({ id: key, op: 'remove', k: '1' });
    save(key);
    expect(splitSlides(content())[0]).toMatch(/^<section[^>]*class="slide"[^>]*><\/section>$/);
    expect(deckInMessage()).toEqual([undefined, 'Two', 'Three']);
    // Let the model picker finish loading inside the test.
    await screen.findByRole('option', { name: 'Chat model' });
  });

  it('saves the slide when another slide is selected', () => {
    render(<DeckStudio code={DECK} messageId="m1" onClose={() => {}} />);
    const first = frameKey(1);
    type(first, 'New cover');
    fireEvent.click(screen.getByLabelText('Select slide 2'));
    expect(deckInMessage()).toEqual(['New cover', 'Two', 'Three']);
    const second = frameKey(2);
    expect(second).not.toBe(first);
    type(second, 'New two');
    save(second);
    expect(deckInMessage()).toEqual(['New cover', 'New two', 'Three']);
    expect(screen.getByText('3 slides · 2 edits')).toBeInTheDocument();
  });

  it('saves typed text before a slide is moved', async () => {
    render(<DeckStudio code={DECK} messageId="m1" onClose={() => {}} />);
    type(frameKey(1), 'Typed cover');
    fireEvent.click(screen.getByLabelText('Move slide 1 down'));
    await waitFor(() => expect(deckInMessage()).toEqual(['Two', 'Typed cover', 'Three']));
    expect(screen.getByText('3 slides · 2 edits')).toBeInTheDocument();
  });

  it('takes back typing that was not saved yet when Undo is clicked', async () => {
    render(<DeckStudio code={DECK} messageId="m1" onClose={() => {}} />);
    const key = frameKey(1);
    type(key, 'A');
    save(key);
    type(key, 'AB');
    fireEvent.click(screen.getByTitle('Undo: Edited slide 1'));
    await waitFor(() => expect(screen.getByText('3 slides · 1 edit')).toBeInTheDocument());
    expect(deckInMessage()).toEqual(['A', 'Two', 'Three']);
  });

  it('saves what was done when the studio is closed', async () => {
    const onDeckChange = vi.fn().mockResolvedValue(undefined);
    render(<HtmlDeckBlock code={DECK} onDeckChange={onDeckChange} />);
    fireEvent.click(screen.getByTitle('Edit deck'));
    type(frameKey(1), 'Saved on close');
    fireEvent.keyDown(screen.getByRole('dialog', { name: 'Deck studio' }), { key: 'Escape' });
    expect(screen.queryByRole('dialog', { name: 'Deck studio' })).not.toBeInTheDocument();
    await waitFor(() => expect(onDeckChange).toHaveBeenCalledTimes(1));
    expect(titles(onDeckChange.mock.calls[0][0])).toEqual(['Saved on close', 'Two', 'Three']);
  });

  it('does not make a slide editable while the model rewrites it', async () => {
    refineSlide.mockReturnValue(new Promise(() => {}));
    render(<DeckStudio code={DECK} messageId="m1" onClose={() => {}} />);
    const bar = screen.getByLabelText('Slide instruction');
    fireEvent.change(bar, { target: { value: 'Make it bold' } });
    fireEvent.keyDown(bar, { key: 'Enter' });
    await waitFor(() => expect(refineSlide).toHaveBeenCalledTimes(1));
    expect(stageDoc(1)).not.toContain('data-kedit');
    fireEvent.click(screen.getByLabelText('Select slide 2'));
    expect(stageDoc(2)).toContain('data-kedit');
  });

  describe('images', () => {
    // The image is "2": the slide is "0", the heading "1".
    const PICTURE = '<section class="slide"><h1>Logo</h1><img src="asset:oldimage01"></section>';
    const DECK_WITH_IMAGE = [PICTURE, slide('Two')].join('\n');
    const chooseFile = (name = 'new.png', mime = 'image/png') =>
      fireEvent.change(screen.getByTestId('deck-image-input'), { target: { files: [new File(['x'], name, { type: mime })] } });

    beforeEach(() => setDeck(DECK_WITH_IMAGE));

    it('replaces the picked image with an uploaded one', async () => {
      upload.mockResolvedValue({ id: 'newimage99', ref: 'asset:newimage99' });
      render(<DeckStudio code={DECK_WITH_IMAGE} messageId="m1" onClose={() => {}} />);
      await waitFor(() => expect(stageDoc(1)).toContain('data-kimg'));
      post({ id: frameKey(1), op: 'pick', k: '2' });
      chooseFile();
      await waitFor(() => expect(content()).toContain('src="asset:newimage99"'));
      expect(upload).toHaveBeenCalledWith(expect.any(File), { sessionId: 'owner', width: 10, height: 10 });
      expect(content()).not.toContain('oldimage01');
      expect(screen.getByText('2 slides · 1 edit')).toBeInTheDocument();
    });

    it('refuses a file that is not an image', async () => {
      render(<DeckStudio code={DECK_WITH_IMAGE} messageId="m1" onClose={() => {}} />);
      await waitFor(() => expect(stageDoc(1)).toContain('data-kimg'));
      post({ id: frameKey(1), op: 'pick', k: '2' });
      chooseFile('notes.svg', 'image/svg+xml');
      expect(await screen.findByRole('alert')).toHaveTextContent('Choose a PNG, JPEG, WebP or GIF image.');
      expect(upload).not.toHaveBeenCalled();
    });

    it('does not put an image on another slide when the person moved on during the upload', async () => {
      let finish: (asset: { id: string }) => void = () => {};
      upload.mockReturnValue(new Promise((resolve) => { finish = resolve; }));
      render(<DeckStudio code={DECK_WITH_IMAGE} messageId="m1" onClose={() => {}} />);
      await waitFor(() => expect(stageDoc(1)).toContain('data-kimg'));
      post({ id: frameKey(1), op: 'pick', k: '2' });
      chooseFile();
      await waitFor(() => expect(upload).toHaveBeenCalled());
      fireEvent.click(screen.getByLabelText('Select slide 2'));
      await act(async () => finish({ id: 'lateimage77' }));
      expect(content()).not.toContain('lateimage77');
      expect(screen.getByRole('alert')).toHaveTextContent('The slide changed before the image was uploaded');
    });
  });
});
