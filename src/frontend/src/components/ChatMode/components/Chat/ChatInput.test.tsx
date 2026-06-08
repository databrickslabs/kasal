import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, within, act } from '@testing-library/react';
import ChatInput from './ChatInput';
import type { ModelConfigResponse } from '../../types/dispatcher';
import { uploadKnowledgeFile } from '../../api/knowledge';

vi.mock('../../api/knowledge', () => ({ uploadKnowledgeFile: vi.fn() }));
const mockUpload = vi.mocked(uploadKnowledgeFile);

const MODELS: ModelConfigResponse[] = [
  { key: 'm1', name: 'Model One', provider: 'databricks' } as ModelConfigResponse,
  { key: 'm2', name: 'Model Two' } as ModelConfigResponse,
];

const baseProps = {
  onSend: vi.fn(),
  models: MODELS,
  selectedModel: 'm1',
  onModelChange: vi.fn(),
};

const ta = () => screen.getByPlaceholderText('Ask a question...') as HTMLTextAreaElement;

beforeEach(() => {
  vi.clearAllMocks();
});

describe('ChatInput — typing & send', () => {
  it('types and sends on Enter (no shift)', () => {
    const onSend = vi.fn();
    render(<ChatInput {...baseProps} onSend={onSend} />);
    fireEvent.change(ta(), { target: { value: 'hello' } });
    fireEvent.keyDown(ta(), { key: 'Enter', shiftKey: false });
    expect(onSend).toHaveBeenCalledWith('hello');
    expect(ta().value).toBe('');
  });

  it('Shift+Enter does not send', () => {
    const onSend = vi.fn();
    render(<ChatInput {...baseProps} onSend={onSend} />);
    fireEvent.change(ta(), { target: { value: 'multi' } });
    fireEvent.keyDown(ta(), { key: 'Enter', shiftKey: true });
    expect(onSend).not.toHaveBeenCalled();
  });

  it('does not send empty/whitespace, and the send button is disabled when empty', () => {
    const onSend = vi.fn();
    render(<ChatInput {...baseProps} onSend={onSend} />);
    fireEvent.change(ta(), { target: { value: '   ' } });
    fireEvent.keyDown(ta(), { key: 'Enter' });
    expect(onSend).not.toHaveBeenCalled();
  });

  it('clicking the send button sends', () => {
    const onSend = vi.fn();
    const { container } = render(<ChatInput {...baseProps} onSend={onSend} />);
    fireEvent.change(ta(), { target: { value: 'click send' } });
    // the send button is the last button in the bottom row
    const buttons = container.querySelectorAll('button');
    fireEvent.click(buttons[buttons.length - 1]);
    expect(onSend).toHaveBeenCalledWith('click send');
  });

  it('disabled prop blocks send and shows the loading spinner', () => {
    const onSend = vi.fn();
    render(<ChatInput {...baseProps} onSend={onSend} disabled />);
    fireEvent.change(ta(), { target: { value: 'x' } });
    fireEvent.keyDown(ta(), { key: 'Enter' });
    expect(onSend).not.toHaveBeenCalled();
  });

  it('auto-resizes on input', () => {
    render(<ChatInput {...baseProps} />);
    fireEvent.input(ta(), { target: { value: 'line' } });
    // handler ran without throwing; height style set
    expect(ta().style.height).toBeDefined();
  });
});

describe('ChatInput — slash command autocomplete', () => {
  it('shows the command list when typing "/" and filters', () => {
    render(<ChatInput {...baseProps} />);
    fireEvent.change(ta(), { target: { value: '/help' } });
    expect(screen.getByText('Commands')).toBeInTheDocument();
    expect(screen.getAllByText('/help').length).toBeGreaterThan(0);
  });

  it('hides the list when no command matches', () => {
    render(<ChatInput {...baseProps} />);
    fireEvent.change(ta(), { target: { value: '/zzzz' } });
    expect(screen.queryByText('Commands')).not.toBeInTheDocument();
  });

  it('ArrowDown/ArrowUp navigate the list (with wraparound) and Enter selects', () => {
    const onSend = vi.fn();
    render(<ChatInput {...baseProps} onSend={onSend} />);
    fireEvent.change(ta(), { target: { value: '/list' } }); // matches /list crews, /list flows
    fireEvent.keyDown(ta(), { key: 'ArrowDown' });
    fireEvent.keyDown(ta(), { key: 'ArrowUp' });
    fireEvent.keyDown(ta(), { key: 'ArrowUp' }); // wrap to last
    // Enter selects current command (these have no trailing space -> sent immediately)
    fireEvent.keyDown(ta(), { key: 'Enter' });
    expect(onSend).toHaveBeenCalled();
  });

  it('Tab selects a command that needs a param (kept in the box, not sent)', () => {
    const onSend = vi.fn();
    render(<ChatInput {...baseProps} onSend={onSend} />);
    fireEvent.change(ta(), { target: { value: '/load crew' } }); // "/load crew " has trailing space
    fireEvent.keyDown(ta(), { key: 'Tab' });
    expect(ta().value).toBe('/load crew ');
    expect(onSend).not.toHaveBeenCalled();
  });

  it('clicking a command in the list selects it', () => {
    const onSend = vi.fn();
    render(<ChatInput {...baseProps} onSend={onSend} />);
    fireEvent.change(ta(), { target: { value: '/help' } });
    fireEvent.click(screen.getAllByText('/help')[0]);
    // /help has no trailing space -> sent immediately
    expect(onSend).toHaveBeenCalledWith('/help');
  });

  it('mouseEnter on a command updates the selected index', () => {
    render(<ChatInput {...baseProps} />);
    fireEvent.change(ta(), { target: { value: '/list' } });
    const cmd = screen.getByText('/list flows');
    fireEvent.mouseEnter(cmd.closest('button')!);
    expect(cmd).toBeInTheDocument();
  });

  it('Escape closes the command list', () => {
    render(<ChatInput {...baseProps} />);
    fireEvent.change(ta(), { target: { value: '/help' } });
    expect(screen.getByText('Commands')).toBeInTheDocument();
    fireEvent.keyDown(ta(), { key: 'Escape' });
    expect(screen.queryByText('Commands')).not.toBeInTheDocument();
  });

  it('ignores other keys while the command list is open', () => {
    render(<ChatInput {...baseProps} />);
    fireEvent.change(ta(), { target: { value: '/help' } });
    // a non-navigation key falls through without closing/selecting
    fireEvent.keyDown(ta(), { key: 'a' });
    expect(screen.getByText('Commands')).toBeInTheDocument();
  });

  it('ArrowDown wraps from the last item back to the first', () => {
    render(<ChatInput {...baseProps} />);
    fireEvent.change(ta(), { target: { value: '/list' } }); // 2 matches: crews, flows
    fireEvent.keyDown(ta(), { key: 'ArrowDown' }); // 0 -> 1 (last)
    fireEvent.keyDown(ta(), { key: 'ArrowDown' }); // 1 -> wrap to 0 (the ":0" arm)
    // list is still open after wrapping
    expect(screen.getByText('Commands')).toBeInTheDocument();
  });
});

describe('ChatInput — command history', () => {
  it('walks history backward/forward with ArrowUp/ArrowDown (terminal-style)', () => {
    render(<ChatInput {...baseProps} />);
    // build a 3-entry history
    for (const m of ['first', 'second', 'third']) {
      fireEvent.change(ta(), { target: { value: m } });
      fireEvent.keyDown(ta(), { key: 'Enter' });
    }
    expect(ta().value).toBe('');

    // ArrowUp from empty -> most recent
    fireEvent.keyDown(ta(), { key: 'ArrowUp' });
    expect(ta().value).toBe('third');
    // keep walking back (Math.max branch)
    fireEvent.keyDown(ta(), { key: 'ArrowUp' });
    expect(ta().value).toBe('second');
    fireEvent.keyDown(ta(), { key: 'ArrowUp' });
    expect(ta().value).toBe('first');
    // clamp at the oldest entry
    fireEvent.keyDown(ta(), { key: 'ArrowUp' });
    expect(ta().value).toBe('first');
    // ArrowDown walks forward (else branch)
    fireEvent.keyDown(ta(), { key: 'ArrowDown' });
    expect(ta().value).toBe('second');
    fireEvent.keyDown(ta(), { key: 'ArrowDown' });
    expect(ta().value).toBe('third');
    // past the newest -> reset to empty
    fireEvent.keyDown(ta(), { key: 'ArrowDown' });
    expect(ta().value).toBe('');
  });

  it('ArrowDown with no active history index is a no-op', () => {
    render(<ChatInput {...baseProps} />);
    fireEvent.change(ta(), { target: { value: 'a' } });
    fireEvent.keyDown(ta(), { key: 'Enter' });
    // historyIndex is -1; ArrowDown should do nothing harmful
    fireEvent.keyDown(ta(), { key: 'ArrowDown' });
    expect(ta().value).toBe('');
  });
});

describe('ChatInput — model picker', () => {
  it('opens the picker, selects a model, and closes', () => {
    const onModelChange = vi.fn();
    render(<ChatInput {...baseProps} onModelChange={onModelChange} />);
    // toggle button shows current model name
    fireEvent.click(screen.getByText('Model One'));
    expect(screen.getByText('Model')).toBeInTheDocument(); // dropdown header
    // both models listed; one with provider, one without
    const picker = screen.getByText('Model').closest('div')!.parentElement!;
    fireEvent.click(within(picker).getByText('Model Two'));
    expect(onModelChange).toHaveBeenCalledWith('m2');
  });

  it('outside mousedown closes the model picker', () => {
    render(<ChatInput {...baseProps} />);
    fireEvent.click(screen.getByText('Model One'));
    expect(screen.getByText('Model')).toBeInTheDocument();
    fireEvent.mouseDown(document.body);
    expect(screen.queryByText('Model')).not.toBeInTheDocument();
  });

  it('mousedown inside the picker does not close it', () => {
    render(<ChatInput {...baseProps} />);
    fireEvent.click(screen.getByText('Model One'));
    const header = screen.getByText('Model');
    fireEvent.mouseDown(header);
    // still open
    expect(screen.getByText('Model')).toBeInTheDocument();
  });

  it('opening the model picker closes the command list', () => {
    render(<ChatInput {...baseProps} />);
    fireEvent.change(ta(), { target: { value: '/help' } });
    expect(screen.getByText('Commands')).toBeInTheDocument();
    fireEvent.click(screen.getByText('Model One'));
    expect(screen.queryByText('Commands')).not.toBeInTheDocument();
  });

  it('falls back to a default display name when the selected model is unknown', () => {
    render(<ChatInput {...baseProps} selectedModel="missing" />);
    expect(screen.getByText('missing')).toBeInTheDocument();
  });

  it('renders no model controls when models list is empty', () => {
    render(<ChatInput {...baseProps} models={[]} selectedModel="" />);
    expect(screen.queryByText('Model One')).not.toBeInTheDocument();
  });
});

describe('ChatInput — format selector', () => {
  it('opens the picker, selects a format, and appends its directive on send', () => {
    const onSend = vi.fn();
    render(<ChatInput {...baseProps} onSend={onSend} />);
    // toggle shows the current (auto) label; opening lists every option
    fireEvent.click(screen.getByText('Auto format'));
    expect(screen.getByText('Interactive quiz')).toBeInTheDocument();
    fireEvent.click(screen.getByText('Presentation')); // select → checkmark + close
    expect(screen.getByText('Presentation')).toBeInTheDocument(); // toggle now reflects it (accent)
    // a natural-language prompt gets the directive appended — to the dispatch
    // payload (meta.dispatchSuffix), NOT the visible message.
    fireEvent.change(ta(), { target: { value: 'make slides' } });
    fireEvent.keyDown(ta(), { key: 'Enter' });
    expect(onSend).toHaveBeenCalledWith(
      'make slides',
      expect.objectContaining({ dispatchSuffix: expect.stringContaining('[Output format:') }),
    );
  });

  it('never appends a directive to slash commands', () => {
    const onSend = vi.fn();
    render(<ChatInput {...baseProps} onSend={onSend} />);
    fireEvent.click(screen.getByText('Auto format'));
    fireEvent.click(screen.getByText('Dashboard'));
    fireEvent.change(ta(), { target: { value: '/unknown thing' } });
    fireEvent.keyDown(ta(), { key: 'Enter' });
    expect(onSend).toHaveBeenCalledWith('/unknown thing'); // literal, no directive
  });

  it('outside mousedown closes the format picker', () => {
    render(<ChatInput {...baseProps} />);
    fireEvent.click(screen.getByText('Auto format'));
    expect(screen.getByText('Report')).toBeInTheDocument();
    fireEvent.mouseDown(document.body);
    expect(screen.queryByText('Report')).not.toBeInTheDocument();
  });

  it('opening the format picker closes the command list', () => {
    render(<ChatInput {...baseProps} />);
    fireEvent.change(ta(), { target: { value: '/help' } });
    expect(screen.getByText('Commands')).toBeInTheDocument();
    fireEvent.click(screen.getByText('Auto format'));
    expect(screen.queryByText('Commands')).not.toBeInTheDocument();
  });

  it('mousedown inside the format picker does not close it', () => {
    render(<ChatInput {...baseProps} />);
    fireEvent.click(screen.getByText('Auto format'));
    const option = screen.getByText('Report');
    fireEvent.mouseDown(option); // inside the picker → stays open
    expect(screen.getByText('Report')).toBeInTheDocument();
  });
});

describe('ChatInput — knowledge attachments', () => {
  const fileInput = () => screen.getByTestId('chat-file-input') as HTMLInputElement;
  const selectFiles = (files: File[]) =>
    fireEvent.change(fileInput(), { target: { files } });

  beforeEach(() => {
    mockUpload.mockReset();
  });

  it('uploads a selected file, shows a ready chip with size, and sends with the knowledge tool', async () => {
    mockUpload.mockResolvedValue({ path: '/Volumes/x/a.txt', status: 'success', filename: 'a.txt' });
    const onSend = vi.fn();
    render(<ChatInput {...baseProps} onSend={onSend} />);

    selectFiles([new File(['hi'], 'a.txt', { type: 'text/plain' })]);

    expect(screen.getByText('a.txt')).toBeInTheDocument();
    expect(mockUpload).toHaveBeenCalledWith(expect.any(File), expect.stringMatching(/^chat-/));
    await screen.findByText('2 B'); // ready -> size shown

    fireEvent.change(ta(), { target: { value: 'explain it' } });
    fireEvent.keyDown(ta(), { key: 'Enter' });

    expect(onSend).toHaveBeenCalledTimes(1);
    const [message, meta] = onSend.mock.calls[0];
    // Displayed message stays clean — the knowledge note is NOT shown in chat.
    expect(message).toBe('explain it');
    expect(message).not.toContain('Knowledge files attached');
    // The note rides along as a hidden dispatch-only suffix, with the tool.
    expect(meta.tools).toEqual(['DatabricksKnowledgeSearchTool']);
    expect(meta.dispatchSuffix).toContain('Knowledge files attached: a.txt');
    // Attachment names are surfaced for display on the message bubble.
    expect(meta.attachments).toEqual(['a.txt']);
    // Attachment stays in the composer after sending (reusable for follow-ups).
    expect(screen.getByText('a.txt')).toBeInTheDocument();
  });

  it('shows a failed chip on upload error and does NOT attach the tool on send', async () => {
    mockUpload.mockRejectedValue(new Error('boom'));
    const onSend = vi.fn();
    render(<ChatInput {...baseProps} onSend={onSend} />);

    selectFiles([new File(['x'], 'bad.txt')]);
    await screen.findByText('failed');

    fireEvent.change(ta(), { target: { value: 'hi' } });
    fireEvent.keyDown(ta(), { key: 'Enter' });
    expect(onSend).toHaveBeenCalledWith('hi'); // single arg, no ready attachments
  });

  it('uses a generic error when the rejection is not an Error', async () => {
    mockUpload.mockRejectedValue('weird');
    render(<ChatInput {...baseProps} />);
    selectFiles([new File(['x'], 'bad.txt')]);
    await screen.findByText('failed');
    // title carries the generic message
    expect(screen.getByText('bad.txt').closest('div')?.getAttribute('title')).toBe('Upload failed');
  });

  it('removes an attachment via its remove button', async () => {
    mockUpload.mockResolvedValue({ path: '/Volumes/x/a.txt', status: 'success', filename: 'a.txt' });
    render(<ChatInput {...baseProps} />);
    selectFiles([new File(['hi'], 'a.txt')]);
    await screen.findByText('2 B');
    fireEvent.click(screen.getByLabelText('Remove a.txt'));
    expect(screen.queryByText('a.txt')).not.toBeInTheDocument();
  });

  it('ignores a change event with no files', () => {
    render(<ChatInput {...baseProps} />);
    fireEvent.change(fileInput(), { target: { files: [] } });
    expect(mockUpload).not.toHaveBeenCalled();
  });

  it('clicking the attach button does not throw and shows the count once attached', async () => {
    mockUpload.mockResolvedValue({ path: 'p', status: 'success', filename: 'a.txt' });
    const { container } = render(<ChatInput {...baseProps} />);
    fireEvent.click(screen.getByLabelText('Attach files'));
    selectFiles([new File(['hi'], 'a.txt'), new File(['yo'], 'b.txt')]);
    await screen.findAllByText('2 B');
    // attach button shows the count "2"
    expect(within(screen.getByLabelText('Attach files')).getByText('2')).toBeInTheDocument();
    expect(container).toBeTruthy();
  });

  it('accepts files via drag-and-drop and shows then hides the overlay', async () => {
    mockUpload.mockResolvedValue({ path: 'p', status: 'success', filename: 'd.txt' });
    const { container } = render(<ChatInput {...baseProps} />);
    const shell = container.querySelector('.kasal-input-shell') as HTMLElement;

    fireEvent.dragEnter(shell, { dataTransfer: { types: ['Files'] } }); // depth 1
    fireEvent.dragEnter(shell, { dataTransfer: { types: ['Files'] } }); // depth 2 (nested child)
    expect(screen.getByText(/Drop files to attach/)).toBeInTheDocument();

    fireEvent.dragLeave(shell); // depth -> 1, overlay stays
    expect(screen.getByText(/Drop files to attach/)).toBeInTheDocument();
    fireEvent.dragLeave(shell); // depth -> 0, overlay hides
    expect(screen.queryByText(/Drop files to attach/)).not.toBeInTheDocument();

    fireEvent.dragOver(shell, { dataTransfer: { types: ['Files'] } });
    fireEvent.drop(shell, { dataTransfer: { files: [new File(['dd'], 'd.txt')] } });
    await screen.findByText('d.txt');
    expect(mockUpload).toHaveBeenCalled();
  });

  it('ignores drag enter when no files are present (e.g. text drag)', () => {
    const { container } = render(<ChatInput {...baseProps} />);
    const shell = container.querySelector('.kasal-input-shell') as HTMLElement;
    fireEvent.dragEnter(shell, { dataTransfer: { types: ['text/plain'] } });
    expect(screen.queryByText(/Drop files to attach/)).not.toBeInTheDocument();
  });

  it('drop with no files does not upload', () => {
    render(<ChatInput {...baseProps} />);
    const shell = screen.getByPlaceholderText('Ask a question...').closest('.kasal-input-shell') as HTMLElement;
    fireEvent.drop(shell, { dataTransfer: { files: [] } });
    expect(mockUpload).not.toHaveBeenCalled();
  });

  it('renders human-readable sizes (0 B and KB)', async () => {
    mockUpload.mockResolvedValue({ path: 'p', status: 'success', filename: 'f' });
    render(<ChatInput {...baseProps} />);
    selectFiles([
      new File([], 'empty.txt'),
      new File([new Uint8Array(1536)], 'k.txt'),
    ]);
    await screen.findByText('0 B');
    expect(screen.getByText('1.5 KB')).toBeInTheDocument();
  });

  it('blocks send while a file is still uploading, then allows it once ready', async () => {
    let resolveUpload!: (v: { path: string; status: string; filename: string }) => void;
    mockUpload.mockReturnValue(
      new Promise((res) => {
        resolveUpload = res;
      })
    );
    const onSend = vi.fn();
    const { container } = render(<ChatInput {...baseProps} onSend={onSend} />);

    selectFiles([new File(['hi'], 'a.txt')]);
    fireEvent.change(ta(), { target: { value: 'go' } });

    // Send button disabled while uploading; Enter is a no-op.
    const sendBtn = container.querySelectorAll('button')[
      container.querySelectorAll('button').length - 1
    ] as HTMLButtonElement;
    expect(sendBtn).toBeDisabled();
    fireEvent.keyDown(ta(), { key: 'Enter' });
    expect(onSend).not.toHaveBeenCalled();

    // Upload completes -> chip ready, send unblocked.
    await act(async () => {
      resolveUpload({ path: '/Volumes/x/a.txt', status: 'success', filename: 'a.txt' });
    });
    await screen.findByText('2 B');
    fireEvent.keyDown(ta(), { key: 'Enter' });
    expect(onSend).toHaveBeenCalledTimes(1);
  });

  it('does not attach the knowledge tool to slash commands even with files', async () => {
    mockUpload.mockResolvedValue({ path: 'p', status: 'success', filename: 'a.txt' });
    const onSend = vi.fn();
    render(<ChatInput {...baseProps} onSend={onSend} />);
    selectFiles([new File(['hi'], 'a.txt')]);
    await screen.findByText('2 B');
    fireEvent.change(ta(), { target: { value: '/clear' } });
    fireEvent.keyDown(ta(), { key: 'Enter' });
    // slash path: single-arg onSend, no tools meta
    expect(onSend).toHaveBeenCalledWith('/clear');
  });
});

describe('ChatInput — attachment persistence (per session)', () => {
  const fileInput = () => screen.getByTestId('chat-file-input') as HTMLInputElement;

  beforeEach(() => {
    mockUpload.mockReset();
    localStorage.clear();
  });

  it('restores ready attachments stored for the session on mount', () => {
    localStorage.setItem(
      'kasal-chat-attachments-s1',
      JSON.stringify([{ id: '1', name: 'kept.txt', size: 10, status: 'ready', path: '/Volumes/x/kept.txt' }]),
    );
    render(<ChatInput {...baseProps} sessionId="s1" />);
    expect(screen.getByText('kept.txt')).toBeInTheDocument();
  });

  it('persists a ready upload to the session key and clears it on removal', async () => {
    mockUpload.mockResolvedValue({ path: '/Volumes/x/a.txt', status: 'success', filename: 'a.txt' });
    render(<ChatInput {...baseProps} sessionId="s2" />);
    fireEvent.change(fileInput(), { target: { files: [new File(['hi'], 'a.txt')] } });
    await screen.findByText('2 B');

    const stored = JSON.parse(localStorage.getItem('kasal-chat-attachments-s2') || '[]');
    expect(stored).toHaveLength(1);
    expect(stored[0].name).toBe('a.txt');

    fireEvent.click(screen.getByLabelText('Remove a.txt'));
    expect(localStorage.getItem('kasal-chat-attachments-s2')).toBeNull();
  });

  it('ignores corrupt stored data without crashing', () => {
    localStorage.setItem('kasal-chat-attachments-s3', 'not-json{');
    render(<ChatInput {...baseProps} sessionId="s3" />);
    expect(screen.queryByText(/\.txt/)).not.toBeInTheDocument();
  });

  it('ignores non-array stored data', () => {
    localStorage.setItem('kasal-chat-attachments-s4', JSON.stringify({ not: 'an array' }));
    render(<ChatInput {...baseProps} sessionId="s4" />);
    expect(screen.queryByText(/\.txt/)).not.toBeInTheDocument();
  });

  it('does not persist when there is no session id', async () => {
    mockUpload.mockResolvedValue({ path: 'p', status: 'success', filename: 'a.txt' });
    render(<ChatInput {...baseProps} />);
    fireEvent.change(fileInput(), { target: { files: [new File(['hi'], 'a.txt')] } });
    await screen.findByText('2 B');
    // No session-scoped key was written.
    expect(Object.keys(localStorage).some((k) => k.startsWith('kasal-chat-attachments-'))).toBe(false);
  });
});

describe('ChatInput — run / generation status (inline, replaces top banner)', () => {
  it('has no Stop button while executing (Stop lives in the run-activity container); Send stays but is disabled', () => {
    render(<ChatInput {...baseProps} isExecuting onStopExecution={vi.fn()} />);
    // Stop moved out of the input — only the run-activity container has it now.
    expect(screen.queryByRole('button', { name: 'Stop execution' })).not.toBeInTheDocument();
    // Send is still present (submit kept) but disabled while a run is active.
    expect(screen.getByRole('button', { name: 'Send message' })).toBeDisabled();
  });

  it('shows a disabled (busy) send button while generating — no Stop, no text', () => {
    render(<ChatInput {...baseProps} isGenerating />);
    expect(screen.queryByText('Generating…')).not.toBeInTheDocument();
    // Generating keeps the Send button (busy/disabled), not the Stop button.
    expect(screen.getByRole('button', { name: 'Send message' })).toBeDisabled();
    expect(screen.queryByRole('button', { name: 'Stop execution' })).not.toBeInTheDocument();
  });

  it('falls back to the Send button when executing without a stop handler', () => {
    render(<ChatInput {...baseProps} isExecuting />);
    expect(screen.queryByRole('button', { name: 'Stop execution' })).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Send message' })).toBeInTheDocument();
  });

  it('shows no Stop control when idle', () => {
    render(<ChatInput {...baseProps} />);
    expect(screen.queryByRole('button', { name: 'Stop execution' })).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Send message' })).toBeInTheDocument();
  });
});

describe('ChatInput — workspace memory toggle (controlled)', () => {
  it('reflects the controlled value and calls onWorkspaceMemoryChange on click', () => {
    const onWorkspaceMemoryChange = vi.fn();
    const { rerender } = render(
      <ChatInput {...baseProps} workspaceMemory onWorkspaceMemoryChange={onWorkspaceMemoryChange} />,
    );
    expect(screen.getByText('Workspace memory')).toBeInTheDocument();
    fireEvent.click(screen.getByText('Workspace memory'));
    expect(onWorkspaceMemoryChange).toHaveBeenCalledWith(false);
    // Parent flips the value → label reflects "Session only".
    rerender(
      <ChatInput {...baseProps} workspaceMemory={false} onWorkspaceMemoryChange={onWorkspaceMemoryChange} />,
    );
    expect(screen.getByText('Session only')).toBeInTheDocument();
  });
});
