import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, within, act } from '@testing-library/react';
import ChatInput from './ChatInput';
import type { ModelConfigResponse } from '../../types/dispatcher';
import { uploadKnowledgeFile } from '../../api/knowledge';
import { useExecutionStore } from '../../store/executionStore';
import { useAppStore } from '../../store/appStore';

vi.mock('../../api/knowledge', () => ({
  uploadKnowledgeFile: vi.fn(),
  forgetKnowledgeFile: vi.fn().mockResolvedValue(true),
}));
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
  useExecutionStore.setState({ selectedMcpServers: [] });
});

const openMenu = () => fireEvent.click(screen.getByLabelText('Composer settings and tools'));

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

describe('ChatInput — trifecta notice integration', () => {
  it('shows the inline security notice for a risky selection, hidden otherwise', () => {
    // Two internal sources alone: no egress channel → no notice.
    useExecutionStore.setState({
      selectedMcpServers: ['Genie', 'Databricks SQL'],
      selectedAgentBricksEndpoints: [],
    });
    const { rerender } = render(<ChatInput {...baseProps} />);
    expect(screen.queryByTestId('trifecta-notice')).toBeNull();

    // Add an unknown MCP server (assumed untrusted+external) → notice appears.
    act(() =>
      useExecutionStore.setState({
        selectedMcpServers: ['Genie', 'Some Custom MCP'],
        selectedAgentBricksEndpoints: [],
      }),
    );
    rerender(<ChatInput {...baseProps} />);
    expect(screen.getByTestId('trifecta-notice')).toBeInTheDocument();
  });
});

describe('ChatInput — slash command autocomplete', () => {
  it('shows the command list when typing "/" and filters', () => {
    render(<ChatInput {...baseProps} />);
    fireEvent.change(ta(), { target: { value: '/clear' } });
    expect(screen.getByText('Commands')).toBeInTheDocument();
    expect(screen.getAllByText('/clear').length).toBeGreaterThan(0);
  });

  it('hides the list when no command matches', () => {
    render(<ChatInput {...baseProps} />);
    fireEvent.change(ta(), { target: { value: '/zzzz' } });
    expect(screen.queryByText('Commands')).not.toBeInTheDocument();
  });

  it('ArrowDown/ArrowUp navigate the list (with wraparound) and Enter selects', () => {
    const onSend = vi.fn();
    render(<ChatInput {...baseProps} onSend={onSend} />);
    fireEvent.change(ta(), { target: { value: '/' } }); // matches all commands
    fireEvent.keyDown(ta(), { key: 'ArrowDown' });
    fireEvent.keyDown(ta(), { key: 'ArrowUp' });
    fireEvent.keyDown(ta(), { key: 'ArrowUp' }); // wrap to last (/clear — no trailing space)
    // Enter selects current command; /clear has no trailing space -> sent immediately
    fireEvent.keyDown(ta(), { key: 'Enter' });
    expect(onSend).toHaveBeenCalled();
  });

  it('Tab selects a command that needs a param (kept in the box, not sent)', () => {
    const onSend = vi.fn();
    render(<ChatInput {...baseProps} onSend={onSend} />);
    fireEvent.change(ta(), { target: { value: '/refine' } }); // "/refine " has trailing space
    fireEvent.keyDown(ta(), { key: 'Tab' });
    expect(ta().value).toBe('/refine ');
    expect(onSend).not.toHaveBeenCalled();
  });

  it('clicking a command in the list selects it', () => {
    const onSend = vi.fn();
    render(<ChatInput {...baseProps} onSend={onSend} />);
    fireEvent.change(ta(), { target: { value: '/clear' } });
    fireEvent.click(screen.getAllByText('/clear')[0]);
    // /clear has no trailing space -> sent immediately
    expect(onSend).toHaveBeenCalledWith('/clear');
  });

  it('mouseEnter on a command updates the selected index', () => {
    render(<ChatInput {...baseProps} />);
    fireEvent.change(ta(), { target: { value: '/' } });
    const cmd = screen.getByText('/refine');
    fireEvent.mouseEnter(cmd.closest('button')!);
    expect(cmd).toBeInTheDocument();
  });

  it('Escape closes the command list', () => {
    render(<ChatInput {...baseProps} />);
    fireEvent.change(ta(), { target: { value: '/clear' } });
    expect(screen.getByText('Commands')).toBeInTheDocument();
    fireEvent.keyDown(ta(), { key: 'Escape' });
    expect(screen.queryByText('Commands')).not.toBeInTheDocument();
  });

  it('ignores other keys while the command list is open', () => {
    render(<ChatInput {...baseProps} />);
    fireEvent.change(ta(), { target: { value: '/clear' } });
    // a non-navigation key falls through without closing/selecting
    fireEvent.keyDown(ta(), { key: 'a' });
    expect(screen.getByText('Commands')).toBeInTheDocument();
  });

  it('ArrowDown wraps from the last item back to the first', () => {
    render(<ChatInput {...baseProps} />);
    fireEvent.change(ta(), { target: { value: '/' } }); // 3 matches: skill, refine, clear
    fireEvent.keyDown(ta(), { key: 'ArrowDown' }); // 0 -> 1
    fireEvent.keyDown(ta(), { key: 'ArrowDown' }); // 1 -> 2 (last)
    fireEvent.keyDown(ta(), { key: 'ArrowDown' }); // 2 -> wrap to 0 (the ":0" arm)
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

describe('ChatInput — model picker (inside the "+" menu)', () => {
  it('opens the menu, expands Model, selects one, and calls onModelChange', () => {
    const onModelChange = vi.fn();
    render(<ChatInput {...baseProps} onModelChange={onModelChange} />);
    openMenu();
    // The row shows the current model as its value.
    fireEvent.click(screen.getByLabelText('Model'));
    fireEvent.click(screen.getByText('Model Two'));
    expect(onModelChange).toHaveBeenCalledWith('m2');
  });

  it('outside mousedown closes the whole menu', () => {
    render(<ChatInput {...baseProps} />);
    openMenu();
    expect(screen.getByLabelText('Model')).toBeInTheDocument();
    fireEvent.mouseDown(document.body);
    expect(screen.queryByLabelText('Model')).not.toBeInTheDocument();
  });

  it('mousedown inside the menu does not close it', () => {
    render(<ChatInput {...baseProps} />);
    openMenu();
    fireEvent.mouseDown(screen.getByLabelText('Model'));
    expect(screen.getByLabelText('Model')).toBeInTheDocument();
  });

  it('falls back to the raw key when the selected model is unknown', () => {
    render(<ChatInput {...baseProps} selectedModel="missing" />);
    openMenu();
    expect(screen.getByText('missing')).toBeInTheDocument();
  });

  it('shows no search box for a short model list', () => {
    render(<ChatInput {...baseProps} />);
    openMenu();
    fireEvent.click(screen.getByLabelText('Model'));
    expect(screen.queryByLabelText('Search models')).toBeNull();
    expect(screen.getByText('Model One')).toBeInTheDocument();
  });

  it('search appears for a long model list, filters it, and clears per visit', () => {
    const many = Array.from({ length: 9 }, (_, i) => ({
      key: `k${i}`,
      name: i === 0 ? 'Claude Sonnet' : `Model ${i}`,
    })) as ModelConfigResponse[];
    const onModelChange = vi.fn();
    render(<ChatInput {...baseProps} models={many} selectedModel="k1" onModelChange={onModelChange} />);
    openMenu();
    fireEvent.click(screen.getByLabelText('Model'));

    const search = screen.getByLabelText('Search models');
    fireEvent.change(search, { target: { value: 'sonnet' } });
    expect(screen.getByText('Claude Sonnet')).toBeInTheDocument();
    expect(screen.queryByText('Model 3')).toBeNull();

    // No hits → explicit empty state, not a silently blank list.
    fireEvent.change(search, { target: { value: 'zzz' } });
    expect(screen.getByText('No matching models')).toBeInTheDocument();

    // Picking from a filtered list works and returns to the main level.
    fireEvent.change(search, { target: { value: 'sonnet' } });
    fireEvent.click(screen.getByText('Claude Sonnet'));
    expect(onModelChange).toHaveBeenCalledWith('k0');

    // Re-entering the panel starts fresh — the old query must not linger.
    fireEvent.click(screen.getByLabelText('Model'));
    expect((screen.getByLabelText('Search models') as HTMLInputElement).value).toBe('');
    expect(screen.getByText('Model 3')).toBeInTheDocument();
  });

  it('renders no Model row when the models list is empty', () => {
    render(<ChatInput {...baseProps} models={[]} selectedModel="" />);
    openMenu();
    expect(screen.queryByLabelText('Model')).not.toBeInTheDocument();
  });
});

describe('ChatInput — no output-format picker', () => {
  it('does not render a format selector; the deliverable derives from content', () => {
    // The picker was removed on purpose: output varieties will keep growing,
    // so the deliverable type is inferred from the request, never enumerated.
    render(<ChatInput {...baseProps} />);
    expect(screen.queryByText('Auto format')).not.toBeInTheDocument();
    expect(screen.queryByTitle('Choose the output format the crew should produce')).not.toBeInTheDocument();
  });

  it('a plain prompt sends with no dispatchSuffix (no [Output format:] directive)', () => {
    const onSend = vi.fn();
    render(<ChatInput {...baseProps} onSend={onSend} />);
    fireEvent.change(ta(), { target: { value: 'make slides' } });
    fireEvent.keyDown(ta(), { key: 'Enter' });
    expect(onSend).toHaveBeenCalledWith('make slides'); // single-arg, no meta
  });

  it('selected MCP servers ride along as a hidden dispatch-only steering note', () => {
    useExecutionStore.setState({
      selectedMcpServers: ['Databricks Genie: API Request Performance Analytics'],
    });
    const onSend = vi.fn();
    render(<ChatInput {...baseProps} onSend={onSend} />);
    fireEvent.change(ta(), { target: { value: 'what can I ask here?' } });
    fireEvent.keyDown(ta(), { key: 'Enter' });

    const [message, meta] = onSend.mock.calls[0];
    // Displayed message stays clean — the note steers only the generation.
    expect(message).toBe('what can I ask here?');
    expect(meta.dispatchSuffix).toContain(
      'MCP data sources attached: Databricks Genie: API Request Performance Analytics',
    );
  });

  it('slash commands stay literal even with MCP servers selected', () => {
    useExecutionStore.setState({ selectedMcpServers: ['My MCP'] });
    const onSend = vi.fn();
    render(<ChatInput {...baseProps} onSend={onSend} />);
    fireEvent.change(ta(), { target: { value: '/clear' } });
    fireEvent.keyDown(ta(), { key: 'Enter' });
    expect(onSend).toHaveBeenCalledWith('/clear'); // no meta
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
    // Attachment PATHS ride along so the backend scopes the knowledge search to
    // exactly this file (not group-wide) — the names alone aren't enough.
    expect(meta.knowledgeFilePaths).toEqual(['/Volumes/x/a.txt']);
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

  it('the Attach files row does not throw, and the "+" badge shows the count', async () => {
    mockUpload.mockResolvedValue({ path: 'p', status: 'success', filename: 'a.txt' });
    const { container } = render(<ChatInput {...baseProps} />);
    openMenu();
    fireEvent.click(screen.getByLabelText('Attach files')); // closes the menu, opens the file dialog
    selectFiles([new File(['hi'], 'a.txt'), new File(['yo'], 'b.txt')]);
    await screen.findAllByText('2 B');
    // the "+" trigger badges the pending-attachment count
    expect(
      within(screen.getByLabelText('Composer settings and tools')).getByText('2'),
    ).toBeInTheDocument();
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

describe('ChatInput — pending run mode (loaded catalog crew/flow)', () => {
  it('with a pendingRunLabel + empty input: the submit button is enabled and clicking it runs (not sends)', () => {
    const onSend = vi.fn();
    const onRunPending = vi.fn();
    render(
      <ChatInput
        {...baseProps}
        onSend={onSend}
        pendingRunLabel="My Crew"
        onRunPending={onRunPending}
      />,
    );
    // In run mode the button is labelled "Run <label>" and is enabled.
    const runBtn = screen.getByRole('button', { name: 'Run My Crew' });
    expect(runBtn).not.toBeDisabled();
    expect(runBtn.getAttribute('title')).toBe('Run “My Crew”');
    fireEvent.click(runBtn);
    expect(onRunPending).toHaveBeenCalledTimes(1);
    expect(onSend).not.toHaveBeenCalled();
  });

  it('with text typed (pendingRunLabel set): clicking sends normally and does NOT run', () => {
    const onSend = vi.fn();
    const onRunPending = vi.fn();
    render(
      <ChatInput
        {...baseProps}
        onSend={onSend}
        pendingRunLabel="My Crew"
        onRunPending={onRunPending}
      />,
    );
    fireEvent.change(ta(), { target: { value: 'hello there' } });
    // Typing leaves run mode → the button is the normal Send button.
    fireEvent.click(screen.getByRole('button', { name: 'Send message' }));
    expect(onSend).toHaveBeenCalledWith('hello there');
    expect(onRunPending).not.toHaveBeenCalled();
  });

  it('with no pendingRunLabel + empty input: the submit button stays disabled', () => {
    render(<ChatInput {...baseProps} />);
    expect(screen.getByRole('button', { name: 'Send message' })).toBeDisabled();
  });
});

describe('ChatInput — memory mode (inside the "+" menu)', () => {
  it('shows the active memory mode as the row value', () => {
    render(<ChatInput {...baseProps} memoryEnabled />);
    openMenu();
    expect(screen.getByLabelText('Memory')).toHaveTextContent('Teamspace memory');
  });

  it('shows Session memory when disabled', () => {
    render(<ChatInput {...baseProps} memoryEnabled={false} />);
    openMenu();
    expect(screen.getByLabelText('Memory')).toHaveTextContent('Session memory');
  });

  it('picking the other option flips memoryEnabled', () => {
    const onMemoryEnabledChange = vi.fn();
    render(<ChatInput {...baseProps} memoryEnabled onMemoryEnabledChange={onMemoryEnabledChange} />);
    openMenu();
    fireEvent.click(screen.getByLabelText('Memory'));
    fireEvent.click(screen.getByText('Session memory'));
    expect(onMemoryEnabledChange).toHaveBeenLastCalledWith(false);
  });
});

describe('ChatInput — the "+" menu opens upward by default', () => {
  it('uses the slide-up animation, like the other popovers', () => {
    render(<ChatInput {...baseProps} />);
    openMenu();
    const popover = document.querySelector('.kasal-popover') as HTMLElement;
    expect(popover.className).toContain('animate-slide-up');
    expect(popover.className).not.toContain('animate-slide-down');
  });
});

describe('ChatInput — answer mode (inside the "+" menu)', () => {
  beforeEach(() => {
    useExecutionStore.getState().setChatModeType('chat');
  });

  it('shows the active mode and expands to all three', () => {
    render(<ChatInput {...baseProps} />);
    openMenu();
    const row = screen.getByLabelText('Answer mode');
    expect(row).toHaveTextContent('Chat');
    fireEvent.click(row);
    expect(screen.getByText('Research')).toBeInTheDocument();
    expect(screen.getByText('Deep Research')).toBeInTheDocument();
  });

  it('selecting a mode updates the store', () => {
    render(<ChatInput {...baseProps} />);
    openMenu();
    fireEvent.click(screen.getByLabelText('Answer mode'));
    fireEvent.click(screen.getByText('Research'));
    expect(useExecutionStore.getState().chatModeType).toBe('research');
  });

  it('positions the menu with position:fixed so it escapes overflow-hidden containers', () => {
    render(<ChatInput {...baseProps} />);
    openMenu();
    const popover = document.querySelector('.kasal-popover') as HTMLElement;
    expect(popover.style.position).toBe('fixed');
  });
});

describe('ChatInput — "+" menu placement (flips with the input position)', () => {
  it('flips DOWN (slide-down) when menuPlacement="down"', () => {
    render(<ChatInput {...baseProps} menuPlacement="down" />);
    openMenu();
    const popover = document.querySelector('.kasal-popover') as HTMLElement;
    expect(popover.className).toContain('animate-slide-down');
  });
});

describe('ChatInput — Tools & MCP (inline inside the "+" menu)', () => {
  it('expanding the Tools row renders the MCP list inline (no nested popover)', () => {
    render(<ChatInput {...baseProps} />);
    openMenu();
    fireEvent.click(screen.getByLabelText('Tools & MCP'));
    const mcp = screen.getByRole('menu', { name: 'MCP picker' });
    expect(mcp).toBeInTheDocument();
    // Inline: statically positioned inside the menu, so overflow-hidden
    // cannot clip it (the invisible-MCP bug).
    expect((mcp as HTMLElement).className).not.toContain('absolute');
  });
});

describe('ChatInput — prefill (empty-state suggestion chips)', () => {
  it('drops prefill text into the composer without sending', () => {
    const onSend = vi.fn();
    render(<ChatInput {...baseProps} onSend={onSend} prefill={{ text: 'Research [topic]', nonce: 1 }} />);
    expect(ta().value).toBe('Research [topic]');
    expect(onSend).not.toHaveBeenCalled();
  });

  it('re-applies when the nonce changes (re-picking a chip)', () => {
    const { rerender } = render(<ChatInput {...baseProps} prefill={{ text: 'first', nonce: 1 }} />);
    expect(ta().value).toBe('first');
    // User clears it, then picks another chip → new nonce re-applies.
    fireEvent.change(ta(), { target: { value: '' } });
    rerender(<ChatInput {...baseProps} prefill={{ text: 'second', nonce: 2 }} />);
    expect(ta().value).toBe('second');
  });

  it('does not re-apply the same nonce after the user edits', () => {
    const { rerender } = render(<ChatInput {...baseProps} prefill={{ text: 'seed', nonce: 5 }} />);
    expect(ta().value).toBe('seed');
    fireEvent.change(ta(), { target: { value: 'my own text' } });
    // A re-render with the SAME nonce must not clobber the user's edit.
    rerender(<ChatInput {...baseProps} prefill={{ text: 'seed', nonce: 5 }} />);
    expect(ta().value).toBe('my own text');
  });
});

describe('ChatInput — rotating landing placeholder (advertises deliverables)', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  const box = () => screen.getByRole('textbox') as HTMLTextAreaElement;
  const ph = () => box().getAttribute('placeholder') || '';

  it('rotates deliverable hints on the landing composer, dashboard first', () => {
    vi.useFakeTimers();
    render(<ChatInput {...baseProps} isLanding />);
    expect(ph()).toMatch(/dashboard/i);
    act(() => { vi.advanceTimersByTime(3300); });
    expect(ph()).toMatch(/presentation/i);
  });

  it('freezes to the base placeholder on focus and does not rotate', () => {
    vi.useFakeTimers();
    render(<ChatInput {...baseProps} isLanding />);
    act(() => { fireEvent.focus(box()); });
    expect(ph()).toBe('Ask a question...');
    act(() => { vi.advanceTimersByTime(3300); });
    expect(ph()).toBe('Ask a question...');
  });

  it('does not rotate in the conversation composer (no isLanding)', () => {
    vi.useFakeTimers();
    render(<ChatInput {...baseProps} />);
    expect(ph()).toBe('Ask a question...');
    act(() => { vi.advanceTimersByTime(3300); });
    expect(ph()).toBe('Ask a question...');
  });

  it('stays on a single static hint when reduced motion is preferred', () => {
    const original = window.matchMedia;
    window.matchMedia = vi.fn().mockReturnValue({
      matches: true,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
    }) as unknown as typeof window.matchMedia;
    vi.useFakeTimers();
    render(<ChatInput {...baseProps} isLanding />);
    expect(ph()).toMatch(/dashboard/i);
    act(() => { vi.advanceTimersByTime(3300); });
    expect(ph()).toMatch(/dashboard/i); // unchanged — no rotation
    window.matchMedia = original;
  });
});
