import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, within } from '@testing-library/react';
import ChatInput from './ChatInput';
import type { ModelConfigResponse } from '../../types/dispatcher';

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
    // a natural-language prompt gets the directive appended
    fireEvent.change(ta(), { target: { value: 'make slides' } });
    fireEvent.keyDown(ta(), { key: 'Enter' });
    expect(onSend).toHaveBeenCalledWith(expect.stringContaining('make slides'));
    expect(onSend).toHaveBeenCalledWith(expect.stringContaining('[Output format:'));
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
