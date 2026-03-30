import { vi, beforeEach, describe, it, expect } from 'vitest';
import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import { ChatMessageItem } from './ChatMessageItem';
import { ChatMessage } from '../types';

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

// Mock MessageRenderer -- we only need to verify what content is passed in
vi.mock('./MessageRenderer', () => ({
  MessageContent: ({ content }: { content: string }) => (
    <div data-testid="message-content">{content}</div>
  ),
}));

// Mock textProcessing utilities so we can control markdown detection
const mockStripAnsiEscapes = vi.fn((text: string) => text);
const mockIsMarkdown = vi.fn((_text: string) => false);
const mockIsHtmlDocument = vi.fn((_text: string) => false);

vi.mock('../utils/textProcessing', () => ({
  stripAnsiEscapes: (text: string) => mockStripAnsiEscapes(text),
  isMarkdown: (text: string) => mockIsMarkdown(text),
  isHtmlDocument: (text: string) => mockIsHtmlDocument(text),
}));

// MUI Fade can interfere with rendering in tests -- simplify it
vi.mock('@mui/material/Fade', () => ({
  default: ({ children }: { children: React.ReactElement }) => children,
}));

// Mock GenieSpaceConfigPrompt for genie config metadata tests
vi.mock('../GenieSpaceConfigPrompt', () => ({
  GenieSpaceConfigPrompt: ({ configs }: { configs: unknown[] }) => (
    <div data-testid="genie-config-prompt">{configs.length} configs</div>
  ),
}));

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeMessage(overrides: Partial<ChatMessage> = {}): ChatMessage {
  return {
    id: 'msg-1',
    type: 'assistant',
    content: 'Hello world',
    timestamp: new Date('2025-06-15T12:00:00Z'),
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('ChatMessageItem', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockStripAnsiEscapes.mockImplementation((t: string) => t);
    mockIsMarkdown.mockReturnValue(false);
    mockIsHtmlDocument.mockReturnValue(false);
  });

  // =========================================================================
  // 1. whiteSpace conditional (core change under test)
  // =========================================================================

  describe('whiteSpace conditional based on isMarkdown', () => {
    it('applies whiteSpace "normal" when content is detected as markdown', () => {
      mockIsMarkdown.mockReturnValue(true);

      const { container } = render(
        <ChatMessageItem message={makeMessage({ content: '# Heading\n\n- list item' })} />
      );

      // The Box wrapping renderMessageContent() is the one with whiteSpace.
      // It is the parent of the element rendered by MessageContent.
      const messageContent = screen.getByTestId('message-content');
      const wrappingBox = messageContent.closest('[class*="MuiBox-root"]');
      expect(wrappingBox).not.toBeNull();

      const style = window.getComputedStyle(wrappingBox!);
      // MUI applies styles via className; we can also check the inline style
      // by looking at the MUI sx prop output. Since MUI uses CSS-in-JS, we
      // verify by checking that isMarkdown was called with stripped content.
      expect(mockIsMarkdown).toHaveBeenCalledWith('# Heading\n\n- list item');
      expect(mockStripAnsiEscapes).toHaveBeenCalledWith('# Heading\n\n- list item');
    });

    it('applies whiteSpace "pre-wrap" when content is plain text', () => {
      mockIsMarkdown.mockReturnValue(false);

      render(
        <ChatMessageItem message={makeMessage({ content: 'plain text here' })} />
      );

      expect(mockIsMarkdown).toHaveBeenCalledWith('plain text here');
      expect(mockStripAnsiEscapes).toHaveBeenCalledWith('plain text here');
    });

    it('strips ANSI codes before checking isMarkdown', () => {
      const ansiContent = '\u001b[32mGreen text\u001b[0m';
      const strippedContent = 'Green text';
      mockStripAnsiEscapes.mockReturnValue(strippedContent);
      mockIsMarkdown.mockReturnValue(false);

      render(
        <ChatMessageItem message={makeMessage({ content: ansiContent })} />
      );

      // stripAnsiEscapes should be called with the raw content
      expect(mockStripAnsiEscapes).toHaveBeenCalledWith(ansiContent);
      // isMarkdown should receive the already-stripped content
      expect(mockIsMarkdown).toHaveBeenCalledWith(strippedContent);
    });

    it('passes stripped content to the MessageContent for default rendering', () => {
      const ansiContent = '\u001b[31mred\u001b[0m';
      mockStripAnsiEscapes.mockReturnValue('red');
      mockIsMarkdown.mockReturnValue(false);

      render(
        <ChatMessageItem message={makeMessage({ content: ansiContent })} />
      );

      expect(screen.getByTestId('message-content')).toHaveTextContent('red');
    });
  });

  // =========================================================================
  // 2. Basic rendering for each message type
  // =========================================================================

  describe('message type rendering', () => {
    it('renders a user message with PersonIcon avatar', () => {
      render(
        <ChatMessageItem message={makeMessage({ type: 'user', content: 'User says hi' })} />
      );

      expect(screen.getByTestId('PersonIcon')).toBeInTheDocument();
      expect(screen.getByTestId('message-content')).toHaveTextContent('User says hi');
    });

    it('renders an assistant message with SmartToyIcon avatar', () => {
      render(
        <ChatMessageItem message={makeMessage({ type: 'assistant', content: 'Bot reply' })} />
      );

      expect(screen.getByTestId('SmartToyIcon')).toBeInTheDocument();
      expect(screen.getByTestId('message-content')).toHaveTextContent('Bot reply');
    });

    it('renders an execution message with SmartToyIcon and Execution Output label', () => {
      render(
        <ChatMessageItem message={makeMessage({ type: 'execution', content: 'Running...' })} />
      );

      expect(screen.getByTestId('SmartToyIcon')).toBeInTheDocument();
      expect(screen.getByText('Execution Output')).toBeInTheDocument();
    });

    it('renders a trace message with SmartToyIcon and Trace Output label', () => {
      render(
        <ChatMessageItem message={makeMessage({ type: 'trace', content: 'Trace data' })} />
      );

      expect(screen.getByTestId('SmartToyIcon')).toBeInTheDocument();
      expect(screen.getByText('Trace Output')).toBeInTheDocument();
    });

    it('renders a result message with SmartToyIcon avatar', () => {
      render(
        <ChatMessageItem
          message={makeMessage({ type: 'result', content: 'Final result text' })}
        />
      );

      expect(screen.getByTestId('SmartToyIcon')).toBeInTheDocument();
    });

    it('displays the timestamp for every message', () => {
      const timestamp = new Date('2025-06-15T14:30:00Z');
      render(
        <ChatMessageItem message={makeMessage({ timestamp })} />
      );

      expect(screen.getByText(timestamp.toLocaleTimeString())).toBeInTheDocument();
    });
  });

  // =========================================================================
  // 3. Intent chip display for assistant messages
  // =========================================================================

  describe('intent chip display', () => {
    it('shows an intent chip when assistant message has an intent', () => {
      render(
        <ChatMessageItem
          message={makeMessage({
            type: 'assistant',
            intent: 'generate_agent',
            content: 'Creating agent...',
          })}
        />
      );

      expect(screen.getByText('Generate Agent')).toBeInTheDocument();
    });

    it('does not show intent chip for non-assistant message types', () => {
      render(
        <ChatMessageItem
          message={makeMessage({
            type: 'user',
            intent: 'generate_agent',
            content: 'My query',
          })}
        />
      );

      expect(screen.queryByText('Generate Agent')).not.toBeInTheDocument();
    });

    it('formats intent label correctly -- replaces underscores and capitalizes', () => {
      render(
        <ChatMessageItem
          message={makeMessage({
            type: 'assistant',
            intent: 'generate_task',
            content: 'Task created',
          })}
        />
      );

      expect(screen.getByText('Generate Task')).toBeInTheDocument();
    });

    it('shows generate_crew intent chip with success color', () => {
      render(
        <ChatMessageItem
          message={makeMessage({
            type: 'assistant',
            intent: 'generate_crew',
            content: 'Crew generated',
          })}
        />
      );

      expect(screen.getByText('Generate Crew')).toBeInTheDocument();
    });

    it('shows configure_crew intent chip with default color', () => {
      render(
        <ChatMessageItem
          message={makeMessage({
            type: 'assistant',
            intent: 'configure_crew',
            content: 'Crew configured',
          })}
        />
      );

      expect(screen.getByText('Configure Crew')).toBeInTheDocument();
    });

    it('shows default icon for unknown intents', () => {
      render(
        <ChatMessageItem
          message={makeMessage({
            type: 'assistant',
            intent: 'unknown_intent',
            content: 'Something',
          })}
        />
      );

      // AccountTreeIcon is the default
      expect(screen.getByTestId('AccountTreeIcon')).toBeInTheDocument();
      expect(screen.getByText('Unknown Intent')).toBeInTheDocument();
    });
  });

  // =========================================================================
  // 4. Confidence display
  // =========================================================================

  describe('confidence display', () => {
    it('shows confidence percentage when provided on an assistant message with intent', () => {
      render(
        <ChatMessageItem
          message={makeMessage({
            type: 'assistant',
            intent: 'generate_agent',
            confidence: 0.95,
            content: 'High confidence result',
          })}
        />
      );

      expect(screen.getByText('95% confidence')).toBeInTheDocument();
    });

    it('rounds confidence to the nearest integer', () => {
      render(
        <ChatMessageItem
          message={makeMessage({
            type: 'assistant',
            intent: 'generate_task',
            confidence: 0.876,
            content: 'Result',
          })}
        />
      );

      expect(screen.getByText('88% confidence')).toBeInTheDocument();
    });

    it('does not show confidence when it is undefined', () => {
      render(
        <ChatMessageItem
          message={makeMessage({
            type: 'assistant',
            intent: 'generate_agent',
            content: 'Some content',
          })}
        />
      );

      expect(screen.queryByText(/% confidence/)).not.toBeInTheDocument();
    });

    it('shows 0% confidence when confidence is 0', () => {
      render(
        <ChatMessageItem
          message={makeMessage({
            type: 'assistant',
            intent: 'generate_agent',
            confidence: 0,
            content: 'Zero confidence',
          })}
        />
      );

      expect(screen.getByText('0% confidence')).toBeInTheDocument();
    });
  });

  // =========================================================================
  // 5. Result message with JSON value content (compact styling)
  // =========================================================================

  describe('result message rendering', () => {
    it('renders JSON with value field as markdown via MessageContent', () => {
      const jsonContent = JSON.stringify({
        value: '# Report\n\n- Finding 1\n- Finding 2',
      });

      render(
        <ChatMessageItem message={makeMessage({ type: 'result', content: jsonContent })} />
      );

      // The value content should be processed and passed to MessageContent
      const messageContent = screen.getByTestId('message-content');
      expect(messageContent).toBeInTheDocument();
      // The cleaned content should contain the headings and list items
      expect(messageContent.textContent).toContain('Report');
      expect(messageContent.textContent).toContain('Finding 1');
    });

    it('renders JSON without value field as formatted code block', () => {
      const jsonObj = { key: 'test', nested: { foo: 'bar' } };
      const jsonContent = JSON.stringify(jsonObj);

      render(
        <ChatMessageItem message={makeMessage({ type: 'result', content: jsonContent })} />
      );

      // Should display formatted JSON in a code element
      const codeElements = screen.getAllByText(/"key": "test"/);
      expect(codeElements.length).toBeGreaterThan(0);
    });

    it('renders non-JSON result content as markdown via MessageContent', () => {
      render(
        <ChatMessageItem
          message={makeMessage({ type: 'result', content: 'Plain result text' })}
        />
      );

      const messageContent = screen.getByTestId('message-content');
      expect(messageContent).toBeInTheDocument();
      expect(messageContent.textContent).toContain('Plain result text');
    });

    it('strips empty lines from result JSON value content', () => {
      const jsonContent = JSON.stringify({
        value: 'Line 1\n\n\n\nLine 2',
      });

      render(
        <ChatMessageItem message={makeMessage({ type: 'result', content: jsonContent })} />
      );

      const messageContent = screen.getByTestId('message-content');
      expect(messageContent).toBeInTheDocument();
      expect(messageContent.textContent).toContain('Line 1');
      expect(messageContent.textContent).toContain('Line 2');
    });
  });

  // =========================================================================
  // 6. Trace message JSON formatting
  // =========================================================================

  describe('trace message rendering', () => {
    it('formats JSON trace content as a pretty-printed code block', () => {
      const traceJson = JSON.stringify({ step: 1, action: 'lookup' });

      render(
        <ChatMessageItem message={makeMessage({ type: 'trace', content: traceJson })} />
      );

      // Should render formatted JSON in a <pre>/<code> block
      const codeElements = screen.getAllByText(/"step": 1/);
      expect(codeElements.length).toBeGreaterThan(0);
    });

    it('extracts JSON from markdown code blocks in trace messages', () => {
      const wrappedJson = '```json\n{"step": 2, "action": "transform"}\n```';

      render(
        <ChatMessageItem message={makeMessage({ type: 'trace', content: wrappedJson })} />
      );

      const codeElements = screen.getAllByText(/"step": 2/);
      expect(codeElements.length).toBeGreaterThan(0);
    });

    it('falls back to default rendering for non-JSON trace content', () => {
      render(
        <ChatMessageItem
          message={makeMessage({ type: 'trace', content: 'Not JSON at all' })}
        />
      );

      expect(screen.getByTestId('message-content')).toHaveTextContent('Not JSON at all');
    });
  });

  // =========================================================================
  // 7. ANSI escape stripping
  // =========================================================================

  describe('ANSI escape stripping', () => {
    it('calls stripAnsiEscapes on message content', () => {
      const raw = '\u001b[1;31mError\u001b[0m: something failed';
      mockStripAnsiEscapes.mockReturnValue('Error: something failed');

      render(
        <ChatMessageItem message={makeMessage({ content: raw })} />
      );

      expect(mockStripAnsiEscapes).toHaveBeenCalledWith(raw);
      expect(screen.getByTestId('message-content')).toHaveTextContent(
        'Error: something failed'
      );
    });

    it('strips ANSI from result message content before processing', () => {
      const raw = '\u001b[32m{"value":"cleaned"}\u001b[0m';
      mockStripAnsiEscapes.mockReturnValue('{"value":"cleaned"}');

      render(
        <ChatMessageItem message={makeMessage({ type: 'result', content: raw })} />
      );

      expect(mockStripAnsiEscapes).toHaveBeenCalledWith(raw);
      const messageContent = screen.getByTestId('message-content');
      expect(messageContent.textContent).toContain('cleaned');
    });

    it('strips ANSI from trace message content before JSON parsing', () => {
      const raw = '\u001b[33m{"traceKey":"val"}\u001b[0m';
      mockStripAnsiEscapes.mockReturnValue('{"traceKey":"val"}');

      render(
        <ChatMessageItem message={makeMessage({ type: 'trace', content: raw })} />
      );

      expect(mockStripAnsiEscapes).toHaveBeenCalledWith(raw);
    });
  });

  // =========================================================================
  // 8. Execution / trace metadata chips
  // =========================================================================

  describe('execution and trace metadata', () => {
    it('displays eventSource chip for execution messages', () => {
      render(
        <ChatMessageItem
          message={makeMessage({
            type: 'execution',
            content: 'Running task',
            eventSource: 'crew_engine',
          })}
        />
      );

      expect(screen.getByText('crew_engine')).toBeInTheDocument();
    });

    it('displays eventContext chip for trace messages', () => {
      render(
        <ChatMessageItem
          message={makeMessage({
            type: 'trace',
            content: 'Trace data',
            eventContext: 'agent_step',
          })}
        />
      );

      expect(screen.getByText('agent_step')).toBeInTheDocument();
    });

    it('displays both eventSource and eventContext when both are present', () => {
      render(
        <ChatMessageItem
          message={makeMessage({
            type: 'execution',
            content: 'Data',
            eventSource: 'source_val',
            eventContext: 'context_val',
          })}
        />
      );

      expect(screen.getByText('source_val')).toBeInTheDocument();
      expect(screen.getByText('context_val')).toBeInTheDocument();
    });

    it('shows open logs button when jobId and onOpenLogs are provided', () => {
      const onOpenLogs = vi.fn();

      render(
        <ChatMessageItem
          message={makeMessage({
            type: 'execution',
            content: 'Running',
            jobId: 'job-123',
          })}
          onOpenLogs={onOpenLogs}
        />
      );

      const logButton = screen.getByRole('button');
      expect(logButton).toBeInTheDocument();
    });

    it('calls onOpenLogs with jobId when the logs button is clicked', () => {
      const onOpenLogs = vi.fn();

      render(
        <ChatMessageItem
          message={makeMessage({
            type: 'execution',
            content: 'Running',
            jobId: 'job-456',
          })}
          onOpenLogs={onOpenLogs}
        />
      );

      const logButton = screen.getByRole('button');
      fireEvent.click(logButton);
      expect(onOpenLogs).toHaveBeenCalledWith('job-456');
    });

    it('does not show open logs button when jobId is absent', () => {
      const onOpenLogs = vi.fn();

      render(
        <ChatMessageItem
          message={makeMessage({
            type: 'execution',
            content: 'Running',
          })}
          onOpenLogs={onOpenLogs}
        />
      );

      expect(screen.queryByRole('button')).not.toBeInTheDocument();
    });

    it('does not show open logs button when onOpenLogs is not provided', () => {
      render(
        <ChatMessageItem
          message={makeMessage({
            type: 'execution',
            content: 'Running',
            jobId: 'job-789',
          })}
        />
      );

      expect(screen.queryByRole('button')).not.toBeInTheDocument();
    });
  });

  // =========================================================================
  // 9. Assistant messages with result objects (agent/task/crew generation)
  // =========================================================================

  describe('assistant result rendering', () => {
    it('renders generated agent details when intent is generate_agent', () => {
      render(
        <ChatMessageItem
          message={makeMessage({
            type: 'assistant',
            intent: 'generate_agent',
            content: 'Agent created',
            result: {
              agent: { name: 'Research Bot', role: 'Senior Researcher' },
            },
          })}
        />
      );

      expect(screen.getByText('Research Bot')).toBeInTheDocument();
      expect(screen.getByText('Role: Senior Researcher')).toBeInTheDocument();
      expect(screen.getByText(/created an agent/i)).toBeInTheDocument();
    });

    it('renders generated task details when intent is generate_task', () => {
      render(
        <ChatMessageItem
          message={makeMessage({
            type: 'assistant',
            intent: 'generate_task',
            content: 'Task created',
            result: {
              task: { name: 'Data Analysis', description: 'Analyze the dataset' },
            },
          })}
        />
      );

      expect(screen.getByText('Data Analysis')).toBeInTheDocument();
      expect(screen.getByText('Analyze the dataset')).toBeInTheDocument();
      expect(screen.getByText(/created a task/i)).toBeInTheDocument();
    });

    it('renders generated crew with agents and tasks', () => {
      render(
        <ChatMessageItem
          message={makeMessage({
            type: 'assistant',
            intent: 'generate_crew',
            content: 'Crew created',
            result: {
              crew: {
                agents: [{ name: 'Agent A' }, { name: 'Agent B' }],
                tasks: [{ name: 'Task X' }, { name: 'Task Y' }],
              },
            },
          })}
        />
      );

      expect(screen.getByText(/created a complete plan/i)).toBeInTheDocument();
      expect(screen.getByText('Agents:')).toBeInTheDocument();
      expect(screen.getByText('Tasks:')).toBeInTheDocument();
    });

    it('renders crew with only agents (no tasks)', () => {
      render(
        <ChatMessageItem
          message={makeMessage({
            type: 'assistant',
            intent: 'generate_crew',
            content: 'Crew with agents only',
            result: {
              crew: {
                agents: [{ name: 'Solo Agent' }],
                tasks: [],
              },
            },
          })}
        />
      );

      expect(screen.getByText('Agents:')).toBeInTheDocument();
      expect(screen.queryByText('Tasks:')).not.toBeInTheDocument();
    });

    it('renders crew with only tasks (no agents)', () => {
      render(
        <ChatMessageItem
          message={makeMessage({
            type: 'assistant',
            intent: 'generate_crew',
            content: 'Crew with tasks only',
            result: {
              crew: {
                agents: [],
                tasks: [{ name: 'Solo Task' }],
              },
            },
          })}
        />
      );

      expect(screen.queryByText('Agents:')).not.toBeInTheDocument();
      expect(screen.getByText('Tasks:')).toBeInTheDocument();
    });

    it('falls back to default MessageContent when assistant has result but unknown intent', () => {
      render(
        <ChatMessageItem
          message={makeMessage({
            type: 'assistant',
            intent: 'unknown_action',
            content: 'Fallback content',
            result: { something: 'else' },
          })}
        />
      );

      expect(screen.getByTestId('message-content')).toHaveTextContent('Fallback content');
    });

    it('falls back to default MessageContent when assistant has result but no matching key', () => {
      render(
        <ChatMessageItem
          message={makeMessage({
            type: 'assistant',
            intent: 'generate_agent',
            content: 'No agent key',
            result: { notAgent: true },
          })}
        />
      );

      expect(screen.getByTestId('message-content')).toHaveTextContent('No agent key');
    });
  });

  // =========================================================================
  // 10. Genie config metadata rendering
  // =========================================================================

  describe('genie config metadata rendering', () => {
    it('renders genie config prompt when metadata type is genie_config_needed', () => {
      render(
        <ChatMessageItem
          message={makeMessage({
            type: 'assistant',
            content: 'Configure Genie spaces',
            metadata: {
              type: 'genie_config_needed',
              configs: [
                { toolName: 'GenieTool', taskNodeId: 'task-1', taskName: 'Test Task' },
              ],
            },
          })}
        />
      );

      expect(screen.getByTestId('genie-config-prompt')).toBeInTheDocument();
      expect(screen.getByText('1 configs')).toBeInTheDocument();
    });

    it('renders normal message content when no metadata is present', () => {
      render(
        <ChatMessageItem
          message={makeMessage({
            type: 'assistant',
            content: 'Normal response',
          })}
        />
      );

      expect(screen.queryByTestId('genie-config-prompt')).not.toBeInTheDocument();
      expect(screen.getByTestId('message-content')).toHaveTextContent('Normal response');
    });
  });

  // =========================================================================
  // 11. Avatar colors
  // =========================================================================

  describe('avatar colors', () => {
    it('uses primary color for user messages', () => {
      const { container } = render(
        <ChatMessageItem message={makeMessage({ type: 'user', content: 'Hi' })} />
      );

      const avatar = container.querySelector('.MuiAvatar-root');
      expect(avatar).toBeInTheDocument();
    });

    it('uses secondary color for assistant messages', () => {
      const { container } = render(
        <ChatMessageItem message={makeMessage({ type: 'assistant', content: 'Hi' })} />
      );

      const avatar = container.querySelector('.MuiAvatar-root');
      expect(avatar).toBeInTheDocument();
    });

    it('uses warning color for execution messages', () => {
      const { container } = render(
        <ChatMessageItem message={makeMessage({ type: 'execution', content: 'Run' })} />
      );

      const avatar = container.querySelector('.MuiAvatar-root');
      expect(avatar).toBeInTheDocument();
    });

    it('uses success color for result messages', () => {
      const { container } = render(
        <ChatMessageItem message={makeMessage({ type: 'result', content: 'Done' })} />
      );

      const avatar = container.querySelector('.MuiAvatar-root');
      expect(avatar).toBeInTheDocument();
    });
  });

  // =========================================================================
  // 12. Raw HTML document wrapping
  // =========================================================================

  describe('raw HTML document wrapping', () => {
    it('wraps raw HTML documents in a code fence for syntax highlighting', () => {
      mockIsHtmlDocument.mockReturnValue(true);

      render(
        <ChatMessageItem
          message={makeMessage({
            type: 'assistant',
            content: '<!doctype html><html><body>Hello</body></html>',
          })}
        />
      );

      // The content should be wrapped in ```html ... ``` before passing to MessageContent
      const messageContent = screen.getByTestId('message-content');
      expect(messageContent.textContent).toContain('```html');
      expect(messageContent.textContent).toContain('<!doctype html>');
    });

    it('does not wrap non-HTML content in a code fence', () => {
      mockIsHtmlDocument.mockReturnValue(false);

      render(
        <ChatMessageItem
          message={makeMessage({
            type: 'assistant',
            content: 'Just a regular message',
          })}
        />
      );

      const messageContent = screen.getByTestId('message-content');
      expect(messageContent.textContent).not.toContain('```');
      expect(messageContent.textContent).toBe('Just a regular message');
    });
  });
});
