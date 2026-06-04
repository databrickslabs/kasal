/**
 * Tests for MessageRenderer component.
 *
 * Covers:
 * - renderWithLinks: URL detection and rendering as clickable links
 * - MessageContent: markdown vs plain text path selection
 * - ReactMarkdown custom renderers: a, p, ul, ol, li, code
 * - Slash command detection in inline code (clickable, keyboard accessible)
 * - Non-command inline code with standard grey styling
 * - Fenced code blocks rendered inside <pre>
 * - Keyboard accessibility (Enter and Space trigger chatCommandClick)
 * - Mouse hover style changes on slash command elements
 * - CustomEvent dispatch on click and keyboard interaction
 *
 * NOTE: The isMarkdown() utility requires at least one markdown pattern
 * (bold, headers, lists, links, code blocks, blockquotes) for content to
 * enter the ReactMarkdown rendering path. Inline backtick code alone does
 * not trigger markdown mode. All tests for the code renderer therefore
 * include a markdown marker (e.g. bold text) alongside inline code.
 */

import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import { vi, describe, it, expect, beforeEach } from 'vitest';
import { renderWithLinks, MessageContent, sanitizeUrl } from './MessageRenderer';

// Mock CodeBlock so fenced code block tests can inspect rendered output
vi.mock('./CodeBlock', () => ({
  CodeBlock: ({ language, code }: { language: string; code: string }) => (
    <pre
      data-testid="code-block"
      data-language={language}
      style={{
        padding: '12px',
        maxHeight: '400px',
        overflow: 'auto',
        fontFamily: 'monospace',
        fontSize: '0.875em',
      }}
    >
      <code className={language ? `language-${language}` : ''}>{code}</code>
    </pre>
  ),
}));

// Helper: wraps inline code content with a bold marker so isMarkdown() returns true
// and the content flows through ReactMarkdown with custom renderers.
const md = (text: string) => `**Note:** ${text}`;

describe('MessageRenderer', () => {
  // ─── renderWithLinks ──────────────────────────────────────────────

  describe('renderWithLinks', () => {
    it('returns plain text when no URLs are present', () => {
      const result = renderWithLinks('Hello world');
      const { container } = render(<>{result}</>);
      expect(container.textContent).toBe('Hello world');
      expect(container.querySelector('a')).toBeNull();
    });

    it('renders a single URL as a clickable link', () => {
      const result = renderWithLinks('Visit https://example.com for more');
      const { container } = render(<>{result}</>);
      const link = container.querySelector('a');
      expect(link).not.toBeNull();
      expect(link).toHaveAttribute('href', 'https://example.com');
      expect(link).toHaveAttribute('target', '_blank');
      expect(link).toHaveAttribute('rel', 'noopener noreferrer');
    });

    it('renders the external link icon next to URLs', () => {
      const result = renderWithLinks('Go to https://example.com now');
      const { container } = render(<>{result}</>);
      const svg = container.querySelector('svg');
      expect(svg).not.toBeNull();
    });

    it('renders multiple URLs in the same text', () => {
      const result = renderWithLinks(
        'See https://example.com and https://other.com for details'
      );
      const { container } = render(<>{result}</>);
      const links = container.querySelectorAll('a');
      expect(links).toHaveLength(2);
      expect(links[0]).toHaveAttribute('href', 'https://example.com');
      expect(links[1]).toHaveAttribute('href', 'https://other.com');
    });

    it('handles text that is only a URL', () => {
      const result = renderWithLinks('https://example.com');
      const { container } = render(<>{result}</>);
      const link = container.querySelector('a');
      expect(link).not.toBeNull();
      expect(link).toHaveAttribute('href', 'https://example.com');
      expect(container.textContent).toContain('https://example.com');
    });

    it('preserves surrounding text around URLs', () => {
      const result = renderWithLinks('Before https://example.com after');
      const { container } = render(<>{result}</>);
      expect(container.textContent).toContain('Before');
      expect(container.textContent).toContain('after');
    });

    it('handles http (non-https) URLs', () => {
      const result = renderWithLinks('Link: http://example.com here');
      const { container } = render(<>{result}</>);
      const link = container.querySelector('a');
      expect(link).not.toBeNull();
      expect(link).toHaveAttribute('href', 'http://example.com');
    });
  });

  // ─── MessageContent ───────────────────────────────────────────────

  describe('MessageContent', () => {
    let chatCommandHandler: ReturnType<typeof vi.fn>;

    beforeEach(() => {
      chatCommandHandler = vi.fn();
      window.addEventListener('chatCommandClick', chatCommandHandler);
    });

    afterEach(() => {
      window.removeEventListener('chatCommandClick', chatCommandHandler);
    });

    // ── plain text (non-markdown) ─────────────────────────────────

    describe('plain text rendering (non-markdown)', () => {
      it('renders plain text without markdown processing', () => {
        render(<MessageContent content="Hello world" />);
        expect(screen.getByText('Hello world')).toBeInTheDocument();
      });

      it('renders URLs as links in plain text mode', () => {
        const { container } = render(
          <MessageContent content="Visit https://example.com today" />
        );
        const link = container.querySelector('a');
        expect(link).not.toBeNull();
        expect(link).toHaveAttribute('href', 'https://example.com');
      });

      it('renders plain text without wrapping in markdown elements', () => {
        const { container } = render(
          <MessageContent content="Just some simple text" />
        );
        // No markdown-generated paragraph wrappers
        expect(container.querySelector('p')).toBeNull();
      });

      it('falls through to renderWithLinks for non-markdown inline code', () => {
        // Backtick-only content without other markdown markers is plain text
        const { container } = render(
          <MessageContent content="Use the `thing` here" />
        );
        // Should render as plain text, no <code> elements
        expect(container.querySelector('code')).toBeNull();
        expect(container.textContent).toContain('`thing`');
      });
    });

    // ── markdown structural renderers ─────────────────────────────

    describe('markdown rendering', () => {
      it('renders bold text', () => {
        const { container } = render(
          <MessageContent content="This is **bold** text" />
        );
        const strong = container.querySelector('strong');
        expect(strong).not.toBeNull();
        expect(strong!.textContent).toBe('bold');
      });

      it('renders headers', () => {
        const { container } = render(
          <MessageContent content="# Header One" />
        );
        const header = container.querySelector('h1');
        expect(header).not.toBeNull();
        expect(header!.textContent).toBe('Header One');
      });

      it('renders unordered lists with compact spacing', () => {
        const { container } = render(
          <MessageContent content={'- Item one\n- Item two'} />
        );
        const ul = container.querySelector('ul');
        expect(ul).not.toBeNull();
        expect(ul!.style.margin).toBe('2px 0px');
        expect(ul!.style.paddingLeft).toBe('20px');

        const items = container.querySelectorAll('li');
        expect(items.length).toBeGreaterThanOrEqual(2);
        items.forEach((li) => {
          expect(li.style.margin).toBe('0px');
          expect(li.style.padding).toBe('0px');
        });
      });

      it('renders ordered lists with compact spacing', () => {
        const { container } = render(
          <MessageContent content={'1. First\n2. Second\n3. Third'} />
        );
        const ol = container.querySelector('ol');
        expect(ol).not.toBeNull();
        expect(ol!.style.margin).toBe('2px 0px');
        expect(ol!.style.paddingLeft).toBe('32px');
      });

      it('renders paragraphs with compact spacing', () => {
        const { container } = render(
          <MessageContent content="**bold** paragraph text here" />
        );
        const p = container.querySelector('p');
        expect(p).not.toBeNull();
        expect(p!.style.margin).toBe('4px 0px');
      });

      it('renders markdown links with external icon', () => {
        const { container } = render(
          <MessageContent content="Click [here](https://example.com) for info" />
        );
        const link = container.querySelector('a');
        expect(link).not.toBeNull();
        expect(link).toHaveAttribute('href', 'https://example.com');
        expect(link).toHaveAttribute('target', '_blank');
        expect(link).toHaveAttribute('rel', 'noopener noreferrer');
        const svg = link!.querySelector('svg');
        expect(svg).not.toBeNull();
      });

      it('renders blockquotes', () => {
        const { container } = render(
          <MessageContent content="> This is a quote" />
        );
        const blockquote = container.querySelector('blockquote');
        expect(blockquote).not.toBeNull();
      });
    });

    // ── inline code rendering (via ReactMarkdown) ─────────────────

    describe('inline code rendering', () => {
      it('renders non-command inline code with grey styling', () => {
        const { container } = render(
          <MessageContent content={md('Use the `console.log` function')} />
        );
        const codeElements = container.querySelectorAll('code');
        const inlineCode = Array.from(codeElements).find(
          (el) => !el.closest('pre')
        );
        expect(inlineCode).not.toBeNull();
        expect(inlineCode!.textContent).toBe('console.log');
        expect(inlineCode!.style.backgroundColor).toBe('rgba(0, 0, 0, 0.08)');
        expect(inlineCode!.style.fontFamily).toBe('monospace');
        expect(inlineCode!.style.fontSize).toBe('0.9em');
        expect(inlineCode!.style.borderRadius).toBe('4px');
        // Non-command code should NOT have role="button"
        expect(inlineCode!.getAttribute('role')).toBeNull();
      });

      it('renders slash command inline code with blue styling', () => {
        const { container } = render(
          <MessageContent content={md('Try the `/help` command')} />
        );
        const commandCode = container.querySelector('code[role="button"]');
        expect(commandCode).not.toBeNull();
        expect(commandCode!.textContent).toBe('/help');
        expect(commandCode!.style.backgroundColor).toBe(
          'rgba(25, 118, 210, 0.08)'
        );
        expect(commandCode!.style.color).toBe('rgb(21, 101, 192)');
        expect(commandCode!.style.cursor).toBe('pointer');
        expect(commandCode!.style.border).toBe(
          '1px solid rgba(25, 118, 210, 0.3)'
        );
        expect(commandCode!.style.fontFamily).toBe('monospace');
        expect(commandCode!.style.fontSize).toBe('0.9em');
        expect(commandCode!.style.transition).toBe('background-color 0.15s');
      });

      it('renders slash command with role="button" and tabIndex=0', () => {
        const { container } = render(
          <MessageContent content={md('Use `/execute` to run')} />
        );
        const commandCode = container.querySelector('code[role="button"]');
        expect(commandCode).not.toBeNull();
        expect(commandCode).toHaveAttribute('tabindex', '0');
      });

      it('dispatches chatCommandClick event on click', () => {
        const { container } = render(
          <MessageContent content={md('Click `/run` here')} />
        );
        const commandCode = container.querySelector('code[role="button"]');
        expect(commandCode).not.toBeNull();
        fireEvent.click(commandCode!);

        expect(chatCommandHandler).toHaveBeenCalledTimes(1);
        const event = chatCommandHandler.mock.calls[0][0] as CustomEvent;
        expect(event.detail).toEqual({ command: '/run' });
      });

      it('dispatches chatCommandClick event on Enter key', () => {
        const { container } = render(
          <MessageContent content={md('Press Enter on `/start`')} />
        );
        const commandCode = container.querySelector('code[role="button"]');
        expect(commandCode).not.toBeNull();
        fireEvent.keyDown(commandCode!, { key: 'Enter' });

        expect(chatCommandHandler).toHaveBeenCalledTimes(1);
        const event = chatCommandHandler.mock.calls[0][0] as CustomEvent;
        expect(event.detail).toEqual({ command: '/start' });
      });

      it('dispatches chatCommandClick event on Space key', () => {
        const { container } = render(
          <MessageContent content={md('Press Space on `/stop`')} />
        );
        const commandCode = container.querySelector('code[role="button"]');
        expect(commandCode).not.toBeNull();
        fireEvent.keyDown(commandCode!, { key: ' ' });

        expect(chatCommandHandler).toHaveBeenCalledTimes(1);
        const event = chatCommandHandler.mock.calls[0][0] as CustomEvent;
        expect(event.detail).toEqual({ command: '/stop' });
      });

      it('does not dispatch event on non-Enter/Space keys', () => {
        const { container } = render(
          <MessageContent content={md('Type on `/noop`')} />
        );
        const commandCode = container.querySelector('code[role="button"]');
        expect(commandCode).not.toBeNull();
        fireEvent.keyDown(commandCode!, { key: 'Tab' });
        fireEvent.keyDown(commandCode!, { key: 'Escape' });
        fireEvent.keyDown(commandCode!, { key: 'a' });

        expect(chatCommandHandler).not.toHaveBeenCalled();
      });

      it('calls preventDefault on Enter key for slash commands', () => {
        const { container } = render(
          <MessageContent content={md('Test `/test`')} />
        );
        const commandCode = container.querySelector('code[role="button"]');
        expect(commandCode).not.toBeNull();

        const prevented = !fireEvent.keyDown(commandCode!, { key: 'Enter' });
        // fireEvent returns false when preventDefault was called
        expect(prevented).toBe(true);
      });

      it('calls preventDefault on Space key for slash commands', () => {
        const { container } = render(
          <MessageContent content={md('Test `/test`')} />
        );
        const commandCode = container.querySelector('code[role="button"]');
        expect(commandCode).not.toBeNull();

        const prevented = !fireEvent.keyDown(commandCode!, { key: ' ' });
        expect(prevented).toBe(true);
      });

      it('does not call preventDefault on non-trigger keys', () => {
        const { container } = render(
          <MessageContent content={md('Test `/test`')} />
        );
        const commandCode = container.querySelector('code[role="button"]');
        expect(commandCode).not.toBeNull();

        const prevented = !fireEvent.keyDown(commandCode!, { key: 'Tab' });
        // Tab should not be prevented
        expect(prevented).toBe(false);
      });

      it('changes background on mouse enter for slash commands', () => {
        const { container } = render(
          <MessageContent content={md('Hover over `/hover`')} />
        );
        const commandCode = container.querySelector(
          'code[role="button"]'
        ) as HTMLElement;
        expect(commandCode).not.toBeNull();

        expect(commandCode.style.backgroundColor).toBe(
          'rgba(25, 118, 210, 0.08)'
        );
        fireEvent.mouseEnter(commandCode);
        expect(commandCode.style.backgroundColor).toBe(
          'rgba(25, 118, 210, 0.16)'
        );
      });

      it('restores background on mouse leave for slash commands', () => {
        const { container } = render(
          <MessageContent content={md('Hover over `/hover`')} />
        );
        const commandCode = container.querySelector(
          'code[role="button"]'
        ) as HTMLElement;
        expect(commandCode).not.toBeNull();

        fireEvent.mouseEnter(commandCode);
        expect(commandCode.style.backgroundColor).toBe(
          'rgba(25, 118, 210, 0.16)'
        );

        fireEvent.mouseLeave(commandCode);
        expect(commandCode.style.backgroundColor).toBe(
          'rgba(25, 118, 210, 0.08)'
        );
      });

      it('handles multiple slash commands in one message', () => {
        const { container } = render(
          <MessageContent content={md('Use `/help` or `/run` commands')} />
        );
        const commandButtons = container.querySelectorAll(
          'code[role="button"]'
        );
        expect(commandButtons.length).toBe(2);

        fireEvent.click(commandButtons[0]);
        expect(chatCommandHandler).toHaveBeenCalledTimes(1);
        expect(
          (chatCommandHandler.mock.calls[0][0] as CustomEvent).detail.command
        ).toBe('/help');

        fireEvent.click(commandButtons[1]);
        expect(chatCommandHandler).toHaveBeenCalledTimes(2);
        expect(
          (chatCommandHandler.mock.calls[1][0] as CustomEvent).detail.command
        ).toBe('/run');
      });

      it('handles slash commands with arguments like /execute job-123', () => {
        const { container } = render(
          <MessageContent content={md('Run `/execute job-123` now')} />
        );
        const commandCode = container.querySelector('code[role="button"]');
        expect(commandCode).not.toBeNull();
        fireEvent.click(commandCode!);

        const event = chatCommandHandler.mock.calls[0][0] as CustomEvent;
        expect(event.detail.command).toBe('/execute job-123');
      });

      it('shows short label for /load crew commands', () => {
        const { container } = render(
          <MessageContent content={md('Try `/load crew My Research Crew`')} />
        );
        const commandCode = container.querySelector('code[role="button"]');
        expect(commandCode).not.toBeNull();
        // Display text should be just "load", not the full command
        expect(commandCode!.textContent).toBe('load');
        // Title should have the full command
        expect(commandCode!.getAttribute('title')).toBe('/load crew My Research Crew');
      });

      it('shows short label for /run flow commands', () => {
        const { container } = render(
          <MessageContent content={md('Try `/run flow Data Pipeline`')} />
        );
        const commandCode = container.querySelector('code[role="button"]');
        expect(commandCode).not.toBeNull();
        expect(commandCode!.textContent).toBe('run');
        expect(commandCode!.getAttribute('title')).toBe('/run flow Data Pipeline');
      });
    });

    // ── fenced code blocks ────────────────────────────────────────

    describe('fenced code block rendering', () => {
      it('renders fenced code blocks via CodeBlock component', () => {
        const content = '```javascript\nconst x = 1;\n```';
        const { container } = render(<MessageContent content={content} />);

        // CodeBlock is now used for fenced code blocks (mocked above).
        const codeBlock = container.querySelector('[data-testid="code-block"]');
        expect(codeBlock).not.toBeNull();
        expect(codeBlock!.getAttribute('data-language')).toBe('javascript');

        const code = codeBlock!.querySelector('code');
        expect(code).not.toBeNull();
        expect(code!.className).toContain('language-javascript');
        expect(code!.textContent).toContain('const x = 1;');
      });

      it('renders code blocks without a language specifier', () => {
        const content = '```\nplain code\n```';
        const { container } = render(<MessageContent content={content} />);
        const pre = container.querySelector('pre');
        expect(pre).not.toBeNull();
        expect(pre!.textContent).toContain('plain code');
      });

      it('does not treat fenced code block content as slash commands', () => {
        // Fenced code blocks get className like "language-*" so isInline is false
        const content = '```bash\n/help\n/run\n```';
        const { container } = render(<MessageContent content={content} />);
        const commandButtons = container.querySelectorAll(
          'code[role="button"]'
        );
        expect(commandButtons.length).toBe(0);

        // Content should be inside a <pre>
        const pre = container.querySelector('pre');
        expect(pre).not.toBeNull();
        expect(pre!.textContent).toContain('/help');
      });

      it('renders fenced code block without language in a pre wrapper', () => {
        // Without language, className may be empty but the code is still in pre
        const content = '```\n/help\n```';
        const { container } = render(<MessageContent content={content} />);
        const pre = container.querySelector('pre');
        expect(pre).not.toBeNull();
        expect(pre!.textContent).toContain('/help');
      });
    });

    // ── mixed content ─────────────────────────────────────────────

    describe('mixed content rendering', () => {
      it('renders markdown with both inline code and slash commands', () => {
        // Bold makes isMarkdown() return true, enabling ReactMarkdown path
        const content =
          '**Setup:** Use `npm install` then run `/start` to begin';
        const { container } = render(<MessageContent content={content} />);

        const allCodes = container.querySelectorAll('code');
        const inlineCodes = Array.from(allCodes).filter(
          (el) => !el.closest('pre')
        );

        expect(inlineCodes.length).toBe(2);

        const npmCode = inlineCodes.find(
          (el) => el.textContent === 'npm install'
        );
        expect(npmCode).not.toBeNull();
        expect(npmCode!.getAttribute('role')).toBeNull();
        expect(npmCode!.style.backgroundColor).toBe('rgba(0, 0, 0, 0.08)');

        const startCode = inlineCodes.find(
          (el) => el.textContent === '/start'
        );
        expect(startCode).not.toBeNull();
        expect(startCode!.getAttribute('role')).toBe('button');
        expect(startCode!.style.backgroundColor).toBe(
          'rgba(25, 118, 210, 0.08)'
        );
      });

      it('renders markdown links within content alongside slash commands', () => {
        const content =
          'Check [the docs](https://example.com) and use `/help`';
        const { container } = render(<MessageContent content={content} />);
        const link = container.querySelector('a');
        expect(link).not.toBeNull();
        expect(link).toHaveAttribute('href', 'https://example.com');

        const commandCode = container.querySelector('code[role="button"]');
        expect(commandCode).not.toBeNull();
        expect(commandCode!.textContent).toBe('/help');
      });

      it('renders bold text alongside slash commands', () => {
        const content = '**Important**: use `/deploy` carefully';
        const { container } = render(<MessageContent content={content} />);
        const strong = container.querySelector('strong');
        expect(strong).not.toBeNull();
        expect(strong!.textContent).toBe('Important');

        const commandCode = container.querySelector('code[role="button"]');
        expect(commandCode).not.toBeNull();
        expect(commandCode!.textContent).toBe('/deploy');
      });

      it('renders a list with inline slash commands', () => {
        const content = '- Run `/help` to see commands\n- Run `/start` to begin';
        const { container } = render(<MessageContent content={content} />);

        const ul = container.querySelector('ul');
        expect(ul).not.toBeNull();

        const commandButtons = container.querySelectorAll(
          'code[role="button"]'
        );
        expect(commandButtons.length).toBe(2);
        expect(commandButtons[0].textContent).toBe('/help');
        expect(commandButtons[1].textContent).toBe('/start');
      });

      it('renders a header with inline code', () => {
        const content = '# Using `config` options';
        const { container } = render(<MessageContent content={content} />);
        const h1 = container.querySelector('h1');
        expect(h1).not.toBeNull();
        expect(h1!.textContent).toContain('config');

        const inlineCode = container.querySelector('code');
        expect(inlineCode).not.toBeNull();
        expect(inlineCode!.textContent).toBe('config');
        // "config" does not start with "/" so grey styling
        expect(inlineCode!.style.backgroundColor).toBe('rgba(0, 0, 0, 0.08)');
      });
    });

    // ── edge cases ────────────────────────────────────────────────

    describe('edge cases', () => {
      it('renders empty string content without errors', () => {
        const { container } = render(<MessageContent content="" />);
        expect(container).toBeDefined();
      });

      it('treats content with only whitespace as plain text', () => {
        const { container } = render(<MessageContent content="   " />);
        expect(container).toBeDefined();
        expect(container.querySelector('h1')).toBeNull();
      });

      it('handles content with special characters in plain text', () => {
        const { container } = render(
          <MessageContent content="Price: $100 & tax < 10%" />
        );
        expect(container.textContent).toContain('Price: $100 & tax < 10%');
      });

      it('handles inline code that is just a forward slash as a command', () => {
        // "/" starts with "/" so it should be treated as a command
        const { container } = render(
          <MessageContent content={md('The separator is `/`')} />
        );
        const commandCode = container.querySelector('code[role="button"]');
        expect(commandCode).not.toBeNull();
        expect(commandCode!.textContent).toBe('/');
      });

      it('does not treat inline code starting with other characters as commands', () => {
        const { container } = render(
          <MessageContent content={md('The variable `myVar` is used')} />
        );
        const codes = container.querySelectorAll('code');
        const inlineCode = Array.from(codes).find(
          (el) => !el.closest('pre') && el.textContent === 'myVar'
        );
        expect(inlineCode).not.toBeNull();
        expect(inlineCode!.getAttribute('role')).toBeNull();
        expect(inlineCode!.style.backgroundColor).toBe('rgba(0, 0, 0, 0.08)');
      });

      it('renders GFM tables correctly', () => {
        const content =
          '| Col A | Col B |\n| --- | --- |\n| **val1** | val2 |';
        const { container } = render(<MessageContent content={content} />);
        const table = container.querySelector('table');
        expect(table).not.toBeNull();
      });

      it('strips trailing newline from inline code text before command check', () => {
        // ReactMarkdown may pass children with trailing newline;
        // the component strips it via .replace(/\n$/, '')
        const { container } = render(
          <MessageContent content={md('Run `/test`')} />
        );
        const commandCode = container.querySelector('code[role="button"]');
        expect(commandCode).not.toBeNull();
        expect(commandCode!.textContent).toBe('/test');
      });

      it('handles command-like text that is not inside backticks', () => {
        // Plain /help text without backticks should just be text
        const { container } = render(
          <MessageContent content="**Tip:** type /help in the chat" />
        );
        // There should be no code[role="button"] because /help is not in backticks
        const commandCode = container.querySelector('code[role="button"]');
        expect(commandCode).toBeNull();
      });

      it('dispatches correct command for multi-word slash command', () => {
        const { container } = render(
          <MessageContent content={md('Try `/execute --verbose --dry-run`')} />
        );
        const commandCode = container.querySelector('code[role="button"]');
        expect(commandCode).not.toBeNull();
        fireEvent.click(commandCode!);

        const event = chatCommandHandler.mock.calls[0][0] as CustomEvent;
        expect(event.detail.command).toBe('/execute --verbose --dry-run');
      });

      it('does not fire event on mouse enter/leave (only style changes)', () => {
        const { container } = render(
          <MessageContent content={md('Hover `/cmd`')} />
        );
        const commandCode = container.querySelector(
          'code[role="button"]'
        ) as HTMLElement;
        expect(commandCode).not.toBeNull();

        fireEvent.mouseEnter(commandCode);
        fireEvent.mouseLeave(commandCode);

        // No chatCommandClick events from hover
        expect(chatCommandHandler).not.toHaveBeenCalled();
      });
    });
  });

  // -------------------------------------------------------------------------
  // Security hardening tests (Phase 1 — Databricks AI Security, Feb 2026)
  // -------------------------------------------------------------------------
  describe('security hardening', () => {
    describe('sanitizeUrl()', () => {
      it('blocks javascript: scheme', () => {
        expect(sanitizeUrl('javascript:alert(1)')).toBe('');
      });

      it('blocks javascript: scheme with leading whitespace', () => {
        expect(sanitizeUrl('  javascript:alert(1)')).toBe('');
      });

      it('blocks javascript: scheme in mixed-case', () => {
        expect(sanitizeUrl('JavaScript:void(0)')).toBe('');
      });

      it('blocks data: scheme (potential XSS and exfiltration vector)', () => {
        expect(sanitizeUrl('data:text/html,<script>alert(1)</script>')).toBe('');
      });

      it('blocks vbscript: scheme', () => {
        expect(sanitizeUrl('vbscript:msgbox(1)')).toBe('');
      });

      it('allows https: scheme', () => {
        const url = 'https://example.com/path?q=1';
        expect(sanitizeUrl(url)).toBe(url);
      });

      it('allows http: scheme', () => {
        const url = 'http://example.com';
        expect(sanitizeUrl(url)).toBe(url);
      });

      it('returns empty string for undefined', () => {
        expect(sanitizeUrl(undefined)).toBe('');
      });

      it('returns empty string for null', () => {
        expect(sanitizeUrl(null)).toBe('');
      });

      it('returns empty string for empty string', () => {
        expect(sanitizeUrl('')).toBe('');
      });
    });

    describe('MessageContent — image rendering blocked', () => {
      it('does not render an <img> tag from markdown image syntax', () => {
        // Use a bold prefix so isMarkdown() returns true and we enter ReactMarkdown path
        const { container } = render(
          <MessageContent content={'**Note:** ![exfil](https://evil.com/track.gif)'} />
        );
        // No img elements should be in the DOM — disallowedElements drops them
        expect(container.querySelector('img')).toBeNull();
      });

      it('does not render an <img> tag for data: URI images', () => {
        const { container } = render(
          <MessageContent content={'**Note:** ![x](data:image/gif;base64,R0lGODlhAQABAAAAACH5BAEKAAEALAAAAAABAAEAAAICTAEAOw==)'} />
        );
        expect(container.querySelector('img')).toBeNull();
      });
    });

    describe('MessageContent — link sanitization', () => {
      it('does not create a clickable javascript: link', () => {
        const { container } = render(
          <MessageContent content={'**Note:** [click me](javascript:alert(1))'} />
        );
        // The anchor (if rendered at all) must not have a javascript: href
        const anchors = container.querySelectorAll('a');
        anchors.forEach((a) => {
          expect(a.getAttribute('href') ?? '').not.toContain('javascript:');
        });
      });

      it('renders javascript: link text as plain text span, not a clickable anchor', () => {
        const { container } = render(
          <MessageContent content={'**Note:** [dangerous link](javascript:alert(1))'} />
        );
        // Text should be visible but no <a> element with a dangerous href
        expect(container.textContent).toContain('dangerous link');
        const anchors = Array.from(container.querySelectorAll('a'));
        const dangerousAnchors = anchors.filter(
          (a) => (a.getAttribute('href') ?? '').toLowerCase().startsWith('javascript:')
        );
        expect(dangerousAnchors).toHaveLength(0);
      });

      it('renders safe https: links as clickable anchors', () => {
        const { container } = render(
          <MessageContent content={'**Note:** [visit](https://example.com)'} />
        );
        const anchors = container.querySelectorAll('a');
        expect(anchors.length).toBeGreaterThan(0);
        const link = Array.from(anchors).find(
          (a) => (a.getAttribute('href') ?? '').includes('example.com')
        );
        expect(link).toBeTruthy();
        expect(link!.getAttribute('href')).toBe('https://example.com');
      });

      it('sets rel="noopener noreferrer" on all rendered links', () => {
        const { container } = render(
          <MessageContent content={'**Note:** [visit](https://example.com)'} />
        );
        const anchors = container.querySelectorAll('a');
        anchors.forEach((a) => {
          const rel = a.getAttribute('rel') ?? '';
          expect(rel).toContain('noopener');
          expect(rel).toContain('noreferrer');
        });
      });
    });

    describe('renderWithLinks — plain text link sanitization', () => {
      it('renders a plain https URL as a clickable link', () => {
        const { container } = render(
          <>{renderWithLinks('Visit https://example.com for more.')}</>
        );
        const link = container.querySelector('a');
        expect(link).not.toBeNull();
        expect(link!.getAttribute('href')).toBe('https://example.com');
      });

      it('does not create a clickable link for javascript: URIs in plain text', () => {
        const { container } = render(
          <>{renderWithLinks('Click javascript:alert(1) please')}</>
        );
        const anchors = container.querySelectorAll('a');
        anchors.forEach((a) => {
          expect(a.getAttribute('href') ?? '').not.toContain('javascript:');
        });
      });
    });
  });
});
