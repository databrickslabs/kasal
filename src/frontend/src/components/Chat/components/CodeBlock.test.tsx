import React from 'react';
import { render, screen, fireEvent, act } from '@testing-library/react';
import { vi, describe, it, expect, beforeEach, afterEach } from 'vitest';
import { CodeBlock } from './CodeBlock';

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

// Mock useTheme to control dark mode
const mockUseTheme = vi.fn(() => ({ isDarkMode: false }));
vi.mock('../../../hooks/global/useTheme', () => ({
  useTheme: () => mockUseTheme(),
}));

// Mock textProcessing -- control isHtmlDocument
const mockIsHtmlDocument = vi.fn((_text: string) => false);
vi.mock('../utils/textProcessing', () => ({
  isHtmlDocument: (text: string) => mockIsHtmlDocument(text),
}));

// Mock prism-react-renderer Highlight with a simple pass-through
vi.mock('prism-react-renderer', () => ({
  Highlight: ({ code, language, children }: {
    code: string;
    language: string;
    theme: unknown;
    children: (args: {
      style: React.CSSProperties;
      tokens: { content: string; types: string[] }[][];
      getLineProps: (opts: { line: unknown }) => { key: number };
      getTokenProps: (opts: { token: unknown }) => { key: number; children: string };
    }) => React.ReactNode;
  }) => {
    const lines = code.split('\n');
    const tokens = lines.map((line: string) => [{ content: line, types: ['plain'] }]);
    return children({
      style: { backgroundColor: '#fafafa' },
      tokens,
      getLineProps: ({ line }: { line: unknown }) => ({ key: 0, 'data-line': line }),
      getTokenProps: ({ token }: { token: { content: string } }) => ({
        key: 0,
        children: token.content,
      }),
    });
  },
  themes: {
    oneDark: { plain: {} },
    oneLight: { plain: {} },
  },
}));

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function generateLargeCode(lineCount: number): string {
  return Array.from({ length: lineCount }, (_, i) => `line ${i + 1}`).join('\n');
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('CodeBlock', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockUseTheme.mockReturnValue({ isDarkMode: false });
    mockIsHtmlDocument.mockReturnValue(false);
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  // =========================================================================
  // 1. Basic rendering
  // =========================================================================

  describe('basic rendering', () => {
    it('renders code content in a pre tag', () => {
      const { container } = render(<CodeBlock language="javascript" code="const x = 1;" />);
      const pre = container.querySelector('pre');
      expect(pre).not.toBeNull();
      expect(pre!.textContent).toContain('const x = 1;');
    });

    it('displays the language label', () => {
      render(<CodeBlock language="python" code="print('hi')" />);
      expect(screen.getByText('python')).toBeInTheDocument();
    });

    it('displays "code" when language is empty', () => {
      render(<CodeBlock language="" code="some code" />);
      expect(screen.getByText('code')).toBeInTheDocument();
    });

    it('renders Copy button', () => {
      render(<CodeBlock language="js" code="x" />);
      expect(screen.getByLabelText('Copy code')).toBeInTheDocument();
    });
  });

  // =========================================================================
  // 2. Preview button visibility
  // =========================================================================

  describe('preview button', () => {
    it('shows Preview button when language is html', () => {
      render(<CodeBlock language="html" code="<div>hello</div>" />);
      expect(screen.getByLabelText('Preview HTML')).toBeInTheDocument();
    });

    it('shows Preview button when isHtmlDocument returns true', () => {
      mockIsHtmlDocument.mockReturnValue(true);
      render(<CodeBlock language="" code="<!doctype html><html></html>" />);
      expect(screen.getByLabelText('Preview HTML')).toBeInTheDocument();
    });

    it('hides Preview button for non-html language', () => {
      mockIsHtmlDocument.mockReturnValue(false);
      render(<CodeBlock language="python" code="print('hi')" />);
      expect(screen.queryByLabelText('Preview HTML')).not.toBeInTheDocument();
    });

    it('dispatches codeBlockPreview event when Preview is clicked', () => {
      const handler = vi.fn();
      window.addEventListener('codeBlockPreview', handler);

      render(<CodeBlock language="html" code="<p>test</p>" />);
      fireEvent.click(screen.getByLabelText('Preview HTML'));

      expect(handler).toHaveBeenCalledTimes(1);
      const detail = (handler.mock.calls[0][0] as CustomEvent).detail;
      expect(detail.html).toBe('<p>test</p>');

      window.removeEventListener('codeBlockPreview', handler);
    });
  });

  // =========================================================================
  // 3. Copy button
  // =========================================================================

  describe('copy button', () => {
    it('copies code to clipboard and shows "Copied!" tooltip', async () => {
      const writeText = vi.fn().mockResolvedValue(undefined);
      Object.assign(navigator, { clipboard: { writeText } });

      render(<CodeBlock language="js" code="const a = 1;" />);

      await act(async () => {
        fireEvent.click(screen.getByLabelText('Copy code'));
      });

      expect(writeText).toHaveBeenCalledWith('const a = 1;');
      // After click, tooltip should show "Copied!"
      expect(screen.getByLabelText('Copied!')).toBeInTheDocument();

      // After 2 seconds, it resets
      act(() => {
        vi.advanceTimersByTime(2000);
      });
      expect(screen.getByLabelText('Copy code')).toBeInTheDocument();
    });
  });

  // =========================================================================
  // 4. Dark mode
  // =========================================================================

  describe('dark mode', () => {
    it('renders preview button with dark mode colors', () => {
      mockUseTheme.mockReturnValue({ isDarkMode: true });
      render(<CodeBlock language="html" code="<p>dark</p>" />);
      // Preview button should exist in dark mode
      expect(screen.getByLabelText('Preview HTML')).toBeInTheDocument();
    });

    it('renders expand button with dark mode colors for large blocks', () => {
      mockUseTheme.mockReturnValue({ isDarkMode: true });
      const code = generateLargeCode(80);
      render(<CodeBlock language="text" code={code} />);
      expect(screen.getByText('Show all 80 lines')).toBeInTheDocument();
    });

    it('uses dark theme colors when isDarkMode is true', () => {
      mockUseTheme.mockReturnValue({ isDarkMode: true });
      const { container } = render(<CodeBlock language="js" code="x" />);
      // The header bar should have dark background
      const headerBox = container.querySelectorAll('.MuiBox-root')[1];
      expect(headerBox).toBeDefined();
    });

    it('uses light theme colors when isDarkMode is false', () => {
      mockUseTheme.mockReturnValue({ isDarkMode: false });
      const { container } = render(<CodeBlock language="js" code="x" />);
      const headerBox = container.querySelectorAll('.MuiBox-root')[1];
      expect(headerBox).toBeDefined();
    });
  });

  // =========================================================================
  // 5. Line truncation
  // =========================================================================

  describe('line truncation', () => {
    it('does not show expand button for code with <= 50 lines', () => {
      const code = generateLargeCode(50);
      render(<CodeBlock language="text" code={code} />);
      expect(screen.queryByText(/Show all/)).not.toBeInTheDocument();
    });

    it('shows "Show all N lines" button for code with > 50 lines', () => {
      const code = generateLargeCode(80);
      render(<CodeBlock language="text" code={code} />);
      expect(screen.getByText('Show all 80 lines')).toBeInTheDocument();
    });

    it('truncates displayed code to 50 lines by default', () => {
      const code = generateLargeCode(80);
      const { container } = render(<CodeBlock language="text" code={code} />);
      const pre = container.querySelector('pre');
      // Only first 50 lines should be rendered
      expect(pre!.textContent).toContain('line 50');
      expect(pre!.textContent).not.toContain('line 51');
    });

    it('expands to show all lines when "Show all" is clicked', () => {
      const code = generateLargeCode(80);
      const { container } = render(<CodeBlock language="text" code={code} />);

      fireEvent.click(screen.getByText('Show all 80 lines'));

      const pre = container.querySelector('pre');
      expect(pre!.textContent).toContain('line 80');
      expect(screen.getByText('Show less')).toBeInTheDocument();
    });

    it('collapses back when "Show less" is clicked', () => {
      const code = generateLargeCode(80);
      const { container } = render(<CodeBlock language="text" code={code} />);

      // Expand
      fireEvent.click(screen.getByText('Show all 80 lines'));
      // Collapse
      fireEvent.click(screen.getByText('Show less'));

      const pre = container.querySelector('pre');
      expect(pre!.textContent).not.toContain('line 51');
      expect(screen.getByText('Show all 80 lines')).toBeInTheDocument();
    });

    it('always copies the full code regardless of truncation', async () => {
      const writeText = vi.fn().mockResolvedValue(undefined);
      Object.assign(navigator, { clipboard: { writeText } });

      const code = generateLargeCode(80);
      render(<CodeBlock language="text" code={code} />);

      await act(async () => {
        fireEvent.click(screen.getByLabelText('Copy code'));
      });

      // Should copy ALL 80 lines, not just the visible 50
      expect(writeText).toHaveBeenCalledWith(code);
    });

    it('dispatches preview with full code regardless of truncation', () => {
      const handler = vi.fn();
      window.addEventListener('codeBlockPreview', handler);

      const code = generateLargeCode(80);
      mockIsHtmlDocument.mockReturnValue(true);
      render(<CodeBlock language="" code={code} />);

      fireEvent.click(screen.getByLabelText('Preview HTML'));

      const detail = (handler.mock.calls[0][0] as CustomEvent).detail;
      expect(detail.html).toBe(code);

      window.removeEventListener('codeBlockPreview', handler);
    });
  });
});
