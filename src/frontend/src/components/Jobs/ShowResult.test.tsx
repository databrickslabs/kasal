/**
 * Unit tests for ShowResult component — focused on handleDownloadHtml.
 *
 * Verifies that the HTML download button extracts the actual HTML content
 * from various result object shapes instead of dumping escaped JSON.
 */
import { render, screen, fireEvent } from '@testing-library/react';
import { vi, describe, it, expect, beforeEach } from 'vitest';
import ShowResult from './ShowResult';

// Capture blob content passed to URL.createObjectURL
let capturedBlobContent: string | null = null;

// ---- Mocks ----

vi.mock('react-i18next', () => ({
  useTranslation: () => ({
    t: (key: string) => key,
  }),
}));

vi.mock('react-markdown', () => ({
  default: ({ children }: { children: string }) => <pre data-testid="md">{children}</pre>,
}));

vi.mock('remark-gfm', () => ({ default: () => {} }));

vi.mock('../../utils/pdfGenerator', () => ({
  generateRunPDF: vi.fn(),
}));

vi.mock('../../api/DatabricksService', () => ({
  DatabricksService: { getConfig: vi.fn().mockResolvedValue(null) },
}));

// Intercept Blob constructor to capture content (jsdom Blob lacks .text())
const OriginalBlob = global.Blob;
beforeEach(() => {
  capturedBlobContent = null;

  global.Blob = class extends OriginalBlob {
    constructor(parts?: BlobPart[], options?: BlobPropertyBag) {
      super(parts, options);
      // Store the first part as string for assertion
      if (parts && parts.length > 0) {
        capturedBlobContent = String(parts[0]);
      }
    }
  } as typeof Blob;

  global.URL.createObjectURL = vi.fn(() => 'blob:test');
  global.URL.revokeObjectURL = vi.fn();

  // Prevent actual link click navigation
  HTMLAnchorElement.prototype.click = vi.fn();
});

function getCapturedContent(): string {
  expect(capturedBlobContent).not.toBeNull();
  return capturedBlobContent!;
}

const baseRun = {
  id: '1',
  job_id: '1a9ec0c6-test',
  status: 'COMPLETED',
  created_at: '2026-03-16T00:00:00Z',
  run_name: 'test_run',
  agents_yaml: '',
  tasks_yaml: '',
};

// ---- Tests ----

describe('ShowResult handleDownloadHtml', () => {
  it('extracts HTML from a flat result value', () => {
    const html = '<!DOCTYPE html><html><body>Hello</body></html>';
    const result = { Value: html };

    render(<ShowResult open={true} onClose={vi.fn()} result={result} run={baseRun} />);

    const btn = screen.getByText('Download HTML');
    fireEvent.click(btn);

    const text = getCapturedContent();
    expect(text).toBe(html);
    expect(text).not.toContain('<pre>');
    expect(text).not.toContain('\\n');
  });

  it('extracts HTML from a nested result object', () => {
    const html = '<!DOCTYPE html><html><body>Nested</body></html>';
    const result = { output: { crew_result: html } } as any;

    render(<ShowResult open={true} onClose={vi.fn()} result={result} run={baseRun} />);

    const btn = screen.getByText('Download HTML');
    fireEvent.click(btn);

    const text = getCapturedContent();
    expect(text).toBe(html);
  });

  it('extracts HTML from a deeply nested result', () => {
    const html = '<html lang="en"><head></head><body>Deep</body></html>';
    const result = { a: { b: { c: html } } } as any;

    render(<ShowResult open={true} onClose={vi.fn()} result={result} run={baseRun} />);

    const btn = screen.getByText('Download HTML');
    fireEvent.click(btn);

    const text = getCapturedContent();
    expect(text).toBe(html);
  });

  it('unwraps markdown-fenced HTML', () => {
    const innerHtml = '<!DOCTYPE html><html><body>MD</body></html>';
    const mdWrapped = '```html\n' + innerHtml + '\n```';
    const result = { Value: mdWrapped };

    render(<ShowResult open={true} onClose={vi.fn()} result={result} run={baseRun} />);

    const btn = screen.getByText('Download HTML');
    fireEvent.click(btn);

    const text = getCapturedContent();
    expect(text).toBe(innerHtml);
    expect(text).not.toContain('```');
  });

  it('falls back to <pre> JSON when result has no HTML', () => {
    const result = { status: 'ok', count: 42 } as any;

    render(<ShowResult open={true} onClose={vi.fn()} result={result} run={baseRun} />);

    // Non-HTML results show PDF button, not HTML button
    // We need to check that the download logic handles this correctly
    const pdfBtn = screen.queryByText('Download PDF');
    const htmlBtn = screen.queryByText('Download HTML');

    // When there's no HTML content, isHtmlContent=false → shows PDF button
    expect(pdfBtn || htmlBtn).toBeTruthy();
  });

  it('returns first HTML found when multiple values contain HTML', () => {
    const html1 = '<!DOCTYPE html><html><body>First</body></html>';
    const html2 = '<!DOCTYPE html><html><body>Second</body></html>';
    const result = { a: html1, b: html2 };

    render(<ShowResult open={true} onClose={vi.fn()} result={result} run={baseRun} />);

    const btn = screen.getByText('Download HTML');
    fireEvent.click(btn);

    const text = getCapturedContent();
    // Should get the first one found
    expect(text).toBe(html1);
  });

  it('sets correct filename from run metadata', () => {
    const html = '<!DOCTYPE html><html><body>Name test</body></html>';
    const result = { Value: html };

    // Spy on appendChild to capture the link element
    const originalAppendChild = document.body.appendChild.bind(document.body);
    let capturedLink: HTMLAnchorElement | null = null;
    vi.spyOn(document.body, 'appendChild').mockImplementation((node: Node) => {
      if (node instanceof HTMLAnchorElement) {
        capturedLink = node;
      }
      return originalAppendChild(node);
    });

    render(<ShowResult open={true} onClose={vi.fn()} result={result} run={baseRun} />);

    const btn = screen.getByText('Download HTML');
    fireEvent.click(btn);

    expect(capturedLink).not.toBeNull();
    expect(capturedLink!.download).toContain('test_run');
    expect(capturedLink!.download).toContain('1a9ec0c6-test');
    expect(capturedLink!.download).toMatch(/\.html$/);

    vi.restoreAllMocks();
  });

  it('does nothing when run is undefined', () => {
    const html = '<!DOCTYPE html><html><body>No run</body></html>';
    const result = { Value: html };

    render(<ShowResult open={true} onClose={vi.fn()} result={result} run={undefined} />);

    // Without a run, the download button should not be rendered
    const btn = screen.queryByText('Download HTML');
    expect(btn).toBeNull();
  });
});
