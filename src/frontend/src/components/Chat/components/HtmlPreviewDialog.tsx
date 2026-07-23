import React, { useState, useEffect, useCallback, useMemo } from 'react';
import ShowResult from '../../Jobs/ShowResult';

/**
 * Singleton component that listens for 'codeBlockPreview' custom events
 * dispatched by CodeBlock and renders ShowResult in a stable location
 * outside the ReactMarkdown tree.
 *
 * Must be rendered exactly once in the chat panel.
 */
export const HtmlPreviewDialog: React.FC = () => {
  const [previewHtml, setPreviewHtml] = useState<string | null>(null);

  const handlePreviewEvent = useCallback((e: Event) => {
    const detail = (e as CustomEvent<{ html: string }>).detail;
    setPreviewHtml(detail.html);
  }, []);

  useEffect(() => {
    window.addEventListener('codeBlockPreview', handlePreviewEvent);
    return () => window.removeEventListener('codeBlockPreview', handlePreviewEvent);
  }, [handlePreviewEvent]);

  // Stabilize the result object so ShowResult's useEffect doesn't re-fire on
  // every render (it depends on the `result` reference).
  const result = useMemo(
    () => (previewHtml !== null ? { 'HTML Preview': previewHtml } : null),
    [previewHtml],
  );

  if (!result) return null;

  return (
    <ShowResult
      open={true}
      onClose={() => setPreviewHtml(null)}
      result={result}
    />
  );
};
