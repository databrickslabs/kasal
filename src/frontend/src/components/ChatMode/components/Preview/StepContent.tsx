/**
 * What one step of a run produced, rendered as the thing it actually is.
 *
 * Three payloads travel through the same field and want three renderings:
 *  - a plan (`todo` / `plan_updated`) is a checklist, not "[>] 1. …" text;
 *  - a tool call's arguments are code — SQL, a JSON envelope — and need a
 *    highlighted block that keeps its line breaks;
 *  - everything else is prose, and a tool that reads pages returns MARKDOWN,
 *    so "## Sources" has to render as a heading rather than as two hashes.
 *
 * All three used to go through the results surface, which flattened every
 * source line into its own paragraph. That is why a CREATE TABLE arrived as
 * sixteen paragraphs and a search result as a wall of literal `###`.
 */
import React from 'react';
import ReactMarkdown, { type Components } from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { containsMarkdown } from '../../utils/markdown';
import PlanChecklist from './PlanChecklist';
import CodeSnippet from './CodeSnippet';
import type { RunStep } from './traceEventStep';

/**
 * Every element is styled here rather than through `prose-*` classes.
 *
 * `@tailwindcss/typography` is NOT installed, so those variants compile to
 * nothing: a scraped page's `# Title` fell back to the user agent's `2em`
 * while inheriting the container's 20px line-height, and a wrapped heading
 * rendered its lines ON TOP OF each other. Explicit sizes with explicit
 * leading is the only styling this content actually gets.
 */
const MARKDOWN_COMPONENTS: Components = {
  h1: ({ children }) => <h1 className="text-[15px] font-semibold leading-snug mt-4 mb-1.5">{children}</h1>,
  h2: ({ children }) => <h2 className="text-sm font-semibold leading-snug mt-4 mb-1.5">{children}</h2>,
  h3: ({ children }) => <h3 className="text-[13px] font-semibold leading-snug mt-3 mb-1">{children}</h3>,
  h4: ({ children }) => <h4 className="text-[13px] font-semibold leading-snug mt-3 mb-1">{children}</h4>,
  h5: ({ children }) => <h5 className="text-xs font-semibold leading-snug mt-2 mb-1">{children}</h5>,
  h6: ({ children }) => <h6 className="text-xs font-semibold leading-snug mt-2 mb-1">{children}</h6>,
  p: ({ children }) => <p className="my-1.5 leading-relaxed">{children}</p>,
  ul: ({ children }) => <ul className="my-1.5 pl-5 list-disc space-y-0.5">{children}</ul>,
  ol: ({ children }) => <ol className="my-1.5 pl-5 list-decimal space-y-0.5">{children}</ol>,
  li: ({ children }) => <li className="leading-relaxed">{children}</li>,
  blockquote: ({ children }) => (
    <blockquote className="my-2 pl-3 italic" style={{ borderLeft: '2px solid var(--border-color)' }}>
      {children}
    </blockquote>
  ),
  hr: () => <hr className="my-3" style={{ borderColor: 'var(--border-color)' }} />,
  // A scraped page's links are long raw URLs: they must wrap, not widen the pane.
  a: ({ href, children }) => (
    <a
      href={href}
      target="_blank"
      rel="noopener noreferrer"
      className="break-words underline"
      style={{ color: 'var(--accent)' }}
    >
      {children}
    </a>
  ),
  table: ({ children }) => (
    <div className="my-2 overflow-x-auto">
      <table className="text-xs border-collapse">{children}</table>
    </div>
  ),
  th: ({ children }) => (
    <th className="text-left font-semibold px-2 py-1" style={{ border: '1px solid var(--border-color)' }}>
      {children}
    </th>
  ),
  td: ({ children }) => (
    <td className="px-2 py-1 align-top" style={{ border: '1px solid var(--border-color)' }}>
      {children}
    </td>
  ),
  code: ({ className, children, ...props }) => {
    const text = String(children).replace(/\n$/, '');
    // A fenced block (it carries a `language-*` class, or simply has newlines)
    // goes to the same snippet component a code payload does.
    const fence = /language-(\w+)/.exec(className || '');
    if (fence || text.includes('\n')) {
      return <CodeSnippet text={text} language={fence?.[1] || 'text'} />;
    }
    return (
      <code
        className="px-1 py-0.5 rounded text-[12px] font-mono break-words"
        style={{ backgroundColor: 'var(--bg-secondary)' }}
        {...props}
      >
        {children}
      </code>
    );
  },
  // The snippet brings its own frame; a wrapping <pre> would add a second one.
  pre: ({ children }) => <>{children}</>,
};

const StepContent: React.FC<{ step: RunStep }> = ({ step }) => {
  if (step.plan) return <PlanChecklist items={step.plan} />;
  if (step.code) {
    return (
      <div className="p-4">
        <CodeSnippet text={step.code.text} language={step.code.language} title={step.label} />
      </div>
    );
  }

  const body = step.detail || '';
  return (
    <div className="p-4 text-sm" style={{ color: 'var(--text-primary)' }}>
      {containsMarkdown(body) ? (
        <ReactMarkdown remarkPlugins={[remarkGfm]} components={MARKDOWN_COMPONENTS}>
          {body}
        </ReactMarkdown>
      ) : (
        <p className="whitespace-pre-wrap leading-relaxed">{body}</p>
      )}
    </div>
  );
};

export default StepContent;
