import React from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { mdComponents } from '../../shared/a2ui/lib/markdown';
import './MemoryRecordMarkdown.css';

interface MemoryRecordMarkdownProps {
  content: string;
  /** Collapsed cards show the first few lines under a fade; expanded show it all. */
  expanded: boolean;
}

/**
 * A memory record's content, rendered.
 *
 * Records are what an agent wrote — headings, tables, bold — and shown raw they
 * read as one run-on line with the hashes and pipes in it. The styles live in a
 * plain stylesheet rather than Tailwind's `prose`: the run pane sits inside the
 * chat root where Tailwind applies, but the Memory backend page does not, and
 * one record must look the same in both.
 */
export const MemoryRecordMarkdown: React.FC<MemoryRecordMarkdownProps> = ({ content, expanded }) => (
  <div
    className="kasal-memory-md"
    data-expanded={expanded ? 'true' : 'false'}
    style={
      expanded
        ? undefined
        : {
            maxHeight: '7.5rem',
            overflow: 'hidden',
            WebkitMaskImage: 'linear-gradient(to bottom, black 70%, transparent)',
            maskImage: 'linear-gradient(to bottom, black 70%, transparent)',
          }
    }
  >
    <ReactMarkdown remarkPlugins={[remarkGfm]} components={mdComponents}>
      {content}
    </ReactMarkdown>
  </div>
);

export default MemoryRecordMarkdown;
