import React from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { containsMarkdown } from '../../utils/markdown';

interface MessageContentProps {
  content: string;
}

// Memoized on the content string: the markdown detection (10 regexes) + full
// ReactMarkdown parse used to re-run for every message on every render tick.
const MessageContent: React.FC<MessageContentProps> = React.memo(({ content }) => {
  if (containsMarkdown(content)) {
    return (
      <div className="prose prose-sm dark:prose-invert max-w-none prose-p:my-1 prose-ul:my-1 prose-ol:my-1 prose-li:my-0.5 prose-headings:my-2 prose-pre:my-2">
        <ReactMarkdown remarkPlugins={[remarkGfm]}>{content}</ReactMarkdown>
      </div>
    );
  }

  return <p className="whitespace-pre-wrap">{content}</p>;
});
MessageContent.displayName = 'MessageContent';

export default MessageContent;
