import React from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

interface HelpCardProps {
  content: string;
}

const HelpCard: React.FC<HelpCardProps> = ({ content }) => {
  return (
    <div
      className="rounded-xl p-4 my-3"
      style={{
        backgroundColor: 'var(--bg-input)',
        border: '1px solid var(--border-color)',
      }}
    >
      <div className="prose prose-sm max-w-none">
        <ReactMarkdown remarkPlugins={[remarkGfm]}>{content}</ReactMarkdown>
      </div>
    </div>
  );
};

export default HelpCard;
