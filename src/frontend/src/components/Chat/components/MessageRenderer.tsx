import React from 'react';
import { Link } from '@mui/material';
import OpenInNewIcon from '@mui/icons-material/OpenInNew';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { urlPattern, isMarkdown } from '../utils/textProcessing';

// Render text with clickable links
export const renderWithLinks = (text: string) => {
  const parts = text.split(urlPattern);
  return parts.map((part, index) => {
    if (part.match(urlPattern)) {
      return (
        <Link
          key={index}
          href={part}
          target="_blank"
          rel="noopener noreferrer"
          sx={{
            display: 'inline-flex',
            alignItems: 'center',
            gap: 0.5,
            color: 'primary.main',
            textDecoration: 'none',
            '&:hover': {
              textDecoration: 'underline'
            }
          }}
        >
          {part}
          <OpenInNewIcon sx={{ fontSize: 16 }} />
        </Link>
      );
    }
    return part;
  });
};

interface MessageContentProps {
  content: string;
}

export const MessageContent: React.FC<MessageContentProps> = ({ content }) => {
  // Check if content is markdown
  if (isMarkdown(content)) {
    return (
      <ReactMarkdown 
        remarkPlugins={[remarkGfm]}
        components={{
          a: ({ href, children }) => (
            <Link
              href={href}
              target="_blank"
              rel="noopener noreferrer"
              sx={{
                display: 'inline-flex',
                alignItems: 'center',
                gap: 0.5,
                color: 'primary.main',
                textDecoration: 'none',
                '&:hover': {
                  textDecoration: 'underline'
                }
              }}
            >
              {children}
              <OpenInNewIcon sx={{ fontSize: 16 }} />
            </Link>
          ),
          code: ({ children, className, ...props }) => {
            const isInline = !className || !className.includes('language-');
            if (isInline) {
              return (
                <code
                  style={{
                    backgroundColor: 'rgba(0, 0, 0, 0.08)',
                    padding: '2px 4px',
                    borderRadius: 4,
                    fontFamily: 'monospace',
                    fontSize: '0.9em'
                  }}
                  {...props}
                >
                  {children}
                </code>
              );
            }
            return (
              <pre
                style={{
                  backgroundColor: 'rgba(0, 0, 0, 0.05)',
                  padding: 12,
                  borderRadius: 8,
                  overflow: 'auto',
                  fontFamily: 'monospace',
                  fontSize: '0.875em',
                  margin: '8px 0',
                  maxWidth: '100%',
                  maxHeight: '400px',
                }}
              >
                <code className={className} {...props}>
                  {children}
                </code>
              </pre>
            );
          }
        }}
      >
        {content}
      </ReactMarkdown>
    );
  }

  // Plain text with URL detection
  return <>{renderWithLinks(content)}</>;
};