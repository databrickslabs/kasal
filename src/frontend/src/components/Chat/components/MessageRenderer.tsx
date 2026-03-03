import React from 'react';
import { Link } from '@mui/material';
import OpenInNewIcon from '@mui/icons-material/OpenInNew';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { urlPattern, isMarkdown } from '../utils/textProcessing';
import { CodeBlock } from './CodeBlock';

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
          p: ({ children }) => (
            <p style={{ margin: '4px 0' }}>{children}</p>
          ),
          ul: ({ children }) => (
            <ul style={{ margin: '2px 0', paddingLeft: 20 }}>{children}</ul>
          ),
          ol: ({ children }) => (
            <ol style={{ margin: '2px 0', paddingLeft: 32 }}>{children}</ol>
          ),
          li: ({ children }) => (
            <li style={{ margin: 0, padding: 0 }}>{children}</li>
          ),
          code: ({ children, className, ...props }) => {
            const isInline = !className || !className.includes('language-');
            if (isInline) {
              const text = String(children).replace(/\n$/, '');
              const isCommand = text.startsWith('/');

              if (isCommand) {
                // Show short label for /load and /run commands, full text for others
                const actionMatch = text.match(/^\/(load|run)\s+(crew|flow)\s+.+/);
                const displayText = actionMatch ? actionMatch[1] : text;
                return (
                  <code
                    role="button"
                    tabIndex={0}
                    title={text}
                    onClick={() => {
                      window.dispatchEvent(
                        new CustomEvent('chatCommandClick', { detail: { command: text } })
                      );
                    }}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter' || e.key === ' ') {
                        e.preventDefault();
                        window.dispatchEvent(
                          new CustomEvent('chatCommandClick', { detail: { command: text } })
                        );
                      }
                    }}
                    style={{
                      backgroundColor: 'rgba(25, 118, 210, 0.08)',
                      color: '#1565c0',
                      padding: '2px 6px',
                      borderRadius: 4,
                      fontFamily: 'monospace',
                      fontSize: '0.9em',
                      border: '1px solid rgba(25, 118, 210, 0.3)',
                      cursor: 'pointer',
                      transition: 'background-color 0.15s',
                    }}
                    onMouseEnter={(e) => {
                      (e.currentTarget as HTMLElement).style.backgroundColor = 'rgba(25, 118, 210, 0.16)';
                    }}
                    onMouseLeave={(e) => {
                      (e.currentTarget as HTMLElement).style.backgroundColor = 'rgba(25, 118, 210, 0.08)';
                    }}
                    {...props}
                  >
                    {displayText}
                  </code>
                );
              }

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
            const lang = className?.replace('language-', '') || '';
            const codeString = String(children).replace(/\n$/, '');
            return <CodeBlock language={lang} code={codeString} />;
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