import React from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import Box from '@mui/material/Box';
import { containsMarkdown } from '../../utils/markdown';
import { chatMarkdownSx } from '../../chatSx';

interface MessageContentProps {
  content: string;
}

const MessageContent: React.FC<MessageContentProps> = ({ content }) => {
  if (containsMarkdown(content)) {
    return (
      <Box data-testid="message-markdown" sx={{ ...chatMarkdownSx, maxWidth: 'none' }}>
        <ReactMarkdown remarkPlugins={[remarkGfm]}>{content}</ReactMarkdown>
      </Box>
    );
  }

  return (
    <Box component="p" data-testid="message-plain" sx={{ whiteSpace: 'pre-wrap' }}>
      {content}
    </Box>
  );
};

export default MessageContent;
