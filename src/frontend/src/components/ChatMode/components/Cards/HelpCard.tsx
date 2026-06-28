import React from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import Box from '@mui/material/Box';
import { chatMarkdownSx } from '../../chatSx';

interface HelpCardProps {
  content: string;
}

const HelpCard: React.FC<HelpCardProps> = ({ content }) => {
  return (
    <Box
      data-testid="help-card"
      sx={{
        borderRadius: '12px',
        p: 2,
        my: 1.5,
        backgroundColor: 'background.paper',
        border: 1,
        borderColor: 'divider',
      }}
    >
      <Box sx={{ ...chatMarkdownSx, maxWidth: 'none' }}>
        <ReactMarkdown remarkPlugins={[remarkGfm]}>{content}</ReactMarkdown>
      </Box>
    </Box>
  );
};

export default HelpCard;
