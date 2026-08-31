import React from 'react';
import { Box } from '@mui/material';
import MessageContent from '../ChatMode/components/Chat/MessageContent';
/** Render a result string through the chat's own MessageContent, so the Jobs
 *  "Show result" shows exactly what the chat showed — a deck paged, scaled and
 *  exportable, a diagram in its sandbox, the surrounding prose as markdown —
 *  rather than one flat sandboxed page or a code block. Scoped under
 *  `.kasal-chat-root` like the A2UI branch: the chat's utilities and theme
 *  variables live under that class. */
export const ChatStyleResult: React.FC<{ text: string; dark: boolean }> = ({ text, dark }) => (
  <Box
    className="kasal-chat-root"
    data-theme={dark ? 'dark' : 'light'}
    sx={{ display: 'flex', flexDirection: 'column', minWidth: 0 }}
  >
    <MessageContent content={text} />
  </Box>
);
