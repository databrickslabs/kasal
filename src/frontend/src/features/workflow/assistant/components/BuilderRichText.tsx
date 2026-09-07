import React from 'react';
import { Box } from '@mui/material';
import ChatContent from '../../../chat/components/Chat/MessageContent';
import { normalizeBuilderHtml } from '../utils/resultContent';
import { useThemeStore } from '../../../../store/theme';
import '../../../chat/chat.css';

/** The same sandboxed HTML/SVG and deck renderer used by Chat. */
export default function BuilderRichText({ content, streaming = false }: { content: string; streaming?: boolean }) {
  const dark = useThemeStore(state => state.isDarkMode);
  const source = normalizeBuilderHtml(content);
  return <Box className="kasal-chat-root" data-theme={dark ? 'dark' : 'light'} sx={{ background: 'transparent', width: '100%', minWidth: 0,
    '& button': { border: 0, backgroundColor: 'transparent', cursor: 'pointer' } }}>
    <ChatContent content={source} streaming={streaming} allowDeckEditing={false} />
  </Box>;
}
