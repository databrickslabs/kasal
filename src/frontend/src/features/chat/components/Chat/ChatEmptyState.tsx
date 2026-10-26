import React from 'react';
import ChatStarters from './ChatStarters';

export interface ChatEmptyStateProps {
  /** Drop a starter prompt into the composer without sending it. */
  onPrefill: (text: string) => void;
}

const ChatEmptyState: React.FC<ChatEmptyStateProps> = ({ onPrefill }) => (
  <div className="w-full mt-4" data-testid="chat-empty-state">
    <ChatStarters onPick={onPrefill} />
  </div>
);

export default ChatEmptyState;
