import React, { useRef, useEffect } from 'react';
import { ChatMessage as ChatMessageType } from '../../types/chat';
import { ModelConfigResponse } from '../../types/dispatcher';
import { PlanData, FlowData } from '../../hooks/useDispatcher';
import { GenerationCompleteData } from '../../hooks/useGenerationStream';
import ChatMessageComponent from './ChatMessage';
import ChatInput from './ChatInput';

export interface ExecutionContext {
  crewName: string;
  agents: { name: string; role?: string }[];
  tasks: { name: string }[];
}

interface ChatContainerProps {
  messages: ChatMessageType[];
  onSend: (message: string) => void;
  onCommand?: (command: string) => void;
  onExecuteCrew?: (plan: PlanData) => void;
  onExecuteFlow?: (flow: FlowData) => void;
  onExecuteGenerated?: (data: GenerationCompleteData, spaceId?: string) => void;
  onStopExecution?: () => void;
  isLoading: boolean;
  isExecuting?: boolean;
  isGenerating?: boolean;
  executionContext?: ExecutionContext | null;
  models: ModelConfigResponse[];
  selectedModel: string;
  onModelChange: (model: string) => void;
}

const ChatContainer: React.FC<ChatContainerProps> = ({
  messages,
  onSend,
  onCommand,
  onExecuteCrew,
  onExecuteFlow,
  onExecuteGenerated,
  onStopExecution,
  isLoading,
  isExecuting,
  isGenerating,
  executionContext,
  models,
  selectedModel,
  onModelChange,
}) => {
  const messagesEndRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const handleCommand = (command: string) => {
    if (onCommand) {
      onCommand(command);
    } else {
      onSend(command);
    }
  };

  const isEmpty = messages.length === 0;

  // Empty state: everything centered vertically — greeting + input
  if (isEmpty && !isExecuting) {
    return (
      <div className="flex flex-col items-center justify-center h-full px-6">
        <div className="w-full max-w-3xl">
          {/* Greeting */}
          <div className="text-center mb-8">
            <img src={`${process.env.PUBLIC_URL || ''}/databricks-logo.png`} alt="Databricks" className="w-14 h-14 mx-auto mb-6" />

            <h1
              className="text-2xl font-semibold mb-2"
              style={{ color: 'var(--text-primary)' }}
            >
              What can I help you with?
            </h1>
            <p
              className="text-sm leading-relaxed"
              style={{ color: 'var(--text-secondary)' }}
            >
              Create agents, build crews, and execute workflows through natural conversation.
            </p>
          </div>

          {/* Input — centered */}
          <ChatInput
            onSend={onSend}
            disabled={isLoading}
            models={models}
            selectedModel={selectedModel}
            onModelChange={onModelChange}
          />
        </div>
      </div>
    );
  }

  // Conversation / executing state
  return (
    <div className="flex flex-col h-full">
      {/* Execution banner at the top */}
      {isExecuting && executionContext && (
        <div className="flex-shrink-0 py-4 animate-fade-in">
          <div className="max-w-3xl mx-auto px-4">
            <div
              className="flex items-start gap-3 rounded-xl px-4 py-3"
              style={{
                backgroundColor: 'var(--bg-secondary)',
                border: '1px solid var(--border-color)',
              }}
            >
              <div
                className="w-5 h-5 rounded-full border-2 border-t-transparent animate-spin flex-shrink-0 mt-0.5"
                style={{ borderColor: 'var(--border-color)', borderTopColor: 'var(--accent)' }}
              />
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2 mb-1">
                  <span
                    className="text-sm font-semibold"
                    style={{ color: 'var(--text-primary)' }}
                  >
                    Running crew...
                  </span>
                  {executionContext.crewName && (
                    <span
                      className="text-xs px-2 py-0.5 rounded-full font-medium"
                      style={{
                        backgroundColor: 'var(--accent)',
                        color: '#ffffff',
                      }}
                    >
                      {executionContext.crewName}
                    </span>
                  )}
                </div>
                <div className="flex flex-wrap gap-x-4 gap-y-1">
                  {executionContext.agents.length > 0 && (
                    <span className="text-xs" style={{ color: 'var(--text-secondary)' }}>
                      <span style={{ color: 'var(--text-muted)' }}>Agents:</span>{' '}
                      {executionContext.agents.map((a) => a.name).join(', ')}
                    </span>
                  )}
                  {executionContext.tasks.length > 0 && (
                    <span className="text-xs" style={{ color: 'var(--text-secondary)' }}>
                      <span style={{ color: 'var(--text-muted)' }}>Tasks:</span>{' '}
                      {executionContext.tasks.map((t) => t.name).join(', ')}
                    </span>
                  )}
                </div>
              </div>
              {onStopExecution && (
                <button
                  onClick={onStopExecution}
                  className="flex-shrink-0 flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium transition-colors hover:opacity-80"
                  style={{
                    backgroundColor: 'rgba(239, 68, 68, 0.1)',
                    color: '#ef4444',
                    border: '1px solid rgba(239, 68, 68, 0.3)',
                  }}
                  title="Stop execution"
                >
                  <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                    <path strokeLinecap="round" strokeLinejoin="round" d="M5.25 7.5A2.25 2.25 0 017.5 5.25h9a2.25 2.25 0 012.25 2.25v9a2.25 2.25 0 01-2.25 2.25h-9a2.25 2.25 0 01-2.25-2.25v-9z" />
                  </svg>
                  Stop
                </button>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Generation banner */}
      {isGenerating && !isExecuting && (
        <div className="flex-shrink-0 py-4 animate-fade-in">
          <div className="max-w-3xl mx-auto px-4">
            <div
              className="flex items-center gap-3 rounded-xl px-4 py-3"
              style={{
                backgroundColor: 'var(--bg-secondary)',
                border: '1px solid var(--border-color)',
              }}
            >
              <div
                className="w-5 h-5 rounded-full border-2 border-t-transparent animate-spin flex-shrink-0"
                style={{ borderColor: 'var(--border-color)', borderTopColor: 'var(--accent)' }}
              />
              <span
                className="text-sm font-semibold"
                style={{ color: 'var(--text-primary)' }}
              >
                Generating crew...
              </span>
            </div>
          </div>
        </div>
      )}

      {/* Messages */}
      <div className="flex-1 overflow-y-auto">
        <div className="py-6 max-w-3xl mx-auto w-full">
          {messages.map((msg) => (
            <ChatMessageComponent
              key={msg.id}
              message={msg}
              onCommand={handleCommand}
              onExecuteCrew={onExecuteCrew}
              onExecuteFlow={onExecuteFlow}
              onExecuteGenerated={onExecuteGenerated}
            />
          ))}
          <div ref={messagesEndRef} />
        </div>
      </div>

      {/* Input pinned to bottom */}
      <div className="max-w-3xl mx-auto w-full">
        <ChatInput
          onSend={onSend}
          disabled={isLoading}
          models={models}
          selectedModel={selectedModel}
          onModelChange={onModelChange}
        />
      </div>
    </div>
  );
};

export default ChatContainer;
