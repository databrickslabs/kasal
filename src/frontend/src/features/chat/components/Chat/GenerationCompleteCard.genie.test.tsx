/**
 * Genie selection persistence on the generation_complete card.
 *
 * The space pick + ran-state must survive leaving the session: they write
 * through to the message's resultData (stored server-side with the chat
 * session) and rehydrate from it on mount.
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';

const updateMessage = vi.fn();
vi.mock('../../store/sessionStore', () => ({
  useSessionStore: { getState: () => ({ updateMessage }) },
}));
vi.mock('../../store/appStore', () => ({
  useAppStore: (sel: (s: { toolNameMap: Record<string, string> }) => unknown) =>
    sel({ toolNameMap: {} }),
}));
vi.mock('../Cards/GenieSpaceSelector', () => ({
  default: ({ value, onChange }: { value: string; onChange: (v: string) => void }) => (
    <button data-testid="pick-space" onClick={() => onChange('space-42')}>
      {value || 'none'}
    </button>
  ),
}));

import { GenerationCompleteCard } from './ChatMessage';

const DATA = {
  agents: [{ name: 'A', role: 'r' }],
  tasks: [{ name: 'T', description: 'd', tools: ['GenieTool'] }],
} as never;

beforeEach(() => updateMessage.mockClear());

describe('GenerationCompleteCard — genie persistence', () => {
  it('writes the space pick through to the message resultData', () => {
    render(<GenerationCompleteCard data={DATA} messageId={`m-${Math.random()}`} onExecute={vi.fn()} />);
    fireEvent.click(screen.getByTestId('pick-space'));

    expect(updateMessage).toHaveBeenCalledWith(expect.any(String), {
      resultType: 'generation_complete',
      resultData: expect.objectContaining({ genieSpaceId: 'space-42', genieRan: false }),
    });
  });

  it('rehydrates the pick and ran-state from persisted resultData', () => {
    const persisted = { ...(DATA as object), genieSpaceId: 'space-7', genieRan: true } as never;
    render(<GenerationCompleteCard data={persisted} messageId={`m-${Math.random()}`} onExecute={vi.fn()} />);

    // Selector shows the restored space; run button reflects the ran state
    expect(screen.getByTestId('pick-space')).toHaveTextContent('space-7');
    expect(screen.getByRole('button', { name: /Running…/ })).toBeDisabled();
  });
});
