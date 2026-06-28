/**
 * The slim inline Genie-space prompt — the only crew-generation UI left in
 * the conversation (cards removed; steps fold into the activity element).
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { screen, fireEvent } from '@testing-library/react';
import { renderWithChatTheme as render } from '../../chatTestRender';

const updateMessage = vi.fn();
vi.mock('../../store/sessionStore', () => ({
  useSessionStore: { getState: () => ({ updateMessage }) },
}));
vi.mock('./GenieSpaceSelector', () => ({
  default: ({ value, onChange }: { value: string; onChange: (v: string) => void }) => (
    <button data-testid="pick-space" onClick={() => onChange('space-9')}>{value || 'none'}</button>
  ),
}));

import GenieSpacePrompt from './GenieSpacePrompt';

const DATA = { agents: [{ name: 'A' }], tasks: [{ name: 'T' }] } as never;

beforeEach(() => updateMessage.mockClear());

describe('GenieSpacePrompt', () => {
  it('runs only after a space is picked, then locks', () => {
    const onExecute = vi.fn();
    render(<GenieSpacePrompt data={DATA} messageId={`g-${Math.random()}`} onExecute={onExecute} />);
    expect(screen.getByRole('button', { name: /Select a Genie space to run/ })).toBeDisabled();

    fireEvent.click(screen.getByTestId('pick-space'));
    fireEvent.click(screen.getByRole('button', { name: /Run crew/ }));

    expect(onExecute).toHaveBeenCalledWith(DATA, 'space-9');
    expect(screen.getByRole('button', { name: /Running…/ })).toBeDisabled();
  });

  it('persists the pick and ran-state through the message resultData', () => {
    render(<GenieSpacePrompt data={DATA} messageId={`g-${Math.random()}`} onExecute={vi.fn()} />);
    fireEvent.click(screen.getByTestId('pick-space'));

    expect(updateMessage).toHaveBeenCalledWith(expect.any(String), {
      resultType: 'genie_space_prompt',
      resultData: expect.objectContaining({ genieSpaceId: 'space-9', genieRan: false }),
    });
  });

  it('rehydrates from persisted resultData (session switch / reload)', () => {
    const persisted = { ...(DATA as object), genieSpaceId: 'space-3', genieRan: true } as never;
    render(<GenieSpacePrompt data={persisted} messageId={`g-${Math.random()}`} onExecute={vi.fn()} />);
    expect(screen.getByTestId('pick-space')).toHaveTextContent('space-3');
    expect(screen.getByRole('button', { name: /Running…/ })).toBeDisabled();
  });

  it('Skip runs immediately WITHOUT a space and with GenieTool stripped from the crew', () => {
    const onExecute = vi.fn();
    const genieData = {
      agents: [{ name: 'A', tools: ['GenieTool', 'SerperDevTool'] }],
      tasks: [
        {
          name: 'T',
          tools: ['35'],
          tool_configs: { GenieTool: { spaceId: 'x' }, SerperDevTool: { k: 1 } },
        },
      ],
    } as never;
    render(<GenieSpacePrompt data={genieData} messageId={`g-${Math.random()}`} onExecute={onExecute} />);

    // Skip works without picking a space.
    fireEvent.click(screen.getByRole('button', { name: /Skip — run without Genie/ }));

    expect(onExecute).toHaveBeenCalledTimes(1);
    const [executed, spaceId] = onExecute.mock.calls[0];
    expect(spaceId).toBeUndefined();
    expect(executed.agents[0].tools).toEqual(['SerperDevTool']); // name ref stripped
    expect(executed.tasks[0].tools).toEqual([]); // id ref stripped
    expect(executed.tasks[0].tool_configs).toEqual({ SerperDevTool: { k: 1 } });

    // Both controls lock once the run starts, and the ran-state persists.
    expect(screen.getByRole('button', { name: /Running…/ })).toBeDisabled();
    expect(screen.getByRole('button', { name: /Skip — run without Genie/ })).toBeDisabled();
    expect(updateMessage).toHaveBeenCalledWith(expect.any(String), {
      resultType: 'genie_space_prompt',
      resultData: expect.objectContaining({ genieRan: true }),
    });
  });
});
