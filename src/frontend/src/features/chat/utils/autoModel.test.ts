import { describe, it, expect, vi, beforeEach } from 'vitest';
import { useSessionStore } from '../../../app/sessions/sessionStore';
import {
  AUTO_MODEL,
  concreteModel,
  isAutoModel,
  modelSelectionLabel,
  pickChatModel,
} from './autoModel';
import { postModelSelection } from './modelSelectionStep';

const models = [{ key: 'k1' }, { key: 'server-default' }];
const base = { stored: '', explicit: null, models, serverDefault: 'server-default' };

describe('pickChatModel', () => {
  it('defaults to Auto when available', () => {
    expect(pickChatModel({ ...base, autoAvailable: true })).toBe(AUTO_MODEL);
  });

  it('keeps an explicit model pick when Auto is available', () => {
    expect(pickChatModel({ ...base, stored: 'k1', explicit: true, autoAvailable: true })).toBe('k1');
  });

  it('keeps an explicit Auto pick', () => {
    expect(pickChatModel({ ...base, stored: 'auto', explicit: true, autoAvailable: true })).toBe(AUTO_MODEL);
  });

  it('reads a pre-Auto stored default as not chosen, any other stored model as chosen', () => {
    expect(pickChatModel({ ...base, stored: 'server-default', autoAvailable: true })).toBe(AUTO_MODEL);
    expect(pickChatModel({ ...base, stored: 'k1', autoAvailable: true })).toBe('k1');
  });

  it('behaves exactly as before when Auto is unavailable', () => {
    expect(pickChatModel({ ...base, autoAvailable: false })).toBe('server-default');
    expect(pickChatModel({ ...base, models: [{ key: 'k1' }], autoAvailable: false })).toBe('k1');
    expect(pickChatModel({ ...base, stored: 'k1', autoAvailable: false })).toBe('k1');
    expect(pickChatModel({ ...base, models: [], autoAvailable: false })).toBe('');
  });

  it('never keeps Auto when it is unavailable', () => {
    expect(pickChatModel({ ...base, stored: 'auto', explicit: true, autoAvailable: false })).toBe('server-default');
  });
});

describe('concreteModel', () => {
  it('drops Auto for endpoints that do not resolve it', () => {
    expect(isAutoModel('Auto')).toBe(true);
    expect(concreteModel('auto')).toBeUndefined();
    expect(concreteModel('')).toBeUndefined();
    expect(concreteModel('k1')).toBe('k1');
  });
});

describe('the picked model in the run activity', () => {
  beforeEach(() => {
    useSessionStore.setState({
      addMessage: vi.fn(() => 'm1'),
      addMessageToTargetSession: vi.fn(() => 'm2'),
    } as never);
  });

  it('labels a pick and a fallback', () => {
    expect(modelSelectionLabel({ requested: 'auto', model: 'databricks-claude-opus-5-5', status: 'selected' }))
      .toBe('Auto → databricks-claude-opus-5-5');
    expect(modelSelectionLabel({ requested: 'auto', model: 'm', status: 'fallback' })).toBe('Auto → m (default)');
  });

  it('posts a trace step to the session that asked', () => {
    postModelSelection({ requested: 'auto', model: 'm', status: 'selected' }, 'session-7');
    const post = vi.mocked(useSessionStore.getState().addMessageToTargetSession);
    expect(post).toHaveBeenCalledWith('session-7', 'assistant', '', expect.objectContaining({
      resultType: 'trace',
      resultData: expect.objectContaining({ label: 'Auto → m', source: 'model_selection' }),
    }));
  });

  it('posts nothing for a request that named a model', () => {
    postModelSelection(undefined, 'session-7');
    postModelSelection(null);
    expect(useSessionStore.getState().addMessageToTargetSession).not.toHaveBeenCalled();
    expect(useSessionStore.getState().addMessage).not.toHaveBeenCalled();
  });
});
