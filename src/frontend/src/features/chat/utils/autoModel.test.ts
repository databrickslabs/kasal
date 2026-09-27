import { describe, it, expect, vi, beforeEach } from 'vitest';
import { useSessionStore } from '../../../app/sessions/sessionStore';
import {
  AUTO_MODEL,
  concreteModel,
  isAutoModel,
  modelSelectionLabel,
  pickChatModel,
  resolveChatModel,
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

describe('resolveChatModel (the live list)', () => {
  const live = { ...base, autoAvailable: false };

  it('never overrides an explicit choice that is still enabled', () => {
    expect(resolveChatModel({ ...live, stored: 'k1', explicit: true })).toBe('k1');
    expect(resolveChatModel({ ...live, stored: 'k1', explicit: true, autoAvailable: true })).toBe('k1');
  });

  it('falls back from a disabled choice to Auto when available', () => {
    expect(resolveChatModel({ ...live, stored: 'gone', explicit: true, autoAvailable: true })).toBe(AUTO_MODEL);
  });

  it('falls back from a disabled choice to the default, else the first enabled model', () => {
    expect(resolveChatModel({ ...live, stored: 'gone', explicit: true })).toBe('server-default');
    expect(resolveChatModel({ ...live, stored: 'gone', explicit: true, models: [{ key: 'k1' }] })).toBe('k1');
  });

  it('falls back to the default when Auto is lost while selected', () => {
    expect(resolveChatModel({ ...live, stored: AUTO_MODEL, explicit: true })).toBe('server-default');
  });

  it('switches to Auto when it becomes available and the user never chose', () => {
    expect(resolveChatModel({ ...live, stored: 'server-default', explicit: null, autoAvailable: true })).toBe(AUTO_MODEL);
    expect(resolveChatModel({ ...live, stored: '', explicit: null, autoAvailable: true })).toBe(AUTO_MODEL);
  });

  it('leaves the pick alone while the list is empty (not known)', () => {
    expect(resolveChatModel({ ...live, stored: 'k1', explicit: true, models: [] })).toBe('k1');
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

  it('says why Auto fell back, and ignores a reason it does not know', () => {
    expect(modelSelectionLabel({ requested: 'auto', model: 'm', status: 'fallback', reason: 'unreachable' }))
      .toBe('Auto → m (default: decision model unreachable)');
    expect(modelSelectionLabel({ requested: 'auto', model: 'm', status: 'fallback', reason: 'new_code' }))
      .toBe('Auto → m (default)');
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
