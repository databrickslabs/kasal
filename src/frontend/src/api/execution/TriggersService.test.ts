import { describe, it, expect, vi, beforeEach, Mock } from 'vitest';
import { TriggersService } from './TriggersService';
import { apiClient } from '../../config/api/ApiConfig';

// TriggersService uses the named `apiClient` export.
vi.mock('../../config/api/ApiConfig', () => ({
  apiClient: {
    post: vi.fn(),
    get: vi.fn(),
    delete: vi.fn(),
  },
}));

const mockPost = apiClient.post as unknown as Mock;
const mockGet = apiClient.get as unknown as Mock;
const mockDelete = apiClient.delete as unknown as Mock;

describe('TriggersService', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('enqueue POSTs to /triggers and returns the created event', async () => {
    mockPost.mockResolvedValue({
      data: { id: 1, status: 'pending', attempts: 0 },
    });
    const payload = {
      target: { kind: 'flow' as const, id: 'f1' },
      payload: { inputs: { topic: 'news' } },
    };
    const res = await TriggersService.enqueue(payload);
    expect(mockPost).toHaveBeenCalledWith('/triggers', payload);
    expect(res.id).toBe(1);
    expect(res.status).toBe('pending');
  });

  it('list passes status + limit as params', async () => {
    mockGet.mockResolvedValue({ data: { events: [], total: 0 } });
    await TriggersService.list('pending', 10);
    expect(mockGet).toHaveBeenCalledWith('/triggers', {
      params: { limit: 10, status: 'pending' },
    });
  });

  it('list omits status when not provided (defaults limit 50)', async () => {
    mockGet.mockResolvedValue({ data: { events: [], total: 0 } });
    await TriggersService.list();
    expect(mockGet).toHaveBeenCalledWith('/triggers', { params: { limit: 50 } });
  });

  it('get fetches a single event by id', async () => {
    mockGet.mockResolvedValue({
      data: { id: 5, status: 'dispatched', attempts: 1 },
    });
    const res = await TriggersService.get(5);
    expect(mockGet).toHaveBeenCalledWith('/triggers/5');
    expect(res.status).toBe('dispatched');
  });

  it('delete calls DELETE /triggers/{id}', async () => {
    mockDelete.mockResolvedValue({});
    await TriggersService.delete(3);
    expect(mockDelete).toHaveBeenCalledWith('/triggers/3');
  });

  it('listSubscriptions GETs /triggers/subscriptions', async () => {
    mockGet.mockResolvedValue({
      data: { subscriptions: [], emit_rules: [] },
    });
    const res = await TriggersService.listSubscriptions();
    expect(mockGet).toHaveBeenCalledWith('/triggers/subscriptions');
    expect(res.subscriptions).toEqual([]);
    expect(res.emit_rules).toEqual([]);
  });

  it('createSubscription POSTs to /triggers/subscriptions', async () => {
    mockPost.mockResolvedValue({
      data: { id: 7, event_type: 'research.done', enabled: true },
    });
    const body = {
      event_type: 'research.done',
      target: { kind: 'crew' as const, id: 'c1' },
    };
    const res = await TriggersService.createSubscription(body);
    expect(mockPost).toHaveBeenCalledWith('/triggers/subscriptions', body);
    expect(res.id).toBe(7);
  });

  it('deleteSubscription calls DELETE /triggers/subscriptions/{id}', async () => {
    mockDelete.mockResolvedValue({});
    await TriggersService.deleteSubscription(7);
    expect(mockDelete).toHaveBeenCalledWith('/triggers/subscriptions/7');
  });

  it('createEmitRule POSTs to /triggers/emit-rules', async () => {
    mockPost.mockResolvedValue({
      data: { id: 9, event_type: 'research.done', enabled: true },
    });
    const body = {
      on_target: { kind: 'crew' as const, id: 'c1' },
      event_type: 'research.done',
    };
    const res = await TriggersService.createEmitRule(body);
    expect(mockPost).toHaveBeenCalledWith('/triggers/emit-rules', body);
    expect(res.id).toBe(9);
  });

  it('deleteEmitRule calls DELETE /triggers/emit-rules/{id}', async () => {
    mockDelete.mockResolvedValue({});
    await TriggersService.deleteEmitRule(9);
    expect(mockDelete).toHaveBeenCalledWith('/triggers/emit-rules/9');
  });

  it('dispatch POSTs to /triggers/dispatch with a batch param', async () => {
    mockPost.mockResolvedValue({ data: { claimed: 3 } });
    const res = await TriggersService.dispatch(5);
    expect(mockPost).toHaveBeenCalledWith('/triggers/dispatch', null, {
      params: { batch: 5 },
    });
    expect(res.claimed).toBe(3);
  });

  it('dispatch defaults the batch to 10', async () => {
    mockPost.mockResolvedValue({ data: { claimed: 0 } });
    await TriggersService.dispatch();
    expect(mockPost).toHaveBeenCalledWith('/triggers/dispatch', null, {
      params: { batch: 10 },
    });
  });
});
