import { describe, it, expect, vi, beforeEach } from 'vitest';

vi.mock('../../config/api/ApiConfig', () => ({
  apiClient: {
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
    patch: vi.fn(),
    delete: vi.fn(),
  },
}));

import { apiClient } from '../../config/api/ApiConfig';
import { SkillService } from './SkillService';

const client = apiClient as unknown as {
  get: ReturnType<typeof vi.fn>;
  post: ReturnType<typeof vi.fn>;
  put: ReturnType<typeof vi.fn>;
  patch: ReturnType<typeof vi.fn>;
  delete: ReturnType<typeof vi.fn>;
};

beforeEach(() => {
  vi.clearAllMocks();
});

describe('SkillService', () => {
  it('unwraps the list envelope', async () => {
    client.get.mockResolvedValue({ data: { skills: [{ id: 1 }], count: 1 } });
    await expect(SkillService.list()).resolves.toEqual([{ id: 1 }]);
  });

  it('returns the validation verdict rather than throwing on an invalid draft', async () => {
    // An invalid draft is the answer the editor asked for, not a failed request.
    client.post.mockResolvedValue({
      data: { valid: false, errors: ["Skill name 'X' must be lowercase"], warnings: [] },
    });
    const result = await SkillService.validate({ name: 'X', description: 'd' });
    expect(result.valid).toBe(false);
    expect(result.errors[0]).toContain('lowercase');
  });

  it('toggles enablement through its own endpoint', async () => {
    client.patch.mockResolvedValue({ data: { id: 9, name: 'pricing' } });
    const saved = await SkillService.setEnabled(1, false);
    expect(client.patch).toHaveBeenCalledWith('/skills/1/enabled', { enabled: false });
    // Disabling a builtin returns the workspace's own copy — a DIFFERENT id,
    // which is why the config page reconciles by name.
    expect(saved.id).toBe(9);
  });

  it('uploads as multipart and defaults to not replacing', async () => {
    client.post.mockResolvedValue({ data: { id: 1 } });
    await SkillService.upload(new File(['x'], 'skill.zip'));
    expect(client.post.mock.calls[0][0]).toBe('/skills/upload?replace=false');
    expect(client.post.mock.calls[0][2]).toMatchObject({
      headers: { 'Content-Type': 'multipart/form-data' },
    });
  });

  it('requests the export as a blob', async () => {
    // A JSON-parsed zip is a corrupt zip; the responseType is the whole point.
    const blob = new Blob(['zip']);
    client.get.mockResolvedValue({ data: blob });
    const createURL = vi.fn().mockReturnValue('blob:x');
    const revokeURL = vi.fn();
    global.URL.createObjectURL = createURL;
    global.URL.revokeObjectURL = revokeURL;

    await SkillService.export(3, 'pricing');

    expect(client.get).toHaveBeenCalledWith('/skills/3/export', { responseType: 'blob' });
    expect(revokeURL).toHaveBeenCalled();
  });
});
