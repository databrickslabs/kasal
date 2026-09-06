import { describe, it, expect, vi, beforeEach } from 'vitest';

const post = vi.fn();
vi.mock('./client', () => ({ getClient: () => ({ post }) }));

import { uploadKnowledgeFile } from './knowledge';

beforeEach(() => {
  post.mockReset();
});

describe('uploadKnowledgeFile', () => {
  it('posts FormData to the scoped endpoint and returns the stored path', async () => {
    post.mockResolvedValue({
      data: { path: '/Volumes/x/test.txt', status: 'success', filename: 'test.txt' },
    });
    const file = new File(['hi'], 'test.txt', { type: 'text/plain' });

    const res = await uploadKnowledgeFile(file, 'chat-123');

    expect(post).toHaveBeenCalledTimes(1);
    const [url, form, opts] = post.mock.calls[0];
    expect(url).toBe('/databricks/knowledge/upload/chat-123');
    expect(form).toBeInstanceOf(FormData);
    expect((form as FormData).get('file')).toBe(file);
    expect((form as FormData).get('volume_config')).toBe('{}');
    expect((form as FormData).get('agent_ids')).toBe('[]');
    expect(opts.headers['Content-Type']).toBe('multipart/form-data');
    expect(res).toEqual({ path: '/Volumes/x/test.txt', status: 'success', filename: 'test.txt' });
  });

  it('defaults missing response fields', async () => {
    post.mockResolvedValue({ data: {} });
    const res = await uploadKnowledgeFile(new File(['x'], 'doc.pdf'), 'c1');
    expect(res).toEqual({ path: '', status: 'success', filename: 'doc.pdf' });
  });

  it('tolerates a missing data object', async () => {
    post.mockResolvedValue({});
    const res = await uploadKnowledgeFile(new File(['x'], 'a.txt'), 'c1');
    expect(res.status).toBe('success');
    expect(res.filename).toBe('a.txt');
  });

  it('throws the backend message on error status', async () => {
    post.mockResolvedValue({ data: { status: 'error', message: 'nope' } });
    await expect(uploadKnowledgeFile(new File(['x'], 'a.txt'), 'c1')).rejects.toThrow('nope');
  });

  it('throws a generic message when error has no message', async () => {
    post.mockResolvedValue({ data: { status: 'error' } });
    await expect(uploadKnowledgeFile(new File(['x'], 'a.txt'), 'c1')).rejects.toThrow('Upload failed');
  });

  it('forwards the abort signal', async () => {
    post.mockResolvedValue({ data: { path: 'p' } });
    const ctrl = new AbortController();
    await uploadKnowledgeFile(new File(['x'], 'a.txt'), 'c1', ctrl.signal);
    expect(post.mock.calls[0][2].signal).toBe(ctrl.signal);
  });

  it('surfaces the backend KasalError detail on an HTTP error', async () => {
    post.mockRejectedValue({ response: { data: { detail: "Schema 'users.x' does not exist" } } });
    await expect(uploadKnowledgeFile(new File(['x'], 'a.txt'), 'c1')).rejects.toThrow(
      "Schema 'users.x' does not exist",
    );
  });

  it('ignores a blank detail and falls back to a generic message', async () => {
    post.mockRejectedValue({ response: { data: { detail: '' } } });
    await expect(uploadKnowledgeFile(new File(['x'], 'a.txt'), 'c1')).rejects.toThrow('Upload failed');
  });

  it('falls back to the error message when an HTTP error has no detail', async () => {
    post.mockRejectedValue(new Error('Network down'));
    await expect(uploadKnowledgeFile(new File(['x'], 'a.txt'), 'c1')).rejects.toThrow('Network down');
  });

  it('falls back to a generic message for a non-Error rejection', async () => {
    post.mockRejectedValue({ foo: 'bar' });
    await expect(uploadKnowledgeFile(new File(['x'], 'a.txt'), 'c1')).rejects.toThrow('Upload failed');
  });
});
