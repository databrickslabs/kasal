import { describe, it, expect, vi, beforeEach, afterEach, Mock } from 'vitest';
import { improveChatPrompt } from './prompt';
import { getClient } from './client';

vi.mock('./client', () => ({
  getClient: vi.fn(),
}));

describe('improveChatPrompt', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    // Silence the console.error in the catch branch
    vi.spyOn(console, 'error').mockImplementation(vi.fn());
  });

  afterEach(() => {
    (console.error as Mock).mockRestore();
  });

  it('posts to /prompt-improvement/improve with target chat and the message', async () => {
    const post = vi.fn().mockResolvedValue({ data: { fields: { message: 'Improved request' } } });
    (getClient as unknown as ReturnType<typeof vi.fn>).mockReturnValue({ post });

    const result = await improveChatPrompt('make me something about sales', 'Qwen3-Coder-30B-A3B-Instruct');

    expect(post).toHaveBeenCalledWith(
      '/prompt-improvement/improve',
      {
        target: 'chat',
        fields: { message: 'make me something about sales' },
        model: 'Qwen3-Coder-30B-A3B-Instruct',
      },
    );
    expect(result).toBe('Improved request');
  });

  it('sends model as undefined when omitted', async () => {
    const post = vi.fn().mockResolvedValue({ data: { fields: { message: 'better' } } });
    (getClient as unknown as ReturnType<typeof vi.fn>).mockReturnValue({ post });

    await improveChatPrompt('some request');

    expect(post).toHaveBeenCalledWith(
      '/prompt-improvement/improve',
      {
        target: 'chat',
        fields: { message: 'some request' },
        model: undefined,
      },
    );
  });

  it('returns null when the response has no improved message', async () => {
    const post = vi.fn().mockResolvedValue({ data: { fields: {} } });
    (getClient as unknown as ReturnType<typeof vi.fn>).mockReturnValue({ post });

    const result = await improveChatPrompt('some request');

    expect(result).toBeNull();
  });

  it('returns null when the client throws', async () => {
    const post = vi.fn().mockRejectedValue(new Error('network down'));
    (getClient as unknown as ReturnType<typeof vi.fn>).mockReturnValue({ post });

    const result = await improveChatPrompt('some request');

    expect(result).toBeNull();
    expect(console.error).toHaveBeenCalled();
  });
});
