import { describe, it, expect, vi, beforeEach } from 'vitest';
import { searchGenieSpaces, getGenieSpace } from './genie';
import { getClient } from './client';

vi.mock('./client', () => ({
  getClient: vi.fn(),
}));

describe('searchGenieSpaces', () => {
  const post = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
    (getClient as unknown as ReturnType<typeof vi.fn>).mockReturnValue({ post });
  });

  it('posts the provided query and returns the spaces', async () => {
    const spaces = [
      { id: '1', name: 'Space One' },
      { id: '2', name: 'Space Two' },
    ];
    post.mockResolvedValue({ data: { spaces } });

    const result = await searchGenieSpaces('analytics');

    expect(getClient).toHaveBeenCalledTimes(1);
    expect(post).toHaveBeenCalledWith('/api/genie/spaces/search', {
      search_query: 'analytics',
      enabled_only: true,
      page_size: 50,
    });
    expect(result).toEqual(spaces);
  });

  it('defaults search_query to empty string when query is omitted', async () => {
    post.mockResolvedValue({ data: { spaces: [] } });

    const result = await searchGenieSpaces();

    expect(post).toHaveBeenCalledWith('/api/genie/spaces/search', {
      search_query: '',
      enabled_only: true,
      page_size: 50,
    });
    expect(result).toEqual([]);
  });

  it('returns an empty array when response.data.spaces is undefined', async () => {
    post.mockResolvedValue({ data: {} });

    const result = await searchGenieSpaces('whatever');

    expect(result).toEqual([]);
  });
});

describe('getGenieSpace', () => {
  const get = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
    (getClient as unknown as ReturnType<typeof vi.fn>).mockReturnValue({ get });
  });

  it('fetches a single space by id (url-encoded) and returns it', async () => {
    const space = { id: 's 1', name: 'Space', url: 'https://example.com/genie/s' };
    get.mockResolvedValue({ data: space });

    const result = await getGenieSpace('s 1');

    expect(get).toHaveBeenCalledWith('/api/genie/spaces/s%201');
    expect(result).toEqual(space);
  });

  it('returns null when the response has no data', async () => {
    get.mockResolvedValue({ data: null });
    expect(await getGenieSpace('s1')).toBeNull();
  });

  it('returns null on error', async () => {
    get.mockRejectedValue(new Error('boom'));
    expect(await getGenieSpace('s1')).toBeNull();
  });
});
