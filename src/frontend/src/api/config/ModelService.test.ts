import { vi, beforeEach, describe, it, expect } from 'vitest';
import { ModelService } from './ModelService';
import { apiClient } from '../../config/api/ApiConfig';

vi.mock('../../config/api/ApiConfig', () => ({
  apiClient: {
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
    delete: vi.fn(),
    patch: vi.fn(),
  },
  config: {
    apiUrl: 'http://localhost:8000/api/v1',
  },
}));

const apiModel = (key: string, supports_reasoning_effort: boolean) => ({
  id: 1,
  key,
  name: key,
  provider: 'openai',
  temperature: 0.7,
  context_window: 400000,
  max_output_tokens: 32000,
  extended_thinking: false,
  enabled: true,
  supports_reasoning_effort,
  created_at: '2026-07-25T00:00:00',
  updated_at: '2026-07-25T00:00:00',
});

describe('ModelService', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    ModelService.getInstance().clearCaches();
  });

  // Regression: supports_reasoning_effort is derived server-side from the same
  // allow-list the engine uses. It was dropped by convertApiResponseToModels, so
  // the Reasoning Effort control read it as undefined and told the user their
  // GPT-5 model "has no reasoning budget" while the engine happily applied one.
  it('carries supports_reasoning_effort through to the model map', async () => {
    const models = [
      apiModel('gpt-5.6-terra', true),
      apiModel('Qwen3-Coder-30B-A3B-Instruct', false),
    ];
    // First call: initializeModelsIfNeeded probes /models; second: /models/enabled.
    (apiClient.get as ReturnType<typeof vi.fn>).mockResolvedValue({
      data: { models, count: models.length },
    });

    const result = await ModelService.getInstance().getEnabledModels();

    expect(result['gpt-5.6-terra'].supports_reasoning_effort).toBe(true);
    expect(result['Qwen3-Coder-30B-A3B-Instruct'].supports_reasoning_effort).toBe(false);
  });

  it('treats a missing supports_reasoning_effort as unsupported', async () => {
    const legacy = { ...apiModel('gpt-4o', false) } as Record<string, unknown>;
    delete legacy.supports_reasoning_effort;
    (apiClient.get as ReturnType<typeof vi.fn>).mockResolvedValue({
      data: { models: [legacy], count: 1 },
    });

    const result = await ModelService.getInstance().getEnabledModels();

    expect(result['gpt-4o'].supports_reasoning_effort).toBe(false);
  });
});
