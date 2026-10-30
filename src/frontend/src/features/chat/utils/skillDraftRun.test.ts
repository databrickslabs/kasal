import { beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('../../../api/tools/SkillService', () => ({
  SkillService: { draftWithTrace: vi.fn() },
}));

import { SkillService, type SkillDraft } from '../../../api/tools/SkillService';
import type { ChatMessage } from '../types/chat';
import { runSkillDraft } from './skillDraftRun';

const draftWithTrace = vi.mocked(SkillService.draftWithTrace);

const DRAFT: SkillDraft = {
  name: 'writing-release-notes',
  description: 'Use when drafting release notes.',
  body: '# Release notes',
  valid: true,
  errors: [],
  warnings: [],
  model: 'served-model',
  attempts: 1,
  job_id: 'job-1',
};

function harness() {
  const messages = new Map<string, Partial<ChatMessage>>();
  const posted: string[] = [];
  let n = 0;
  const io = {
    post: vi.fn((content: string, extra?: Partial<ChatMessage>) => {
      const id = `m${(n += 1)}`;
      messages.set(id, { content, ...extra });
      posted.push(content);
      return id;
    }),
    update: vi.fn((id: string, updates: Partial<ChatMessage>) => {
      messages.set(id, { ...messages.get(id), ...updates });
    }),
  };
  return { io, messages, posted };
}

const cmd = { mode: 'blank' as const, request: 'a skill for release notes' };

beforeEach(() => vi.clearAllMocks());

describe('runSkillDraft', () => {
  it('puts the run on the drafting step as soon as it starts, so its activity opens live', async () => {
    const { io, messages } = harness();
    let finish: (d: SkillDraft) => void = () => {};
    draftWithTrace.mockImplementation((_r, _t, _m, onStarted) => {
      onStarted('job-1');
      return new Promise<SkillDraft>((resolve) => { finish = resolve; });
    });

    const pending = runSkillDraft(cmd, undefined, 'm', io);
    // Still drafting: the step already carries the run id RunProgress reads the trace by.
    const step = messages.get('m1');
    expect(step?.resultType).toBe('trace');
    expect(step?.executionId).toBe('job-1');
    expect((step?.resultData as { label: string }).label).toBe('Drafting skill');

    finish(DRAFT);
    await pending;
    const done = messages.get('m1');
    expect(done?.executionId).toBe('job-1');
    expect((done?.resultData as { label: string }).label).toBe('Skill drafted');
    expect(draftWithTrace).toHaveBeenCalledWith(cmd.request, undefined, 'm', expect.any(Function));
  });

  it('keeps a failed draft openable and says why', async () => {
    const { io, messages, posted } = harness();
    draftWithTrace.mockImplementation(async (_r, _t, _m, onStarted) => {
      onStarted('job-2');
      throw new Error('endpoint down');
    });

    await runSkillDraft(cmd, undefined, undefined, io);
    expect(messages.get('m1')?.executionId).toBe('job-2');
    expect(posted.at(-1)).toBe('Could not draft the skill: endpoint down');
  });

  it('posts the draft card when the run completes', async () => {
    const { io, posted } = harness();
    draftWithTrace.mockResolvedValue(DRAFT);
    await runSkillDraft(cmd, [{ role: 'user', content: 'hi' }], undefined, io);
    expect(posted.at(-1)).toContain('```skill');
  });
});
