import { expect, it, vi } from 'vitest';
import { createMessageUpdateQueue } from './messageUpdates';

it('a terminal flush waits for an in-flight write and finishes without another write', async () => {
  let finish!: () => void;
  const write = vi.fn(() => new Promise<void>(resolve => { finish = resolve; }));
  const queue = createMessageUpdateQueue(write);
  const update = queue.enqueue('message', { content: 'final answer' });
  await Promise.resolve();
  const flushed = queue.flush('message');
  expect(flushed).toBe(update);
  let done = false;
  void flushed.then(() => { done = true; });
  await Promise.resolve();
  expect(done).toBe(false);
  finish();
  await flushed;
  expect(done).toBe(true);
  expect(write).toHaveBeenCalledTimes(1);
  await expect(queue.flush('unknown')).resolves.toBeUndefined();
});
