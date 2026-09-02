import type { ChatMessage } from '../types/chat';
import type { TranscriptTurn } from '../../../api/tools/SkillService';

/**
 * The chat-side half of "create a skill by chatting".
 *
 * Skill creation is a product action — a dedicated generation call on the
 * backend (POST /skills/draft) — not knowledge an agent has to load mid-turn.
 * This module owns the pure parts: recognising the request, splitting it into
 * the two modes, building the transcript for capture mode, and turning the
 * validated draft into the ```skill block the chat renders as a save card.
 */

/** Turns of the current conversation sent for capture mode (the tail). */
export const TRANSCRIPT_TURNS = 30;

export interface SkillCommand {
  /** capture: mine the conversation; blank: draft from the request alone. */
  mode: 'capture' | 'blank';
  request: string;
}

const SLASH = /^\/skill(?:\s+([\s\S]*))?$/i;
// Plain-language asks. Deliberately narrow: a verb of creation + "skill" in
// the same sentence; "which skills exist?" or "skill issue" must not match.
const NATURAL = /\b(create|make|write|draft|build|save|turn\b.*\binto)\b[^.?!]{0,80}\b(a |this |it |that |the )?(new )?skill\b/i;
const CAPTURE_HINT = /\b(this|our|the)\s+(chat|conversation|exchange|thread|session)\b|\bwhat we (learned|did|discussed)\b|\bsave (this|it|that)\b/i;

/** Parse a chat message into a skill command, or null when it is not one. */
export function parseSkillCommand(message: string): SkillCommand | null {
  const text = message.trim();
  const slash = text.match(SLASH);
  if (slash) {
    const rest = (slash[1] || '').trim();
    // Bare "/skill" (or "/skill capture") captures the conversation; anything
    // else is the request for a blank-page draft.
    if (!rest || /^capture$/i.test(rest)) return { mode: 'capture', request: '' };
    return { mode: CAPTURE_HINT.test(rest) ? 'capture' : 'blank', request: rest };
  }
  if (NATURAL.test(text)) {
    return { mode: CAPTURE_HINT.test(text) ? 'capture' : 'blank', request: text };
  }
  return null;
}

/** The conversation tail as user/assistant turns, oldest first. */
export function buildTranscript(messages: ChatMessage[]): TranscriptTurn[] {
  return messages
    .filter((m) => (m.role === 'user' || m.role === 'assistant') && m.content?.trim())
    .slice(-TRANSCRIPT_TURNS)
    .map((m) => ({ role: m.role as 'user' | 'assistant', content: m.content }));
}

/** SKILL.md text for a draft — what the ```skill card parses and saves. */
export function toSkillMarkdown(draft: { name: string; description: string; body: string }): string {
  const description = JSON.stringify(draft.description || '');
  return `---\nname: ${draft.name}\ndescription: ${description}\n---\n\n${draft.body.trim()}\n`;
}

/** The assistant message the chat posts: one line, then the fenced block. */
export function draftMessage(draft: {
  name: string;
  description: string;
  body: string;
  valid: boolean;
  errors?: string[];
}): string {
  const intro = draft.valid
    ? `Here's a draft of **${draft.name}** — review it, then save it to your teamspace:`
    : `Here's a draft of **${draft.name || 'the skill'}** — it did not pass validation yet (${(draft.errors || []).join('; ')}). You can still edit and save it from Configuration → Skills, or ask me to fix it:`;
  return `${intro}\n\n\`\`\`skill\n${toSkillMarkdown(draft)}\`\`\``;
}
