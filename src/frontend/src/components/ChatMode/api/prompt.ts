import { getClient } from './client';

/*
 * Prompt improvement for the chat composer. Reuses Kasal's prompt-improvement
 * endpoint (the same one the agent/task forms use): the typed request is
 * rewritten with prompt-engineering best practices (target "chat" keeps it a
 * first-person request and names the deliverable) and the improved text
 * replaces the composer's value — nothing is sent until the user does.
 */

interface PromptImprovementResponse {
  fields: Record<string, string>;
}

/**
 * Improve the user's typed chat request. Returns the improved text, or null
 * on failure (the caller keeps the current text).
 */
export async function improveChatPrompt(
  message: string,
  model?: string,
): Promise<string | null> {
  try {
    const response = await getClient().post<PromptImprovementResponse>(
      '/prompt-improvement/improve',
      {
        target: 'chat',
        fields: { message },
        model: model || undefined,
      },
    );
    return response.data?.fields?.message || null;
  } catch (err) {
    console.error('Error improving chat prompt:', err);
    return null;
  }
}
