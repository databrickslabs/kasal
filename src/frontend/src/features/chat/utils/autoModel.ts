/**
 * "Auto" in the chat model selector: the decision model picks the model per
 * message, from the models the workspace has enabled.
 *
 * `auto` is only ever a REQUEST. The backend resolves it (dispatcher and
 * ExecutionService.create_execution) before anything calls a model, and
 * reports what it picked as `model_selection`. Requests to endpoints that do not
 * resolve Auto (prompt improvement, skill drafts, saving a crew, slide edits)
 * send `concreteModel(...)` instead, which leaves the choice to the server's
 * default exactly as when no model is selected.
 */
import i18n from 'i18next';
import type { ModelSelection } from '../types/execution';
import { fallbackModelKey } from '../../../utils/modelFallback';

export type { ModelSelection };

/** A translated string, or its English default before i18n has initialised. */
function tr(key: string, defaultValue: string, vars: Record<string, string> = {}): string {
  const text = i18n.isInitialized ? i18n.t(key, { ...vars, defaultValue }) : '';
  return typeof text === 'string' && text
    ? text
    : defaultValue.replace(/\{\{(\w+)\}\}/g, (_, name: string) => vars[name] ?? '');
}

export const AUTO_MODEL = 'auto';

/** Set when the user picked a model themselves (see pickChatModel). */
export const MODEL_EXPLICIT_STORAGE_KEY = 'kasal-chat-model-explicit';

export function isAutoModel(model: string | null | undefined): boolean {
  return (model ?? '').trim().toLowerCase() === AUTO_MODEL;
}

/** The model to send to an endpoint that does not resolve Auto. */
export function concreteModel(model: string | null | undefined): string | undefined {
  return model && !isAutoModel(model) ? model : undefined;
}

/**
 * The composer's model on load.
 *
 * - Auto available: Auto is the default. A model the user picked themselves
 *   stays picked. A stored model with no explicit flag predates Auto: it counts
 *   as the user's choice unless it is the server default, which is what the
 *   selector used to store on its own.
 * - Auto unavailable: exactly the old behaviour (keep what is stored, else the
 *   server default when enabled, else the first enabled model). A stored `auto`
 *   is treated as nothing stored.
 */
export function pickChatModel(args: {
  stored: string;
  explicit: boolean | null;
  models: { key: string }[];
  autoAvailable: boolean;
  serverDefault: string;
}): string {
  const { stored, explicit, models, autoAvailable, serverDefault } = args;
  const storedModel = isAutoModel(stored) ? '' : stored;
  if (autoAvailable) {
    const chosen = explicit ?? (!!storedModel && storedModel !== serverDefault);
    return chosen && storedModel ? storedModel : AUTO_MODEL;
  }
  if (storedModel) return storedModel;
  return fallbackModelKey(models, serverDefault);
}

/**
 * The composer's model given the LIVE model list — `pickChatModel`, plus the
 * rule for a stored model that is no longer enabled.
 *
 * Re-run whenever the enabled models or Auto's availability change, from the
 * user's stored preference (not the current effective model), so:
 * - an explicit choice that is still enabled is never overridden;
 * - a disabled choice falls back to Auto when available, else the server
 *   default, else the first enabled model — and returns if re-enabled;
 * - Auto lost while selected falls back to the default; Auto gained is taken
 *   when the user never chose a model themselves (pickChatModel's rule).
 * An empty list means nothing to validate against, so the pick stands.
 */
export function resolveChatModel(args: Parameters<typeof pickChatModel>[0]): string {
  const key = pickChatModel(args);
  if (!key || isAutoModel(key) || args.models.length === 0) return key;
  if (args.models.some((m) => m.key === key)) return key;
  return args.autoAvailable ? AUTO_MODEL : fallbackModelKey(args.models, args.serverDefault);
}

/** "Auto → model" for the run activity, or the fallback wording. */
export function modelSelectionLabel(selection: ModelSelection): string {
  const model = selection.model ?? tr('chat.autoModel.defaultModel', 'the default model');
  return selection.status === 'selected'
    ? tr('chat.autoModel.picked', 'Auto → {{model}}', { model })
    : tr('chat.autoModel.fellBack', 'Auto → {{model}} (default)', { model });
}

export function modelSelectionDetail(selection: ModelSelection): string {
  return selection.status === 'selected'
    ? tr('chat.autoModel.pickedDetail', 'The decision model chose this model for the message.')
    : tr(
        'chat.autoModel.fellBackDetail',
        'The decision model did not choose, so the workspace default was used.',
      );
}
