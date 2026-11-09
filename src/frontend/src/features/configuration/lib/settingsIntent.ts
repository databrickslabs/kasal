import { settingsSections, type SettingsSectionId } from '../components/settingsSections';

/*
 * Opening Settings at a section from ANYWHERE (a tool's config, a node form).
 * Kept apart from settingsNavigation, which reads the stores: this is imported
 * by leaf components, and a store import at module load is a side effect they
 * should not inherit.
 */
const INTENT_KEY = 'kasal-settings-intent';
export const OPEN_SETTINGS_EVENT = 'kasal:open-settings';

/** Where to land when Settings is opened from elsewhere — e.g. a tool's config
 *  linking to Skills. */
export interface SettingsIntent {
  section: SettingsSectionId;
}

/** Open Settings at `intent.section`, whether or not it is already mounted. */
export function openSettingsSection(intent: SettingsIntent) {
  sessionStorage.setItem(INTENT_KEY, JSON.stringify(intent));
  window.dispatchEvent(new CustomEvent(OPEN_SETTINGS_EVENT, { detail: intent }));
}

export function readSettingsIntent(): SettingsIntent | null {
  try {
    const value = JSON.parse(sessionStorage.getItem(INTENT_KEY) || 'null');
    return value && settingsSections.some(section => section.id === value.section) ? value : null;
  } catch { return null; }
}

/** Forget the pending intent — Settings reads it once, on open. */
export function clearSettingsIntent() {
  sessionStorage.removeItem(INTENT_KEY);
}
