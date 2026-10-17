/**
 * The teamspace (group) this browser tab works in.
 *
 * localStorage is shared by every tab of the origin. When the selection lived
 * only there, switching teamspace in one tab silently re-targeted every other
 * open tab: its saves, runs and API keys went to the new teamspace while the
 * tab still showed the old one. So each tab keeps its own selection in
 * sessionStorage, which is per tab and survives a reload. localStorage keeps
 * the last selection only to seed tabs opened later.
 *
 * Read the selection through `getTabGroupId()`, never from localStorage.
 */

/** The last selection in any tab; seeds tabs opened later. */
const SHARED_GROUP_KEY = 'selectedGroupId';
/** This tab's own selection. An empty value means "this tab has none". */
const TAB_GROUP_KEY = 'kasal-tab-group';

export const TEAMSPACE_REQUIRED_MESSAGE =
  'You no longer have access to this teamspace. Pick a teamspace from the teamspace menu, then try again.';

// Bound once, at load: a tab's sessionStorage never changes.
const tabStorage: Storage | null = (() => {
  try { return window.sessionStorage; } catch { return null; }
})();

function sharedStorage(): Storage | null {
  try { return window.localStorage; } catch { return null; }
}

function read(storage: Storage | null, key: string): string | null {
  try { return storage?.getItem(key) ?? null; } catch { return null; }
}

function write(storage: Storage | null, key: string, value: string | null): void {
  try {
    if (value === null) storage?.removeItem(key);
    else storage?.setItem(key, value);
  } catch { /* storage full or blocked */ }
}

// Set when the server refused this tab's teamspace during this page's life.
// Kept in memory on purpose: a reload re-reads everything under a fresh scope.
let lostGroupId: string | null = null;

/** This tab's teamspace, or null when it has none. */
export function getTabGroupId(): string | null {
  const own = read(tabStorage, TAB_GROUP_KEY);
  if (own !== null) return own || null;
  // Not pinned yet (no selection existed when the tab opened).
  return read(sharedStorage(), SHARED_GROUP_KEY);
}

/** Adopt the last selection as this tab's own. Runs once, when the tab opens. */
export function pinTabGroup(): void {
  if (read(tabStorage, TAB_GROUP_KEY) !== null) return; // a reload keeps the tab's choice
  const last = read(sharedStorage(), SHARED_GROUP_KEY);
  if (last) write(tabStorage, TAB_GROUP_KEY, last);
}

/** The user (or the membership check) chose this tab's teamspace. */
export function setTabGroupId(groupId: string): void {
  lostGroupId = null;
  write(tabStorage, TAB_GROUP_KEY, groupId);
  write(sharedStorage(), SHARED_GROUP_KEY, groupId);
}

/**
 * The server refused `deniedGroupId`: if that is this tab's teamspace, drop it
 * until the user picks one. A late denial for a teamspace the tab has already
 * left does not touch the current one.
 */
export function markTabGroupAccessLost(deniedGroupId?: string | null): void {
  const groupId = getTabGroupId();
  if (deniedGroupId && deniedGroupId !== groupId) return;
  lostGroupId = groupId ?? '';
  write(tabStorage, TAB_GROUP_KEY, '');
  // Do not seed new tabs into a teamspace the user can no longer open.
  if (groupId && read(sharedStorage(), SHARED_GROUP_KEY) === groupId) {
    write(sharedStorage(), SHARED_GROUP_KEY, null);
  }
}

/** True once this tab has lost its teamspace and the user has not picked another. */
export function tabGroupAccessLost(): boolean {
  return lostGroupId !== null;
}
