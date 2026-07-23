import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { MCPService } from '../../../../api/MCPService';
import type { MCPServerConfig } from '../../../Configuration/MCP/MCPConfiguration';
import { usePermissionStore } from '../../../../store/permissions';
import ChatMcpCatalog from './ChatMcpCatalog';

/**
 * Chat-native MCP "Connect a tool" dialog.
 *
 * A tokenized (chat `--bg-*`/`--text-*`) alternative to the MUI MCPConfigDialog,
 * used when the picker's "Connect a tool" action is triggered from ChatMode so
 * the surface matches the chat aesthetic instead of the app's MUI config look.
 *
 * RBAC / two-tier model — MCP is registered globally, then enabled per-workspace:
 *  - System (Kasal) admin: sees BOTH controls per server on one row — a "Global"
 *    toggle (availability to all workspaces) and a "This workspace" toggle
 *    (enable here) — plus register (add) and delete. A server only appears in
 *    chat once it's globally available AND enabled for the workspace.
 *  - Workspace admin (non-system): sees only the single "This workspace" enable
 *    toggle — no global control, no register, no delete.
 *  - Operators/editors: never reach this dialog (the picker hides the "Connect a
 *    tool" action for anyone who isn't a workspace/system admin).
 *
 * Rendered inside #kasal-chat-root (NOT portaled) so the chat CSS tokens apply;
 * `position: fixed` escapes the chat layout's overflow clipping. Buttons set
 * padding INLINE because the #kasal-chat-root reset zeroes Tailwind px/py.
 */
export interface ChatMcpDialogProps {
  open: boolean;
  onClose: () => void;
}

const svc = () => MCPService.getInstance();

/** A per-server row merging the global catalog entry with the workspace-effective
 *  entry (matched by canonical lowercase name). */
interface ServerRow {
  name: string;
  server_type?: string;
  server_url?: string;
  /** Global catalog entry — present for system admins; `enabled` = availability. */
  base: MCPServerConfig | null;
  /** Workspace-effective entry — `enabled` = enabled for THIS workspace. */
  effective: MCPServerConfig | null;
}

const Toggle: React.FC<{
  checked: boolean;
  disabled?: boolean;
  label: string;
  onChange: () => void;
}> = ({ checked, disabled, label, onChange }) => (
  <button
    type="button"
    role="switch"
    aria-checked={checked}
    aria-label={label}
    title={label}
    disabled={disabled}
    onClick={onChange}
    className="relative flex-shrink-0 rounded-full transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
    style={{
      width: 34,
      height: 20,
      padding: 0,
      // Neutral (ink) "on" state — no accent/red, so it stays theme-agnostic.
      backgroundColor: checked ? 'var(--text-secondary)' : 'var(--border-color)',
    }}
  >
    <span
      className="absolute rounded-full transition-all"
      style={{ top: 2, left: checked ? 16 : 2, width: 16, height: 16, backgroundColor: '#fff' }}
    />
  </button>
);

/** A small stacked control: tiny caption above the toggle (used for the two-toggle
 *  system-admin layout so both fit on one row). */
const LabeledToggle: React.FC<{
  caption: string;
  checked: boolean;
  disabled?: boolean;
  ariaLabel: string;
  onChange: () => void;
}> = ({ caption, checked, disabled, ariaLabel, onChange }) => (
  <div className="flex flex-col items-center gap-1 flex-shrink-0">
    <span className="text-[9px] font-semibold uppercase tracking-wide whitespace-nowrap" style={{ color: 'var(--text-muted)' }}>
      {caption}
    </span>
    <Toggle checked={checked} disabled={disabled} label={ariaLabel} onChange={onChange} />
  </div>
);

const ChatMcpDialog: React.FC<ChatMcpDialogProps> = ({ open, onClose }) => {
  const isSystemAdmin = usePermissionStore((s) => s.isSystemAdmin);
  const [baseServers, setBaseServers] = useState<MCPServerConfig[] | null>(null);
  const [wsServers, setWsServers] = useState<MCPServerConfig[] | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState<string | null>(null);

  // Inline "add server" panel — system admins only (registration is global). Two
  // sources: the Databricks catalog (Genie, SQL, UC functions, AI Search, external
  // connections) or a manual URL entry.
  const [adding, setAdding] = useState(false);
  const [addTab, setAddTab] = useState<'databricks' | 'manual'>('databricks');
  const [form, setForm] = useState({ name: '', server_url: '', server_type: 'streamable', api_key: '' });
  const [saving, setSaving] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      if (isSystemAdmin) {
        // System admin needs BOTH the global catalog (availability) and the
        // workspace-effective set (per-workspace enabled) to drive two toggles.
        const [base, ws] = await Promise.all([svc().getBaseServers(), svc().getMcpServers()]);
        setBaseServers(base.servers ?? []);
        setWsServers(ws.servers ?? []);
      } else {
        const ws = await svc().getMcpServers();
        setBaseServers(null);
        setWsServers(ws.servers ?? []);
      }
    } catch (e) {
      setBaseServers([]);
      setWsServers([]);
      setError(e instanceof Error ? e.message : 'Could not load MCP servers');
    } finally {
      setLoading(false);
    }
  }, [isSystemAdmin]);

  useEffect(() => {
    if (!open) return;
    setAdding(false);
    void load();
  }, [open, load]);

  // Merge global + workspace-effective entries into one row per server (by name).
  const rows = useMemo<ServerRow[]>(() => {
    const byName = new Map<string, ServerRow>();
    (baseServers ?? []).forEach((b) => {
      byName.set(b.name.toLowerCase(), {
        name: b.name,
        server_type: b.server_type,
        server_url: b.server_url,
        base: b,
        effective: null,
      });
    });
    (wsServers ?? []).forEach((w) => {
      const key = w.name.toLowerCase();
      const existing = byName.get(key);
      if (existing) existing.effective = w;
      else byName.set(key, { name: w.name, server_type: w.server_type, server_url: w.server_url, base: null, effective: w });
    });
    return [...byName.values()];
  }, [baseServers, wsServers]);

  if (!open) return null;

  const run = async (key: string, fn: () => Promise<unknown>) => {
    setBusy(key);
    setError(null);
    try {
      await fn();
      await load();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to update server');
    } finally {
      setBusy(null);
    }
  };

  const toggleGlobal = (row: ServerRow) => {
    if (!row.base) return;
    void run(`g:${row.name}`, () => svc().setGlobalAvailability(row.base!.id, !row.base!.enabled));
  };
  const toggleWorkspace = (row: ServerRow) => {
    if (!row.effective) return;
    void run(`w:${row.name}`, () => svc().setWorkspaceEnabled(row.effective!.id, !row.effective!.enabled));
  };
  const remove = (row: ServerRow) => {
    if (!row.base) return;
    if (!window.confirm(`Delete "${row.name}"? This removes it from all teamspaces.`)) return;
    void run(`d:${row.name}`, () => svc().deleteMcpServer(row.base!.id));
  };

  const addServer = async () => {
    const name = form.name.trim();
    const server_url = form.server_url.trim();
    if (!name || !server_url) return;
    setSaving(true);
    setError(null);
    try {
      await svc().createGlobalServer({
        name,
        server_url,
        server_type: form.server_type,
        api_key: form.api_key.trim(),
        auth_type: 'api_key',
        enabled: true,
        global_enabled: false,
        timeout_seconds: 30,
        max_retries: 3,
        rate_limit: 60,
      });
      setForm({ name: '', server_url: '', server_type: 'streamable', api_key: '' });
      setAdding(false);
      await load();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to register server');
    } finally {
      setSaving(false);
    }
  };

  const inputStyle: React.CSSProperties = {
    padding: '7px 10px',
    backgroundColor: 'var(--bg-input)',
    color: 'var(--text-primary)',
    border: '1px solid var(--border-color)',
    borderRadius: 8,
    fontSize: 13,
    width: '100%',
    outline: 'none',
  };

  return (
    <div
      className="fixed inset-0 z-[60] flex items-center justify-center p-4 animate-fade-in"
      style={{ backgroundColor: 'rgba(0,0,0,0.4)' }}
      onMouseDown={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div
        role="dialog"
        aria-label="Connect a tool"
        className="w-full max-w-lg rounded-2xl flex flex-col overflow-hidden"
        style={{
          backgroundColor: 'var(--bg-primary)',
          border: '1px solid var(--border-color)',
          boxShadow: 'var(--shadow-popover)',
          maxHeight: '85vh',
        }}
      >
        {/* Header */}
        <div className="flex items-center gap-3 px-5 py-4" style={{ borderBottom: '1px solid var(--border-color)' }}>
          <span
            className="flex items-center justify-center w-9 h-9 rounded-lg flex-shrink-0"
            style={{ backgroundColor: 'var(--bg-active-chip)', color: 'var(--text-secondary)' }}
          >
            <svg className="w-[18px] h-[18px]" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M3.75 13.5l10.5-11.25L12 10.5h8.25L9.75 21.75 12 13.5H3.75z" />
            </svg>
          </span>
          <div className="flex-1 min-w-0">
            <div className="text-base font-semibold" style={{ color: 'var(--text-primary)' }}>Connect a tool</div>
            <div className="text-xs" style={{ color: 'var(--text-muted)' }}>MCP servers Kasal can equip agents with</div>
          </div>
          <button
            type="button"
            onClick={onClose}
            aria-label="Close"
            className="w-8 h-8 rounded-lg flex items-center justify-center transition-colors hover:bg-[var(--bg-rail-hover)]"
            style={{ color: 'var(--text-muted)' }}
          >
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        <div className="px-5 pt-4 overflow-y-auto">
          {/* Two-step hint (system admins only) */}
          {isSystemAdmin && (
            <div
              className="text-xs leading-relaxed rounded-lg mb-3"
              style={{ padding: '10px 12px', backgroundColor: 'var(--bg-secondary)', color: 'var(--text-secondary)', border: '1px solid var(--border-color)' }}
            >
              Make a server <strong style={{ color: 'var(--text-primary)' }}>Global</strong> (available to all teamspaces),
              then turn it on for <strong style={{ color: 'var(--text-primary)' }}>This teamspace</strong> — a server appears
              in chat only when both are on.
            </div>
          )}

          {/* Server list */}
          <div className="pb-2">
            {loading ? (
              <div className="py-8 text-center text-sm" style={{ color: 'var(--text-muted)' }}>Loading…</div>
            ) : rows.length === 0 ? (
              <div className="py-8 text-center text-sm" style={{ color: 'var(--text-muted)' }}>
                {isSystemAdmin ? 'No servers registered yet.' : 'No servers available for this teamspace yet.'}
              </div>
            ) : (
              rows.map((row) => {
                const rowBusy = busy === `g:${row.name}` || busy === `w:${row.name}` || busy === `d:${row.name}`;
                // Workspace toggle only makes sense once a server is globally
                // available (system-admin view); otherwise there's no effective row.
                const wsDisabled = rowBusy || (isSystemAdmin && !row.effective);
                return (
                  <div
                    key={row.name}
                    className="flex items-center gap-2 rounded-xl mb-2"
                    style={{ padding: '10px 10px', backgroundColor: 'var(--bg-secondary)', border: '1px solid var(--border-color)' }}
                  >
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2">
                        <span className="text-sm font-medium truncate" style={{ color: 'var(--text-primary)' }}>{row.name}</span>
                        {row.server_type && (
                          <span
                            className="text-[10px] uppercase tracking-wide flex-shrink-0 rounded"
                            style={{ padding: '1px 6px', color: 'var(--text-muted)', border: '1px solid var(--border-color)' }}
                          >
                            {row.server_type}
                          </span>
                        )}
                      </div>
                      {row.server_url && (
                        <div className="text-[11px] truncate mt-0.5" style={{ color: 'var(--text-muted)' }} title={row.server_url}>
                          {row.server_url}
                        </div>
                      )}
                    </div>

                    {isSystemAdmin ? (
                      <>
                        {row.base && (
                          <LabeledToggle
                            caption="Global"
                            checked={row.base.enabled}
                            disabled={rowBusy}
                            ariaLabel={`Global availability: ${row.name}`}
                            onChange={() => toggleGlobal(row)}
                          />
                        )}
                        <LabeledToggle
                          caption="Teamspace"
                          checked={Boolean(row.effective?.enabled)}
                          disabled={wsDisabled}
                          ariaLabel={`Enabled for this teamspace: ${row.name}`}
                          onChange={() => toggleWorkspace(row)}
                        />
                        {row.base && (
                          <button
                            type="button"
                            onClick={() => remove(row)}
                            disabled={rowBusy}
                            aria-label={`Delete ${row.name}`}
                            className="w-8 h-8 rounded-lg flex items-center justify-center flex-shrink-0 transition-colors hover:bg-[var(--bg-rail-hover)] disabled:opacity-50"
                            style={{ color: 'var(--text-muted)' }}
                          >
                            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
                              <path strokeLinecap="round" strokeLinejoin="round" d="M14.74 9l-.346 9m-4.788 0L9.26 9m9.968-3.21c.342.052.682.107 1.022.166M18.16 19.673a2.25 2.25 0 01-2.244 2.077H8.084a2.25 2.25 0 01-2.244-2.077L4.772 5.79m14.456 0a48.108 48.108 0 00-3.478-.397m-12 .562A48.11 48.11 0 015.25 5.75m0 0V4.874c0-1.18.91-2.164 2.09-2.201a51.964 51.964 0 013.32 0c1.18.037 2.09 1.022 2.09 2.201v.916" />
                            </svg>
                          </button>
                        )}
                      </>
                    ) : (
                      <Toggle
                        checked={Boolean(row.effective?.enabled)}
                        disabled={rowBusy}
                        label={`Enabled for this teamspace: ${row.name}`}
                        onChange={() => toggleWorkspace(row)}
                      />
                    )}
                  </div>
                );
              })
            )}

            {error && <div className="text-xs mt-1" style={{ color: 'var(--accent)' }}>{error}</div>}
          </div>

          {/* Add server (register) — system admins only. Two sources: the
              Databricks catalog (Genie/SQL/UC functions/AI Search/external) or a
              manual URL. */}
          {isSystemAdmin && (
            adding ? (
              <div className="rounded-xl mb-3" style={{ padding: 12, backgroundColor: 'var(--bg-secondary)', border: '1px solid var(--border-color)' }}>
                {/* Source tabs + close */}
                <div className="flex items-center gap-1 mb-3">
                  {(['databricks', 'manual'] as const).map((k) => (
                    <button
                      key={k}
                      type="button"
                      onClick={() => { setAddTab(k); setError(null); }}
                      className="text-xs font-medium rounded-lg transition-colors"
                      style={{
                        padding: '5px 10px',
                        color: addTab === k ? 'var(--text-primary)' : 'var(--text-muted)',
                        backgroundColor: addTab === k ? 'var(--bg-active-chip)' : 'transparent',
                      }}
                    >
                      {k === 'databricks' ? 'Databricks catalog' : 'Manual'}
                    </button>
                  ))}
                  <div className="flex-1" />
                  <button
                    type="button"
                    onClick={() => { setAdding(false); setError(null); }}
                    aria-label="Done adding"
                    className="w-7 h-7 rounded-lg flex items-center justify-center transition-colors hover:bg-[var(--bg-rail-hover)]"
                    style={{ color: 'var(--text-muted)' }}
                  >
                    <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                      <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
                    </svg>
                  </button>
                </div>

                {addTab === 'databricks' ? (
                  <div className="max-h-[40vh] overflow-y-auto">
                    <ChatMcpCatalog scope="global" onRegistered={load} />
                  </div>
                ) : (
                  <div className="flex flex-col gap-2">
                    <input style={inputStyle} placeholder="Name (e.g. my-mcp)" value={form.name} onChange={(e) => setForm((f) => ({ ...f, name: e.target.value }))} />
                    <input style={inputStyle} placeholder="Server URL (https://…/mcp)" value={form.server_url} onChange={(e) => setForm((f) => ({ ...f, server_url: e.target.value }))} />
                    <div className="flex gap-2">
                      <select style={{ ...inputStyle, width: 140 }} value={form.server_type} onChange={(e) => setForm((f) => ({ ...f, server_type: e.target.value }))}>
                        <option value="streamable">Streamable</option>
                        <option value="sse">SSE</option>
                      </select>
                      <input style={inputStyle} placeholder="API key (optional)" value={form.api_key} onChange={(e) => setForm((f) => ({ ...f, api_key: e.target.value }))} />
                    </div>
                    <div className="flex justify-end mt-1">
                      <button type="button" onClick={addServer} disabled={saving || !form.name.trim() || !form.server_url.trim()} className="text-xs font-medium rounded-lg transition-colors hover:bg-[var(--bg-rail-hover)] disabled:opacity-50" style={{ padding: '7px 12px', backgroundColor: 'var(--bg-primary)', color: 'var(--text-primary)', border: '1px solid var(--border-color)' }}>
                        {saving ? 'Registering…' : 'Register server'}
                      </button>
                    </div>
                  </div>
                )}
              </div>
            ) : (
              <button type="button" onClick={() => { setAddTab('databricks'); setAdding(true); }} className="w-full flex items-center justify-center gap-1.5 text-xs font-medium rounded-xl mb-3 transition-colors hover:bg-[var(--bg-rail-hover)]" style={{ padding: '10px', color: 'var(--text-secondary)', border: '1px dashed var(--border-color)' }}>
                <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M12 4.5v15m7.5-7.5h-15" />
                </svg>
                Add server
              </button>
            )
          )}
        </div>
      </div>
    </div>
  );
};

export default ChatMcpDialog;
