import React, { useState } from 'react';
import { Alert, Button, CircularProgress } from '@mui/material';
import { useTranslation } from 'react-i18next';
import { MCPService } from '../../../../api/tools/MCPService';

/**
 * Offers the admin an explicit "migrate" for MCP registrations still on the
 * legacy `/api/2.0/mcp/external/` proxy URL.
 *
 * This used to happen automatically: loading the catalog POSTed the migration
 * whenever rows were pending, so merely viewing the page rewrote
 * registrations. A write is now something the admin chooses, sees the result
 * of, and can retry when it fails. Only admins reach this: the catalog it sits
 * in, and the endpoint it calls, are admin-only on the backend.
 */
interface LegacyMcpUrlNoticeProps {
  /** Pending registrations, from the catalog's `legacy_external_count`. */
  count: number;
  /** Called after a successful migration so the parent can reload servers. */
  onMigrated?: (migrated: number) => Promise<void> | void;
}

const LegacyMcpUrlNotice: React.FC<LegacyMcpUrlNoticeProps> = ({ count, onMigrated }) => {
  const { t } = useTranslation();
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [migrated, setMigrated] = useState<number | null>(null);
  const [closed, setClosed] = useState(false);

  // After a migration the parent's count is stale until it reloads the
  // catalog, so never fall back to the "pending" prompt once one succeeded.
  if (closed) return null;
  if (migrated !== null) {
    return (
      <Alert severity="success" sx={{ mb: 2 }} onClose={() => setClosed(true)}>
        {t('configuration.mcp.legacyMigrated', {
          defaultValue: 'Migrated {{count}} MCP registration(s) to the UC MCP Service URL.',
          count: migrated,
        })}
      </Alert>
    );
  }
  if (count <= 0) return null;

  const migrate = async () => {
    setBusy(true);
    setError(null);
    try {
      const done = await MCPService.getInstance().migrateLegacyExternalUrls();
      setMigrated(done);
      // A failed reload is the parent's to report; the migration succeeded.
      void Promise.resolve(onMigrated?.(done)).catch(() => undefined);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Could not migrate the registrations');
    } finally {
      setBusy(false);
    }
  };

  return (
    <Alert
      severity={error ? 'error' : 'warning'}
      sx={{ mb: 2 }}
      action={
        <Button color="inherit" size="small" onClick={migrate} disabled={busy}>
          {busy ? (
            <CircularProgress size={14} color="inherit" />
          ) : (
            t('configuration.mcp.legacyMigrate', { defaultValue: 'Migrate' })
          )}
        </Button>
      }
    >
      {error ??
        t('configuration.mcp.legacyPending', {
          defaultValue:
            '{{count}} MCP registration(s) still use the legacy external-MCP proxy URL. Migrate them to the UC MCP Service URL.',
          count,
        })}
    </Alert>
  );
};

export default LegacyMcpUrlNotice;
