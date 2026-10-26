import React, { useEffect, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  TextField,
  Typography,
} from '@mui/material';

/**
 * One dialog for every Unity Catalog sync action — publish one skill, publish
 * all, or pull all — because they ask the user the SAME thing: which
 * catalog.schema. The caller supplies the title, the confirm label, and the
 * action (`onConfirm`); this owns the catalog/schema inputs, the busy state, and
 * surfacing the backend's error verbatim.
 *
 * catalog.schema defaults to what's configured in the Databricks section (the
 * dialog only opens when that's set), and a prior explicit choice is remembered
 * so a run of publishes doesn't retype it.
 */
const CATALOG_KEY = 'kasal.uc.skills.catalog';
const SCHEMA_KEY = 'kasal.uc.skills.schema';

const read = (key: string): string => {
  try {
    return localStorage.getItem(key) ?? '';
  } catch {
    return '';
  }
};

const SkillPublishDialog: React.FC<{
  open: boolean;
  title: string;
  /** For a single skill, its name — shown as the resolved `cat.sch.name`. Omit
   *  for batch actions (there's no single target name). */
  fqnName?: string;
  confirmLabel?: string;
  busyLabel?: string;
  defaultCatalog: string;
  defaultSchema: string;
  onClose: () => void;
  /** Runs the sync; throws to surface an error in the dialog, resolves to close it. */
  onConfirm: (catalog: string, schema: string) => Promise<void>;
}> = ({
  open,
  title,
  fqnName,
  confirmLabel = 'Publish',
  busyLabel = 'Publishing…',
  defaultCatalog,
  defaultSchema,
  onClose,
  onConfirm,
}) => {
  const [catalog, setCatalog] = useState('');
  const [schema, setSchema] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (open) {
      setCatalog(read(CATALOG_KEY) || defaultCatalog);
      setSchema(read(SCHEMA_KEY) || defaultSchema);
      setError(null);
    }
  }, [open, defaultCatalog, defaultSchema]);

  const canConfirm = Boolean(catalog.trim() && schema.trim() && !busy);

  const handleConfirm = async () => {
    const cat = catalog.trim();
    const sch = schema.trim();
    setBusy(true);
    setError(null);
    try {
      await onConfirm(cat, sch);
      try {
        localStorage.setItem(CATALOG_KEY, cat);
        localStorage.setItem(SCHEMA_KEY, sch);
      } catch {
        /* a private window can refuse storage — not worth failing the action */
      }
      onClose();
    } catch (err) {
      const detail = (err as { response?: { data?: { detail?: string } } })?.response
        ?.data?.detail;
      setError(detail ?? (err instanceof Error ? err.message : 'Sync failed.'));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Dialog open={open} onClose={busy ? undefined : onClose} maxWidth="sm" fullWidth>
      <DialogTitle>{title}</DialogTitle>
      <DialogContent>
        <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
          Governed by Unity Catalog. Syncing again updates in place.
        </Typography>
        <Box sx={{ display: 'flex', gap: 2 }}>
          <TextField
            label="Catalog"
            value={catalog}
            onChange={(e) => setCatalog(e.target.value)}
            fullWidth
            size="small"
            autoFocus
            disabled={busy}
          />
          <TextField
            label="Schema"
            value={schema}
            onChange={(e) => setSchema(e.target.value)}
            fullWidth
            size="small"
            disabled={busy}
          />
        </Box>
        {fqnName && catalog.trim() && schema.trim() && (
          <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: 'block' }}>
            Target:{' '}
            <code>
              {catalog.trim()}.{schema.trim()}.{fqnName}
            </code>
          </Typography>
        )}
        {error && (
          <Alert severity="error" sx={{ mt: 2 }}>
            {error}
          </Alert>
        )}
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose} disabled={busy}>
          Cancel
        </Button>
        <Button
          onClick={() => void handleConfirm()}
          variant="contained"
          disabled={!canConfirm}
          startIcon={busy ? <CircularProgress size={16} /> : undefined}
        >
          {busy ? busyLabel : confirmLabel}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default SkillPublishDialog;
