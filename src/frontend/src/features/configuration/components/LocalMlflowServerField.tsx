import React, { useEffect, useState } from 'react';
import { Box, Button, TextField } from '@mui/material';

interface Props {
  value: string | null | undefined;
  saving: boolean;
  /** An empty string clears the local server. */
  onSave: (uri: string) => void;
}

/**
 * The local (OSS) MLflow server this workspace traces to. It replaced launching
 * the backend with MCP_SERVER_ENABLED=true and MLFLOW_TRACKING_URI=<server>.
 */
const LocalMlflowServerField: React.FC<Props> = ({ value, saving, onSave }) => {
  const [draft, setDraft] = useState(value ?? '');
  useEffect(() => setDraft(value ?? ''), [value]);
  const trimmed = draft.trim().replace(/\/+$/, '');
  const invalid = trimmed !== '' && !/^https?:\/\//.test(trimmed);

  return (
    <Box sx={{ display: 'flex', gap: 1, alignItems: 'flex-start', mb: 2 }}>
      <TextField
        size="small"
        label="Local MLflow server"
        placeholder="http://127.0.0.1:5555"
        value={draft}
        onChange={(e) => setDraft(e.target.value)}
        error={invalid}
        helperText={
          invalid
            ? 'Use an http:// or https:// URL.'
            : 'For development: traces, judges and prompt optimization use this server when no Databricks workspace is configured.'
        }
        inputProps={{ 'aria-label': 'Local MLflow server' }}
        sx={{ flex: 1, maxWidth: 460 }}
      />
      <Button
        size="small"
        variant="outlined"
        disabled={saving || invalid || trimmed === (value ?? '')}
        onClick={() => onSave(trimmed)}
        sx={{ mt: 0.5 }}
      >
        Save
      </Button>
      <Button size="small" disabled={saving || !value} onClick={() => onSave('')} sx={{ mt: 0.5 }}>
        Clear
      </Button>
    </Box>
  );
};

export default LocalMlflowServerField;
