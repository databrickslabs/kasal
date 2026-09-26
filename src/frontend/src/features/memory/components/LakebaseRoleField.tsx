import React from 'react';
import { TextField } from '@mui/material';

/** What Kasal assumes when no role is set (the owner of Databricks-created objects). */
const DEFAULT_LAKEBASE_DB_ROLE = 'databricks_superuser';
const SAFE_ROLE = /^[A-Za-z_][A-Za-z0-9_]*$/;

interface Props {
  value: string | undefined;
  onChange: (value: string | undefined) => void;
}

/**
 * The role knowledge sessions assume on Lakebase (SET ROLE), stored as
 * `lakebase_config.db_role`. It replaced the LAKEBASE_KNOWLEDGE_ROLE env var.
 */
const LakebaseRoleField: React.FC<Props> = ({ value, onChange }) => {
  const invalid = !!value && !SAFE_ROLE.test(value);
  return (
    <TextField
      fullWidth
      size="small"
      sx={{ mt: 2 }}
      label="Database role"
      value={value ?? ''}
      placeholder={DEFAULT_LAKEBASE_DB_ROLE}
      onChange={(e) => onChange(e.target.value.trim() || undefined)}
      error={invalid}
      helperText={
        invalid
          ? 'Letters, digits and underscores only.'
          : `Role assumed for knowledge search on this instance. Empty uses ${DEFAULT_LAKEBASE_DB_ROLE}.`
      }
      inputProps={{ 'aria-label': 'Database role' }}
    />
  );
};

export default LakebaseRoleField;
