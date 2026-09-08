import { Alert, Box, MenuItem, TextField, Typography } from '@mui/material';

const options = {
  use: { label: 'Use existing data', description: 'Connect to an instance that already has the Kasal schema and data.' },
  use_expand: { label: 'Use and expand schema', description: 'Keep existing data and add any missing tables or columns.' },
  recreate: { label: 'Migrate schema and data', description: 'Replaces the existing Lakebase schema and copies data from the current database. Existing Lakebase data will be deleted.' },
  schema_only: { label: 'Create an empty schema', description: 'Replaces the existing Lakebase schema with an empty one. Existing Lakebase data will be deleted; no data is copied.' },
};

type SetupOption = keyof typeof options;

export default function LakebaseSetupOptions({ value, onChange }: {
  value: SetupOption;
  onChange: (value: SetupOption) => void;
}) {
  const replacesData = value === 'recreate' || value === 'schema_only';
  return <Box>
    <TextField select fullWidth label="Setup option" value={value} onChange={event => onChange(event.target.value as SetupOption)}>
      {Object.entries(options).map(([id, option]) => <MenuItem key={id} value={id}>{option.label}</MenuItem>)}
    </TextField>
    {replacesData
      ? <Alert severity="warning" sx={{ mt: 1 }}>{options[value].description}</Alert>
      : <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 1 }}>{options[value].description}</Typography>}
  </Box>;
}
