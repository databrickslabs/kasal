import React, { useState } from 'react';
import { Box, Button, FormHelperText, Popover, Typography } from '@mui/material';
import McpConnectionPicker from './McpConnectionPicker';

interface MCPServerSelectorProps {
  value: string | string[] | null;
  onChange: (value: string | string[] | null) => void;
  multiple?: boolean;
  label?: string;
  placeholder?: string;
  disabled?: boolean;
  required?: boolean;
  helperText?: string;
  error?: boolean;
  fullWidth?: boolean;
}

/** Agent and task forms share the same inline picker used by the composers. */
export const MCPServerSelector: React.FC<MCPServerSelectorProps> = ({
  value, onChange, multiple = true, label = 'MCP Servers', placeholder = 'Select MCP servers...',
  disabled = false, required = false, helperText, error = false, fullWidth = true,
}) => {
  const [anchor, setAnchor] = useState<HTMLElement | null>(null);
  const names = Array.isArray(value) ? value : value ? [value] : [];
  return <Box>
    <Typography variant="caption" color={error ? 'error' : 'text.secondary'}>{label}{required ? ' *' : ''}</Typography>
    <Button role="combobox" aria-label={label} aria-expanded={Boolean(anchor)} aria-haspopup="dialog"
      variant="outlined" fullWidth={fullWidth} disabled={disabled} onClick={(e) => setAnchor(e.currentTarget)}
      sx={{ justifyContent: 'flex-start', textTransform: 'none', color: error ? 'error.main' : 'text.primary' }}>
      {names.length ? names.join(', ') : placeholder}
    </Button>
    <Popover open={Boolean(anchor) && !disabled} anchorEl={anchor} onClose={() => setAnchor(null)}
      anchorOrigin={{ vertical: 'bottom', horizontal: 'left' }}
      slotProps={{ paper: { sx: { width: 380, maxWidth: 'calc(100vw - 24px)', maxHeight: 'min(520px, 70vh)', borderRadius: 2 } } }}>
      <McpConnectionPicker selectedNames={names} onChange={(next) => {
        onChange(multiple ? next : next.find((n) => !names.includes(n)) || null);
        if (!multiple) setAnchor(null);
      }} disabled={disabled} />
    </Popover>
    {helperText && <FormHelperText error={error}>{helperText}</FormHelperText>}
  </Box>;
};
export default MCPServerSelector;
