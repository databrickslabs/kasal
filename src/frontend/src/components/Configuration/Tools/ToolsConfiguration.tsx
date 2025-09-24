
import { useEffect, useMemo, useState } from 'react';
import {
  Box,
  Typography,
  Paper,
  Grid,
  Button,
  Chip,
  IconButton,
  Stack,
  Tooltip,
  CircularProgress,
  Switch,
  FormControlLabel,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  TextField,
  Alert,
} from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import DeleteIcon from '@mui/icons-material/Delete';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import ErrorOutlineIcon from '@mui/icons-material/ErrorOutline';
import RocketLaunchIcon from '@mui/icons-material/RocketLaunch';
import BuildIcon from '@mui/icons-material/Build';

import { usePermissionStore } from '../../../store/permissions';
import { GroupToolService, type GroupToolMapping } from '../../../api/GroupToolService';
import { ToolService, type Tool } from '../../../api/ToolService';
import { APIKeysService, type ApiKey } from '../../../api/APIKeysService';

function KeyReadinessChip({ ready, onClick }: { ready: boolean; onClick?: () => void }) {
  if (ready) {
    return <Chip size="small" color="success" icon={<CheckCircleIcon />} label="Validated" variant="outlined" />;
  }
  return (
    <Chip
      size="small"
      color="warning"
      icon={<ErrorOutlineIcon />}
      label="Missing API key"
      onClick={onClick}
      clickable
      variant="outlined"
    />
  );
}

export default function ToolsConfiguration({ mode = 'auto' }: { mode?: 'system' | 'workspace' | 'auto' }): JSX.Element {
  const { isSystemAdmin, userRole } = usePermissionStore((s) => ({
    isSystemAdmin: s.isSystemAdmin,
    userRole: s.userRole,
  }));
  const isWorkspaceAdmin = useMemo(() => isSystemAdmin || userRole === 'admin', [isSystemAdmin, userRole]);
  // Config dialog state
  const [configOpen, setConfigOpen] = useState(false);
  const [configText, setConfigText] = useState<string>('');
  const [configError, setConfigError] = useState<string | null>(null);
  const [selectedMapping, setSelectedMapping] = useState<GroupToolMapping | null>(null);

  const [selectedGlobalTool, setSelectedGlobalTool] = useState<Tool | null>(null);

  const openGlobalConfigure = (t: Tool) => {
    setSelectedGlobalTool(t);
    setConfigText(JSON.stringify(t.config ?? {}, null, 2));
    setConfigError(null);
    setConfigOpen(true);
  };

  const openConfigure = (m: GroupToolMapping) => {
    setSelectedMapping(m);
    setConfigText(JSON.stringify(m.config ?? {}, null, 2));
    setConfigError(null);
    setConfigOpen(true);
  };

  const closeConfigure = () => {
    setConfigOpen(false);
    setSelectedMapping(null);
    setSelectedGlobalTool(null);
    setConfigError(null);
  };

  const saveConfigure = async () => {
    try {
      const parsed = configText.trim() ? JSON.parse(configText) : {};
      if (selectedGlobalTool) {
        await ToolService.updateTool(selectedGlobalTool.id, { config: parsed });
        await loadGlobal();
      } else if (selectedMapping) {
        await GroupToolService.updateConfig(selectedMapping.tool_id, parsed);
        await loadWorkspaceData();
      } else {
        return;
      }
      closeConfigure();
    } catch (e: any) {
      setConfigError(e?.message ?? 'Invalid JSON');
    }
  };


  const [loading, setLoading] = useState(true);
  const [available, setAvailable] = useState<Tool[]>([]);
  const [added, setAdded] = useState<GroupToolMapping[]>([]);
  const [globalTools, setGlobalTools] = useState<Tool[]>([]);
  const [toolLookup, setToolLookup] = useState<Record<number, Tool>>({});
  const [apiKeys, setApiKeys] = useState<ApiKey[]>([]);

  const loadWorkspaceData = async () => {
    setLoading(true);
    try {
      const [a, m, keys] = await Promise.all([
        GroupToolService.listAvailable(),
        GroupToolService.listAdded(),
        APIKeysService.getInstance().getAPIKeys(),
      ]);
      setAvailable(a);
      setAdded(m);
      setApiKeys(keys);
      // Prime lookup with available tools
      setToolLookup((prev) => {
        const next = { ...prev };
        a.forEach((t) => { next[t.id] = t; });
        return next;
      });
    } finally {
      setLoading(false);
    }
  };

  const loadGlobal = async () => {
    setLoading(true);
    try {
      const list = await ToolService.listGlobal();
      setGlobalTools(list);
      // Prime lookup with global tools
      setToolLookup((prev) => {
        const next = { ...prev };
        list.forEach((t) => { next[t.id] = t; });
        return next;
      });
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (isSystemAdmin) {
      void loadGlobal();
    }
    if (isWorkspaceAdmin) {
      void loadWorkspaceData();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isSystemAdmin, isWorkspaceAdmin]);

  // Ensure we have titles for added tools; fetch any missing from backend
  useEffect(() => {
    const missingIds = added
      .map((m) => m.tool_id)
      .filter((id, idx, arr) => arr.indexOf(id) === idx) // unique
      .filter((id) => !toolLookup[id]);
    if (missingIds.length === 0) return;

    (async () => {
      const results = await Promise.all(missingIds.map((id) => ToolService.getTool(id)));
      setToolLookup((prev) => {
        const next = { ...prev };
        results.forEach((t) => {
          if (t) next[t.id] = t;
        });
        return next;
      });
    })();
  }, [added, toolLookup]);

  const handleAdd = async (toolId: number) => {
    await GroupToolService.addTool(toolId);
    await loadWorkspaceData();
  };

  const handleToggleEnabled = async (toolId: number, enabled: boolean) => {
    await GroupToolService.setEnabled(toolId, enabled);
    await loadWorkspaceData();
  };

  const handleRemove = async (toolId: number) => {
    await GroupToolService.remove(toolId);
    await loadWorkspaceData();
  };

  const handleGlobalToggle = async (toolId: number, enabled: boolean) => {
    await ToolService.setGlobalAvailability(toolId, enabled);
    await loadGlobal();
  };


  // Tool -> required API key mapping (workspace/local keystore keys)
  const REQUIRED_KEYS_BY_TOOL_ID: Record<number, string> = {
    4: 'COMPOSIO_API_KEY',
    10: 'EXA_API_KEY',
    12: 'FIRECRAWL_API_KEY',
    13: 'FIRECRAWL_API_KEY',
    14: 'FIRECRAWL_API_KEY',
    16: 'SERPER_API_KEY',
    31: 'PERPLEXITY_API_KEY',
    45: 'LINKUP_API_KEY'
  };

  const apiKeyMap = useMemo(() => {
    const map: Record<string, string> = {};
    (apiKeys || []).forEach(k => { map[k.name] = k.value; });
    return map;
  }, [apiKeys]);

  const getReadiness = (m: GroupToolMapping): { ready: boolean; keyName?: string } => {
    const keyName = REQUIRED_KEYS_BY_TOOL_ID[m.tool_id];
    if (!keyName) return { ready: true }; // no key required
    const val = apiKeyMap[keyName];
    const isSet = !!val && val !== 'Not set' && val.trim() !== '';
    return { ready: isSet, keyName };
  };

  const gotoLocalKeystore = (keyName?: string) => {
    try {
      window.dispatchEvent(new CustomEvent('kasal:navigate-config', { detail: { section: 'API Keys' } }));
      window.dispatchEvent(new CustomEvent('kasal:api-keys:set-tab', { detail: { tab: 'local' } }));
      if (keyName) {
        window.dispatchEvent(new CustomEvent('kasal:api-keys:focus-key', { detail: { name: keyName } }));
      }
    } catch (e) { /* no-op */ }
  };

  if (loading) {
    return (
      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'center', py: 6 }}>
        <CircularProgress size={24} />
        <Typography variant="body2" sx={{ ml: 1.5 }}>Loading tools…</Typography>
      </Box>
    );
  }

  return (
    <Stack spacing={2}>
      {isSystemAdmin && (mode === 'auto' || mode === 'system') && (
        <Paper variant="outlined" sx={{ p: 2 }}>
          <Stack direction="row" alignItems="center" spacing={1} mb={1}>
            <BuildIcon color="primary" fontSize="small" />
            <Typography variant="subtitle1">Global Tools (System Admin)</Typography>
          </Stack>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
            These are the list of tools that will be allowed to be used within Kasal.
          </Typography>

          <Grid container spacing={2}>
            {globalTools.map((t) => (
              <Grid item xs={12} md={6} lg={4} key={t.id}>
                <Paper variant="outlined" sx={{ p: 1.5 }}>
                  <Stack spacing={1}>
                    <Typography variant="subtitle2" noWrap>{t.title}</Typography>
                    <Stack direction="row" spacing={1} alignItems="center" sx={{ flexWrap: 'wrap' }}>
                      <Button size="small" onClick={() => openGlobalConfigure(t)}>Configure</Button>
                      <FormControlLabel
                        sx={{ m: 0 }}
                        control={
                          <Switch
                            size="small"
                            checked={!!t.enabled}
                            onChange={(e) => void handleGlobalToggle(t.id, e.target.checked)}
                            inputProps={{ 'aria-label': `toggle-global-${t.title}` }}
                          />
                        }
                        label="Published"
                      />
                    </Stack>
                  </Stack>
                </Paper>
              </Grid>
            ))}
            {globalTools.length === 0 && (
              <Grid item xs={12}><Typography variant="body2" color="text.secondary">No global tools defined.</Typography></Grid>
            )}
          </Grid>
        </Paper>
      )}

      {isWorkspaceAdmin && (mode === 'auto' || mode === 'workspace') && (
        <Paper variant="outlined" sx={{ p: 2 }}>
          <Stack direction="row" alignItems="center" spacing={1} mb={1}>
            <RocketLaunchIcon color="primary" fontSize="small" />
            <Typography variant="subtitle1">Workspace Tools</Typography>
          </Stack>

          <Grid container spacing={2}>
            <Grid item xs={12} md={6}>
              <Typography variant="subtitle2" sx={{ mb: 1 }}>Available to Add</Typography>
              <Stack spacing={1}>
                {available.map((t) => (
                  <Paper key={t.id} variant="outlined" sx={{ p: 1.5, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <Box>
                      <Typography variant="body2">{t.title}</Typography>
                    </Box>
                    <Button size="small" startIcon={<AddIcon />} onClick={() => void handleAdd(t.id)}>Add</Button>
                  </Paper>
                ))}
                {available.length === 0 && (
                  <Typography variant="body2" color="text.secondary">All globally published tools are already added.</Typography>
                )}
              </Stack>
            </Grid>

            <Grid item xs={12} md={6}>
              <Typography variant="subtitle2" sx={{ mb: 1 }}>Added in this Workspace</Typography>
              <Stack spacing={1}>
                {added.map((m) => {
                  const readiness = getReadiness(m);
                  return (
                    <Paper key={m.id} variant="outlined" sx={{ p: 1.5, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                      <Box>
                        <Typography variant="body2">{toolLookup[m.tool_id]?.title ?? `Tool #${m.tool_id}`}</Typography>
                        <Box sx={{ mt: 0.5 }}>
                          <KeyReadinessChip ready={readiness.ready} onClick={!readiness.ready ? () => gotoLocalKeystore(readiness.keyName) : undefined} />
                        </Box>
                        <Typography variant="caption" color="text.secondary">Enabled: {m.enabled ? 'Yes' : 'No'}</Typography>
                      </Box>
                      <Stack direction="row" spacing={1} alignItems="center">
                        <Button size="small" onClick={() => openConfigure(m)}>Configure</Button>
                        <Tooltip title={m.enabled ? 'Disable' : 'Enable'}>
                          <Switch
                            checked={!!m.enabled}
                            onChange={(e) => void handleToggleEnabled(m.tool_id, e.target.checked)}
                          />
                        </Tooltip>
                        <Tooltip title="Remove from workspace">
                          <IconButton size="small" onClick={() => void handleRemove(m.tool_id)}>
                            <DeleteIcon fontSize="small" />
                          </IconButton>
                        </Tooltip>
                      </Stack>
                    </Paper>
                  );
                })}
                {added.length === 0 && (
                  <Typography variant="body2" color="text.secondary">No tools added to this workspace yet.</Typography>
                )}
              </Stack>
            </Grid>
          </Grid>
        </Paper>
      )}

      {!isSystemAdmin && !isWorkspaceAdmin && (
        <Typography variant="body2" color="text.secondary">You don’t have permissions to manage tools.</Typography>
      )}
      {/* Config Dialog */}
      <Dialog open={configOpen} onClose={closeConfigure} maxWidth="md" fullWidth>
        <DialogTitle>
          Configure Tool {selectedGlobalTool ? `#${selectedGlobalTool.id} (${selectedGlobalTool.title})` : selectedMapping ? `#${selectedMapping.tool_id}` : ''}
        </DialogTitle>
        <DialogContent>
          {configError && (
            <Box sx={{ mb: 1 }}>
              <Alert severity="error">{configError}</Alert>
            </Box>
          )}
          <TextField
            label="Workspace Configuration (JSON)"
            value={configText}
            onChange={(e) => setConfigText(e.target.value)}
            multiline
            minRows={10}
            fullWidth
            placeholder={'{\n  "apiKey": "..." \n}'}
            sx={{ mt: 2 }}
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={closeConfigure}>Cancel</Button>
          <Button variant="contained" onClick={() => void saveConfigure()}>Save</Button>
        </DialogActions>
      </Dialog>
    </Stack>
  );
}

