import React, { useEffect, useState } from 'react';
import { Accordion, AccordionDetails, AccordionSummary, Alert, Typography } from '@mui/material';
import { ExpandMore as ExpandMoreIcon } from '@mui/icons-material';
import { EngineConfigService } from '../../../../api/config/EngineConfigService';
import type { EngineSettings, EngineSettingsPatch } from '../../../../types/config/engines';
import SystemSettingsFields from './SystemSettingsFields';
import { SETTING_GROUPS, type SettingGroupId } from './settingGroups';

/**
 * "Advanced (all workspaces)" for one section: the server-wide settings that
 * belong to it. Loads them itself and renders nothing unless the API lets the
 * viewer read them (system administrators), so a section can always mount it.
 */
const SystemSettingsPanel: React.FC<{ group: SettingGroupId; standalone?: boolean }> = ({ group, standalone = false }) => {
  const [settings, setSettings] = useState<EngineSettings | null>(null);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => {
    let active = true;
    EngineConfigService.getSettings()
      .then((loaded) => active && setSettings(loaded))
      .catch(() => {
        /* not a system administrator (403) or unavailable: show nothing */
      });
    return () => {
      active = false;
    };
  }, []);

  // A response without the registry (older server, partial mock) shows nothing
  // rather than taking the whole section down.
  if (!settings?.advanced || !settings.advanced_specs) return null;

  const patch = async (body: EngineSettingsPatch) => {
    setSaving(true);
    setError('');
    try {
      setSettings(await EngineConfigService.updateSettings(body));
    } catch (err) {
      const detail = (err as { response?: { data?: { detail?: string } } }).response?.data?.detail;
      setError(detail || 'Could not save the setting.');
    } finally {
      setSaving(false);
    }
  };

  return (
    <Accordion disableGutters variant="outlined" sx={{ mt: standalone ? 0 : 3 }} defaultExpanded={standalone}>
      <AccordionSummary expandIcon={<ExpandMoreIcon />}>
        <Typography variant="subtitle2">
          {standalone ? `${SETTING_GROUPS[group].title}: defaults for every workspace` : `Advanced: ${SETTING_GROUPS[group].title} (all workspaces)`}
        </Typography>
      </AccordionSummary>
      <AccordionDetails>
        <Typography variant="caption" color="text.secondary" sx={{ display: 'block' }}>
          {standalone
            ? 'A workspace can override these in its own Output design, except the on/off switch: off here turns rich answers off everywhere.'
            : 'System administrators only. Applies to every workspace, to runs started after saving.'}
        </Typography>
        {error && (
          <Alert severity="error" sx={{ mt: 1 }}>
            {error}
          </Alert>
        )}
        <SystemSettingsFields groups={[group]} settings={settings} saving={saving} onPatch={(b) => void patch(b)} titles={false} />
      </AccordionDetails>
    </Accordion>
  );
};

export default SystemSettingsPanel;
