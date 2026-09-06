import { useEffect, useState } from 'react';
import { Alert, Box, Button, FormControl, InputLabel, MenuItem, Select, Stack, Typography } from '@mui/material';
import { useTranslation } from 'react-i18next';
import { LanguageService } from '../../../api/config/LanguageService';
import { LANGUAGES } from '../../../config/i18n/config';
import { useThemeStore } from '../../../store/theme';
import ThemeModeIcon from '../../../components/ThemeModeIcon';

export default function GeneralSettings() {
  const { t } = useTranslation();
  const [language, setLanguage] = useState('');
  const [savedLanguage, setSavedLanguage] = useState('');
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');
  const [saved, setSaved] = useState(false);
  const dark = useThemeStore(state => state.isDarkMode);
  useEffect(() => {
    let active = true;
    LanguageService.getInstance().getCurrentLanguage().then(value => {
      if (active) { setLanguage(value); setSavedLanguage(value); }
    }).catch(() => { if (active) setError('Could not load your language preference.'); });
    return () => { active = false; };
  }, []);
  const save = async () => {
    setSaving(true); setError(''); setSaved(false);
    try { await LanguageService.getInstance().setLanguage(language); setSavedLanguage(language); setSaved(true); }
    catch (err) { setError(err instanceof Error ? err.message : 'Could not save your language preference.'); }
    finally { setSaving(false); }
  };
  const dirty = language !== savedLanguage;
  return <Stack spacing={4} sx={{ maxWidth: 760 }}>
    {error && <Alert severity="error">{error}</Alert>}
    <Box sx={{ display: 'flex', gap: 3, alignItems: { sm: 'center' }, flexDirection: { xs: 'column', sm: 'row' } }}>
      <Box sx={{ flex: 1 }}><Typography fontWeight={600}>{t('configuration.language.title', { defaultValue: 'Language' })}</Typography><Typography variant="body2" color="text.secondary">Choose the language used throughout Kasal.</Typography></Box>
      <FormControl size="small" sx={{ width: { xs: '100%', sm: 220 } }} disabled={!savedLanguage || saving}>
        <InputLabel id="settings-language-label">Language</InputLabel>
        <Select labelId="settings-language-label" label="Language" value={language} onChange={event => { setLanguage(event.target.value); setSaved(false); }}>
          {Object.entries(LANGUAGES).map(([code, { nativeName }]) => <MenuItem key={code} value={code}>{nativeName}</MenuItem>)}
        </Select>
      </FormControl>
    </Box>
    <Box sx={{ display: 'flex', gap: 3, alignItems: { sm: 'center' }, flexDirection: { xs: 'column', sm: 'row' } }}>
      <Box sx={{ flex: 1 }}><Typography fontWeight={600}>Appearance</Typography><Typography variant="body2" color="text.secondary">Use the same light or dark appearance across Kasal.</Typography></Box>
      <Button color="inherit" startIcon={<ThemeModeIcon dark={dark} />} onClick={() => { void useThemeStore.getState().toggleTheme(); }} sx={{ minWidth: 220, justifyContent: 'flex-start', bgcolor: 'action.hover' }}>Switch to {dark ? 'light' : 'dark'} mode</Button>
    </Box>
    {(dirty || saved) && <Stack direction="row" spacing={1} alignItems="center" justifyContent="flex-end">
      {saved && !dirty && <Typography role="status" variant="body2" color="text.secondary">Changes saved</Typography>}
      {dirty && <><Button color="inherit" disabled={saving} onClick={() => setLanguage(savedLanguage)}>Cancel</Button><Button variant="contained" disableElevation disabled={saving} onClick={save}>{saving ? 'Saving…' : 'Save changes'}</Button></>}
    </Stack>}
  </Stack>;
}
