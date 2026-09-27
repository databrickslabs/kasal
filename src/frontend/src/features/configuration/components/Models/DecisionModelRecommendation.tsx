import React, { useState } from 'react';
import { Alert, Button, Stack, TextField } from '@mui/material';
import { useTranslation } from 'react-i18next';
import { DecisionConfigService } from '../../../../api/config/DecisionConfigService';

const K = 'configuration.models.decisionModel';

/** A user-requested recommendation, never an automatic model change. */
export default function DecisionModelRecommendation() {
  const { t } = useTranslation();
  const [prompt, setPrompt] = useState('');
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState('');

  const recommend = async () => {
    setBusy(true);
    setResult('');
    try {
      const advice = await DecisionConfigService.recommend(prompt);
      setResult(advice.model || advice.effort
        ? `Suggested model: ${advice.model ?? 'no confident recommendation'}. Effort: ${advice.effort ?? 'keep current'}. Your current selections have not changed.`
        : 'No confident recommendation. Keep your current model and effort.');
    } catch {
      setResult('Could not get a recommendation. Keep your current model and effort.');
    } finally {
      setBusy(false);
    }
  };

  return <Stack spacing={1}>
    <TextField label="Task for model and effort advice" multiline value={prompt}
      disabled={busy} inputProps={{ maxLength: 12000 }}
      onChange={(event) => { setPrompt(event.target.value); setResult(''); }} />
    <Button disabled={busy || !prompt.trim()} onClick={() => void recommend()} sx={{ alignSelf: 'flex-start' }}>
      {t(`${K}.recommend`, { defaultValue: 'Ask the decision model for a recommendation' })}
    </Button>
    {result && <Alert severity="info">{result}</Alert>}
  </Stack>;
}
