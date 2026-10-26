import React, { useState } from 'react';
import { Alert, Box, Button, TextField, Typography } from '@mui/material';
import { useTranslation } from 'react-i18next';
import { CrewLabelsState } from '../hooks/useCrewLabels';

interface CrewLabelsFieldsProps {
  labels: CrewLabelsState;
}

/**
 * The crew's labels: what its deliverable must contain. They enable the
 * built-in judges that compare against labels (Correctness). Review notes on
 * past answers are offered as suggestions and are only used once accepted.
 */
const CrewLabelsFields: React.FC<CrewLabelsFieldsProps> = ({ labels }) => {
  const { t } = useTranslation();
  const [editing, setEditing] = useState<{ suggestion: string; text: string } | null>(null);

  return (
    <Box data-testid="crew-labels" sx={{ display: 'flex', flexDirection: 'column', gap: 1, mt: 2 }}>
      <Typography variant="caption" color="text.secondary" sx={{ fontWeight: 600 }}>
        {t('optimize.labels.title', { defaultValue: 'Labels (optional)' })}
      </Typography>
      <TextField
        size="small"
        label={t('optimize.labels.facts', { defaultValue: 'Expected facts' })}
        helperText={t('optimize.labels.factsHint', {
          defaultValue: 'One point per line that every good deliverable must contain.',
        })}
        value={labels.facts}
        onChange={(event) => labels.setFacts(event.target.value)}
        multiline
        minRows={2}
        InputLabelProps={{ shrink: true }}
      />
      <TextField
        size="small"
        label={t('optimize.labels.answer', { defaultValue: 'Expected answer' })}
        value={labels.answer}
        onChange={(event) => labels.setAnswer(event.target.value)}
        multiline
        minRows={2}
        InputLabelProps={{ shrink: true }}
      />
      {labels.suggestions.map((suggestion) =>
        editing?.suggestion === suggestion ? (
          <Box key={suggestion} sx={{ display: 'flex', gap: 1, alignItems: 'flex-start' }}>
            <TextField
              size="small"
              fullWidth
              multiline
              autoFocus
              label={t('optimize.labels.editSuggestion', { defaultValue: 'Edit suggested fact' })}
              value={editing.text}
              onChange={(event) => setEditing({ suggestion, text: event.target.value })}
            />
            <Button
              size="small"
              onClick={() => {
                labels.accept(suggestion, editing.text);
                setEditing(null);
              }}
            >
              {t('optimize.labels.add', { defaultValue: 'Add' })}
            </Button>
          </Box>
        ) : (
          <Alert
            key={suggestion}
            severity="info"
            variant="outlined"
            action={
              <>
                <Button size="small" onClick={() => labels.accept(suggestion)}>
                  {t('optimize.labels.accept', { defaultValue: 'Accept' })}
                </Button>
                <Button size="small" onClick={() => setEditing({ suggestion, text: suggestion })}>
                  {t('optimize.labels.edit', { defaultValue: 'Edit' })}
                </Button>
              </>
            }
          >
            <Typography variant="caption" display="block" color="text.secondary">
              {t('optimize.labels.suggested', {
                defaultValue: 'Suggested from your review notes (not used until you accept it)',
              })}
            </Typography>
            <Typography variant="body2">{suggestion}</Typography>
          </Alert>
        ),
      )}
      {labels.skipped.map((reason) => (
        <Alert key={reason} severity="warning" variant="outlined">
          {reason}
        </Alert>
      ))}
    </Box>
  );
};

export default CrewLabelsFields;
