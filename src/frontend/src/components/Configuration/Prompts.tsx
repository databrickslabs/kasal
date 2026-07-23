import React, { useState } from 'react';
import {
  Box,
  Dialog,
  DialogContent,
  DialogTitle,
  IconButton,
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import AutoFixHighIcon from '@mui/icons-material/AutoFixHigh';
import { useTranslation } from 'react-i18next';
import PromptConfiguration from './PromptConfiguration';
import PromptOptimization from './PromptOptimization';

/**
 * Prompts panel: the seeded template list is the single surface; each
 * optimizable template's row carries an Optimize action that opens the
 * GEPA optimization flow in a dialog scoped to that template.
 */
const Prompts: React.FC = () => {
  const { t } = useTranslation();
  const [optimizeTarget, setOptimizeTarget] = useState<string | null>(null);

  return (
    <Box>
      <PromptConfiguration onOptimize={(name) => setOptimizeTarget(name)} />

      <Dialog
        open={optimizeTarget !== null}
        onClose={() => setOptimizeTarget(null)}
        fullWidth
        maxWidth="lg"
      >
        <DialogTitle sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <AutoFixHighIcon fontSize="small" />
          {t('configuration.prompts.optimizeTitle', { defaultValue: 'Optimize' })}{' '}
          {optimizeTarget}
          <Box sx={{ flexGrow: 1 }} />
          <IconButton onClick={() => setOptimizeTarget(null)} size="small">
            <CloseIcon fontSize="small" />
          </IconButton>
        </DialogTitle>
        <DialogContent>
          {optimizeTarget && <PromptOptimization fixedTemplate={optimizeTarget} />}
        </DialogContent>
      </Dialog>
    </Box>
  );
};

export default Prompts;
