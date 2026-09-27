import React, { useState } from 'react';
import { Tab, Tabs } from '@mui/material';
import { useTranslation } from 'react-i18next';
import ModelConfiguration from './ModelConfiguration';
import DecisionModelConfiguration from './DecisionModelConfiguration';
import DecisionModelSystemSettings from './DecisionModelSystemSettings';

type ModelsMode = 'system' | 'workspace' | 'auto';
type ModelsTab = 'models' | 'decision';

/**
 * The Models settings section: the model list and the decision model, each on
 * its own tab so the decision model is not buried under a long model list.
 * The workspace opt-in shows under Workspace settings, the provider endpoint
 * under System administration.
 */
const ModelsSection: React.FC<{ mode?: ModelsMode }> = ({ mode = 'auto' }) => {
  const { t } = useTranslation();
  const [tab, setTab] = useState<ModelsTab>('models');

  return (
    <>
      <Tabs
        value={tab}
        onChange={(_e, value: ModelsTab) => setTab(value)}
        sx={{ mb: 2, borderBottom: 1, borderColor: 'divider' }}
      >
        <Tab label={t('configuration.models.tabs.models', { defaultValue: 'Models' })} value="models" />
        <Tab
          label={t('configuration.models.tabs.decisionModel', { defaultValue: 'Decision model' })}
          value="decision"
        />
      </Tabs>
      {tab === 'models' && <ModelConfiguration mode={mode} />}
      {tab === 'decision'
        && (mode === 'system' ? <DecisionModelSystemSettings /> : <DecisionModelConfiguration />)}
    </>
  );
};

export default ModelsSection;
