import React from 'react';
import ModelConfiguration from './ModelConfiguration';
import DecisionModelConfiguration from './DecisionModelConfiguration';
import DecisionModelSystemSettings from './DecisionModelSystemSettings';

type ModelsMode = 'system' | 'workspace' | 'auto';

/**
 * The Models settings section. The decision model sits here at both levels:
 * the workspace opt-in under Workspace settings, the provider endpoint under
 * System administration.
 */
const ModelsSection: React.FC<{ mode?: ModelsMode }> = ({ mode = 'auto' }) => (
  <>
    <ModelConfiguration mode={mode} />
    {mode === 'system' ? <DecisionModelSystemSettings /> : <DecisionModelConfiguration />}
  </>
);

export default ModelsSection;
