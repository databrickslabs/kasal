/**
 * Task-form config sections for the tools that run the Power BI pipeline
 * extraction: Pipeline Config Generator (Tool 90) and UCMV Drift Monitor (Tool 98).
 *
 * Lives beside TaskForm (which is over the file-size ceiling) so new tool
 * sections land here instead of growing it.
 */
import React from 'react';
import { Box, Typography } from '@mui/material';
import {
  PipelineConfigGeneratorConfigSelector,
  type PipelineConfigGeneratorConfig,
} from '../../../tools/components/configuration/PipelineConfigGeneratorConfigSelector';
import {
  UCMVDriftMonitorConfigSelector,
  type UCMVDriftMonitorConfig,
} from '../../../tools/components/configuration/UCMVDriftMonitorConfigSelector';

interface Props {
  isToolSelected: (title: string) => boolean;
  toolConfigs: Record<string, unknown>;
  setToolConfig: (key: string, config: unknown) => void;
}

const Section: React.FC<{ title: string; children: React.ReactNode }> = ({ title, children }) => (
  <Box sx={{ mt: 2 }}>
    <Typography variant="subtitle2" sx={{ mb: 1, fontWeight: 600 }}>
      {title}
    </Typography>
    <Box
      sx={{
        p: 2,
        backgroundColor: 'rgba(25, 118, 210, 0.04)',
        borderRadius: 1,
        border: '1px solid rgba(25, 118, 210, 0.2)',
      }}
    >
      {children}
    </Box>
  </Box>
);

export const BIExtractionToolConfigSections: React.FC<Props> = ({
  isToolSelected,
  toolConfigs,
  setToolConfig,
}) => (
  <>
    {isToolSelected('Pipeline Config Generator') && (
      <Section title="Pipeline Config Generator (Tool 90) Configuration">
        <PipelineConfigGeneratorConfigSelector
          value={(toolConfigs['Pipeline Config Generator'] || {}) as PipelineConfigGeneratorConfig}
          onChange={(config) => setToolConfig('Pipeline Config Generator', config)}
        />
      </Section>
    )}
    {isToolSelected('UCMV Drift Monitor') && (
      <Section title="UCMV Drift Monitor (Tool 98) Configuration">
        <UCMVDriftMonitorConfigSelector
          value={(toolConfigs['UCMV Drift Monitor'] || {}) as UCMVDriftMonitorConfig}
          onChange={(config) => setToolConfig('UCMV Drift Monitor', config)}
        />
      </Section>
    )}
  </>
);
