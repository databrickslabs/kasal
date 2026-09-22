/**
 * BIArtifactsView — restores the downloadable, reviewable BI artifacts on the
 * results page.
 *
 * A completed BI-migration run persists a `{text, a2ui}` envelope, so the results
 * page renders the A2UI dashboard (stat tiles + PDF) and the structured viewers
 * (UCMVResultViewer / ConfigGenResultView) — which carry the YAML/SQL/JSON
 * downloads and the config review — never trigger. The real artifacts are in
 * conversion_history keyed by execution_id (reliable since the handoff fix), so
 * this fetches them by job_id and renders those viewers above the dashboard.
 *
 * Renders nothing when the run has no BI artifacts, so it is inert for every
 * other kind of run.
 */
import React, { useEffect, useMemo, useState } from 'react';
import { Box, Divider, Typography } from '@mui/material';
import { ConverterService } from '../../../api/tools/ConverterService';
import UCMVResultViewer, { type UCMVResult } from './UCMVResultViewer';
import ConfigGenResultView from './ConfigGenResultView';

interface BIArtifactsViewProps {
  jobId: string;
}

const BIArtifactsView: React.FC<BIArtifactsViewProps> = ({ jobId }) => {
  const [ucmv, setUcmv] = useState<UCMVResult | null>(null);
  const [config, setConfig] = useState<Record<string, unknown> | null>(null);

  useEffect(() => {
    if (!jobId) return;
    let cancelled = false;
    (async () => {
      try {
        const resp = await ConverterService.listHistory({ execution_id: jobId, limit: 20 });
        const rows = resp.history || [];
        // Newest UC Metric View record that actually carries YAML.
        const ucmvRec = rows
          .filter((r) => r.target_format === 'uc_metrics' && r.output_data?.yaml)
          .sort((a, b) => (a.created_at < b.created_at ? 1 : -1))[0];
        // Newest Pipeline Config record.
        const cfgRec = rows
          .filter((r) => r.target_format === 'pipeline_config')
          .sort((a, b) => (a.created_at < b.created_at ? 1 : -1))[0];
        if (cancelled) return;
        if (ucmvRec?.output_data?.yaml) {
          setUcmv({
            yaml: ucmvRec.output_data.yaml as Record<string, string>,
            sql: (ucmvRec.output_data.sql as Record<string, string>) ?? {},
            stats: (ucmvRec.output_data.stats as Record<string, never>) ?? {},
            untranslatable_items: ucmvRec.output_data.untranslatable_items,
            // Persisted alongside yaml/sql (uc_metric_view_generator_tool
            // output_data.pbi_ucmv_mapping) — carry it through so the
            // "Download Mapping" button renders at the builder/gate step, not
            // only when the viewer holds the live in-session tool result.
            pbi_ucmv_mapping: ucmvRec.output_data.pbi_ucmv_mapping as
              | Record<string, string>
              | undefined,
            // Live-connection flag (which semantic model/table to parse) —
            // persisted in output_data; carry it so the UI note shows at the gate.
            live_connections: ucmvRec.output_data.live_connections as
              | Record<string, { server: string; database: string; table: string }>
              | undefined,
          });
        }
        if (cfgRec) {
          setConfig({
            proposed_config: cfgRec.output_data?.proposed_config,
            // config-gen persists the extracted measures (with DAX) in input_data.
            measures_json: cfgRec.output_data?.measures_json ?? cfgRec.input_data?.measures,
            mquery_json: cfgRec.output_data?.mquery_json,
            relationships_json: cfgRec.output_data?.relationships_json,
            summary: cfgRec.output_summary,
          });
        }
      } catch {
        /* no BI artifacts / fetch failed — render nothing, leave the normal view */
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [jobId]);

  const hasConfig = useMemo(
    () => !!config && (config.proposed_config != null || config.measures_json != null),
    [config],
  );

  if (!ucmv && !hasConfig) return null;

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2, mb: 2 }}>
      <Typography variant="overline" color="text.secondary" sx={{ fontWeight: 700 }}>
        Downloadable artifacts
      </Typography>
      {hasConfig && config && <ConfigGenResultView cfg={config} jobId={jobId} />}
      {ucmv && hasConfig && <Divider flexItem />}
      {ucmv && <UCMVResultViewer result={ucmv} />}
      <Divider flexItem sx={{ mt: 1 }} />
    </Box>
  );
};

export default BIArtifactsView;
