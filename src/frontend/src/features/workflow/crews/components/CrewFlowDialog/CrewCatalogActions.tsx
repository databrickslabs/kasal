import React, { useState } from 'react';
import { Box, IconButton, Tooltip } from '@mui/material';
import AutoFixHighIcon from '@mui/icons-material/AutoFixHigh';
import DownloadIcon from '@mui/icons-material/Download';
import DeleteIcon from '@mui/icons-material/Delete';
import RocketLaunchIcon from '@mui/icons-material/RocketLaunch';
import type { CrewResponse } from '../../../../../types/workflow/crew';
import ExportCrewDialog from '../../../export/components/ExportCrewDialog';
import PublishButton from './PublishButton';

interface Props {
  crew: CrewResponse;
  canEdit: boolean;
  canDelete: boolean;
  mlflowEnabled: boolean;
  published: boolean;
  onPublished: (published: boolean) => void;
  onOptimize: () => void;
  onExport: (event: React.MouseEvent) => void;
  onDelete: (event: React.MouseEvent) => void;
}

export default function CrewCatalogActions({
  crew, canEdit, canDelete, mlflowEnabled, published, onPublished, onOptimize, onExport, onDelete,
}: Props) {
  const [deployOpen, setDeployOpen] = useState(false);
  return <Box
    onClick={event => event.stopPropagation()}
    sx={{ display: 'flex', justifyContent: 'flex-end', alignItems: 'center', flexWrap: 'wrap', gap: 0.25, mt: 1.5, pt: 1 }}
  >
    {canEdit && mlflowEnabled && (
      <Tooltip title="Optimize Prompts">
        <IconButton size="small" onClick={() => onOptimize()}>
          <AutoFixHighIcon fontSize="small" />
        </IconButton>
      </Tooltip>
    )}
    {canEdit && (
      <PublishButton
        entityType="crew"
        entityId={String(crew.id)}
        entityName={crew.name}
        nodes={crew.nodes}
        published={published}
        onChanged={onPublished}
      />
    )}
    {canEdit && (
      <Tooltip title="Deploy to Databricks Apps">
        <IconButton size="small" aria-label="Deploy to Databricks Apps" onClick={() => setDeployOpen(true)}>
          <RocketLaunchIcon fontSize="small" />
        </IconButton>
      </Tooltip>
    )}
    <Tooltip title="Export Crew">
      <IconButton size="small" onClick={onExport}>
        <DownloadIcon fontSize="small" />
      </IconButton>
    </Tooltip>
    {canDelete && (
      <Tooltip title="Delete Crew">
        <IconButton size="small" onClick={onDelete}>
          <DeleteIcon fontSize="small" />
        </IconButton>
      </Tooltip>
    )}
    {canEdit && deployOpen && <ExportCrewDialog
      open
      onClose={() => setDeployOpen(false)}
      crewId={String(crew.id)}
      crewName={crew.name}
    />}
  </Box>;
}
