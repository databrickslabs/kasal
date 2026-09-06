import React from 'react';
import { Badge, IconButton, Tooltip } from '@mui/material';
import HelpOutlineIcon from '@mui/icons-material/HelpOutline';
import { useWorkflowStore } from '../../../store/workflow';

export default function TutorialButton({ onClick }: { onClick: () => void }) {
  const hasSeenTutorial = useWorkflowStore(state => state.hasSeenTutorial);
  return <Tooltip title="Start Tutorial / Help" placement="left">
    <IconButton aria-label="Start Tutorial / Help" data-tour="help-button" onClick={onClick} sx={{ width: 40, height: 40, borderRadius: 2, color: 'text.secondary' }}>
      <Badge variant="dot" color="primary" invisible={hasSeenTutorial}><HelpOutlineIcon sx={{ fontSize: 20 }} /></Badge>
    </IconButton>
  </Tooltip>;
}
