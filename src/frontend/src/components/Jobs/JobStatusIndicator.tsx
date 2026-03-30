import React, { useEffect } from 'react';
import { Dialog, CircularProgress, Typography } from '@mui/material';
import { useRunStatusStore } from '../../store/runStatus';
import { JobStatusIndicatorProps } from '../../types/common';

const JobStatusIndicator: React.FC<JobStatusIndicatorProps> = ({ open, jobId, onClose }) => {
  const { activeRuns } = useRunStatusStore();
  const run = jobId ? activeRuns[jobId] : null;

  useEffect(() => {
    if (run && ['completed', 'failed'].includes(run.status)) {
      setTimeout(onClose, 2000);
    }
  }, [run, onClose]);

  if (!run) return null;

  return (
    <Dialog open={open} onClose={onClose}>
      <div style={{ padding: '20px', textAlign: 'center' }}>
        {run.error ? (
          <Typography color="error">{run.error}</Typography>
        ) : (
          <>
            <CircularProgress size={24} style={{ marginRight: '10px' }} />
            <Typography>
              Job Status: {run.status}
            </Typography>
          </>
        )}
      </div>
    </Dialog>
  );
};

export default JobStatusIndicator; 