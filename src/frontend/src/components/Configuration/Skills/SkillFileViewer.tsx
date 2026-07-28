import React, { useEffect, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Typography,
} from '@mui/material';
import { SkillService } from '../../../api/tools/SkillService';

interface Props {
  open: boolean;
  skillId: number | null;
  skillName: string;
  path: string;
  onClose: () => void;
}

/**
 * Read one file bundled with a skill.
 *
 * These are tier 3 of progressive disclosure: the agent reads them only when a
 * skill's instructions say to. A human needs the same access for a different
 * reason — a reference the skill body points at, with no way to open it, reads
 * as a broken link and makes the whole skill look unfinished.
 *
 * Content is fetched when the dialog opens, not with the skill list, for the
 * same reason the agent does not get it up front.
 */
const SkillFileViewer: React.FC<Props> = ({
  open,
  skillId,
  skillName,
  path,
  onClose,
}) => {
  const [content, setContent] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!open || skillId == null || !path) return;
    let cancelled = false;
    setLoading(true);
    setError(null);
    SkillService.readFile(skillId, path)
      .then((file) => {
        if (!cancelled) setContent(file.content);
      })
      .catch((err) => {
        if (cancelled) return;
        const detail =
          (err as { response?: { data?: { detail?: string } } })?.response?.data
            ?.detail;
        setError(detail ?? 'Could not read that file.');
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [open, skillId, path]);

  return (
    <Dialog open={open} onClose={onClose} maxWidth="md" fullWidth>
      <DialogTitle sx={{ pb: 0 }}>
        {path}
        <Typography variant="caption" color="text.secondary" display="block">
          Bundled with {skillName}. The agent reads this only when the skill&apos;s
          instructions point at it.
        </Typography>
      </DialogTitle>
      <DialogContent>
        {loading ? (
          <Box sx={{ display: 'flex', justifyContent: 'center', py: 4 }}>
            <CircularProgress size={28} />
          </Box>
        ) : error ? (
          <Alert severity="error">{error}</Alert>
        ) : (
          <Box
            component="pre"
            sx={{
              mt: 1,
              p: 2,
              borderRadius: 1,
              bgcolor: 'action.hover',
              fontFamily: 'monospace',
              fontSize: 13,
              // Markdown, shown as written rather than rendered: what matters
              // here is exactly what the agent will read.
              whiteSpace: 'pre-wrap',
              wordBreak: 'break-word',
              maxHeight: '60vh',
              overflow: 'auto',
            }}
          >
            {content}
          </Box>
        )}
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Close</Button>
      </DialogActions>
    </Dialog>
  );
};

export default SkillFileViewer;
