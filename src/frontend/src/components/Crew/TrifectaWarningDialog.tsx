/**
 * TrifectaWarningDialog — Pre-flight security warning shown when a crew
 * satisfies the "lethal trifecta" condition before execution.
 *
 * The lethal trifecta (per Databricks AI Security, Feb 2026):
 *   1. Reads sensitive internal data
 *   2. Ingests untrusted external content
 *   3. Communicates externally
 *
 * This combination enables indirect prompt injection attacks that can
 * exfiltrate internal data to attacker-controlled endpoints.
 */

import React from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Typography,
  Box,
  Chip,
  Alert,
  Divider,
} from '@mui/material';
import WarningAmberIcon from '@mui/icons-material/WarningAmber';
import SecurityIcon from '@mui/icons-material/Security';
import { TrifectaAssessment } from '../../utils/toolCapabilityManifest';

interface TrifectaWarningDialogProps {
  open: boolean;
  assessment: TrifectaAssessment | null;
  onProceed: () => void;
  onCancel: () => void;
}

const TrifectaWarningDialog: React.FC<TrifectaWarningDialogProps> = ({
  open,
  assessment,
  onProceed,
  onCancel,
}) => {
  if (!assessment) return null;

  return (
    <Dialog
      open={open}
      onClose={onCancel}
      maxWidth="sm"
      fullWidth
      PaperProps={{
        sx: { border: '2px solid', borderColor: 'warning.main' }
      }}
    >
      <DialogTitle sx={{ display: 'flex', alignItems: 'center', gap: 1.5, pb: 1 }}>
        <WarningAmberIcon color="warning" sx={{ fontSize: 28 }} />
        <Box>
          <Typography variant="h6" component="span">
            Security Warning
          </Typography>
          <Typography variant="body2" color="text.secondary" display="block">
            Lethal Trifecta Detected
          </Typography>
        </Box>
      </DialogTitle>

      <DialogContent>
        <Alert
          severity="warning"
          icon={<SecurityIcon />}
          sx={{ mb: 2 }}
        >
          <Typography variant="body2" sx={{ fontWeight: 600, mb: 0.5 }}>
            This crew is at high risk of indirect prompt injection.
          </Typography>
          <Typography variant="caption">
            It combines tools that read internal data, ingest untrusted external content,
            and communicate externally — an attacker could use malicious web content to
            exfiltrate your sensitive data.
          </Typography>
        </Alert>

        <Typography variant="subtitle2" sx={{ mb: 1.5, fontWeight: 600 }}>
          Why this is flagged:
        </Typography>

        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1.5, mb: 2 }}>
          {/* Condition 1 */}
          <Box sx={{
            p: 1.5, borderRadius: 1,
            bgcolor: assessment.readsSensitive ? 'error.50' : 'action.hover',
            border: '1px solid',
            borderColor: assessment.readsSensitive ? 'error.light' : 'divider',
            opacity: assessment.readsSensitive ? 1 : 0.5
          }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.5 }}>
              <Chip
                label={assessment.readsSensitive ? '✓' : '✗'}
                size="small"
                color={assessment.readsSensitive ? 'error' : 'default'}
                sx={{ minWidth: 32, height: 20, fontSize: '0.7rem' }}
              />
              <Typography variant="body2" sx={{ fontWeight: 600 }}>
                Reads sensitive internal data
              </Typography>
            </Box>
            {assessment.sensitiveTools.length > 0 && (
              <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5, pl: 4.5 }}>
                {assessment.sensitiveTools.map(t => (
                  <Chip key={t} label={t} size="small" variant="outlined" color="error"
                    sx={{ height: 20, fontSize: '0.65rem' }} />
                ))}
              </Box>
            )}
          </Box>

          {/* Condition 2 */}
          <Box sx={{
            p: 1.5, borderRadius: 1,
            bgcolor: assessment.ingestsUntrusted ? 'error.50' : 'action.hover',
            border: '1px solid',
            borderColor: assessment.ingestsUntrusted ? 'error.light' : 'divider',
            opacity: assessment.ingestsUntrusted ? 1 : 0.5
          }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.5 }}>
              <Chip
                label={assessment.ingestsUntrusted ? '✓' : '✗'}
                size="small"
                color={assessment.ingestsUntrusted ? 'error' : 'default'}
                sx={{ minWidth: 32, height: 20, fontSize: '0.7rem' }}
              />
              <Typography variant="body2" sx={{ fontWeight: 600 }}>
                Ingests untrusted external content
              </Typography>
            </Box>
            {assessment.untrustedTools.length > 0 && (
              <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5, pl: 4.5 }}>
                {assessment.untrustedTools.map(t => (
                  <Chip key={t} label={t} size="small" variant="outlined" color="error"
                    sx={{ height: 20, fontSize: '0.65rem' }} />
                ))}
              </Box>
            )}
          </Box>

          {/* Condition 3 */}
          <Box sx={{
            p: 1.5, borderRadius: 1,
            bgcolor: assessment.communicatesExternally ? 'error.50' : 'action.hover',
            border: '1px solid',
            borderColor: assessment.communicatesExternally ? 'error.light' : 'divider',
            opacity: assessment.communicatesExternally ? 1 : 0.5
          }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.5 }}>
              <Chip
                label={assessment.communicatesExternally ? '✓' : '✗'}
                size="small"
                color={assessment.communicatesExternally ? 'error' : 'default'}
                sx={{ minWidth: 32, height: 20, fontSize: '0.7rem' }}
              />
              <Typography variant="body2" sx={{ fontWeight: 600 }}>
                Communicates externally
              </Typography>
            </Box>
            {assessment.externalTools.length > 0 && (
              <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5, pl: 4.5 }}>
                {assessment.externalTools.map(t => (
                  <Chip key={t} label={t} size="small" variant="outlined" color="error"
                    sx={{ height: 20, fontSize: '0.65rem' }} />
                ))}
              </Box>
            )}
          </Box>
        </Box>

        <Divider sx={{ mb: 2 }} />

        <Typography variant="caption" color="text.secondary">
          <strong>Recommendation:</strong> Separate untrusted-input tasks from
          internal-data tasks and add an LLM injection guardrail between them.
          You can still proceed — this warning is informational and does not block execution.
        </Typography>
      </DialogContent>

      <DialogActions sx={{ px: 3, pb: 2, gap: 1 }}>
        <Button onClick={onCancel} variant="outlined" color="inherit">
          Cancel
        </Button>
        <Button
          onClick={onProceed}
          variant="contained"
          color="warning"
          startIcon={<WarningAmberIcon />}
        >
          Proceed Anyway
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default TrifectaWarningDialog;
