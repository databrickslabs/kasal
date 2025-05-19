import React from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogContentText,
  DialogActions,
  Button,
  TextField,
} from '@mui/material';
import { useTranslation } from 'react-i18next';

interface RunDialogsProps {
  deleteDialogOpen: boolean;
  deleteLoading: boolean;
  scheduleDialogOpen: boolean;
  scheduleName: string;
  cronExpression: string;
  scheduleNameInputRef: React.RefObject<HTMLInputElement>;
  deleteRunDialogOpen: boolean;
  onCloseDeleteDialog: () => void;
  onCloseScheduleDialog: () => void;
  onCloseDeleteRunDialog: () => void;
  onDeleteAllRuns: () => Promise<void>;
  onDeleteRun: () => Promise<void>;
  onScheduleJob: () => Promise<void>;
  onScheduleNameChange: (e: React.ChangeEvent<HTMLInputElement>) => void;
  onCronExpressionChange: (e: React.ChangeEvent<HTMLInputElement>) => void;
}

const RunDialogs: React.FC<RunDialogsProps> = ({
  deleteDialogOpen,
  deleteLoading,
  scheduleDialogOpen,
  scheduleName,
  cronExpression,
  scheduleNameInputRef,
  deleteRunDialogOpen,
  onCloseDeleteDialog,
  onCloseScheduleDialog,
  onCloseDeleteRunDialog,
  onDeleteAllRuns,
  onDeleteRun,
  onScheduleJob,
  onScheduleNameChange,
  onCronExpressionChange,
}) => {
  const { t } = useTranslation();

  return (
    <>
      <Dialog
        open={deleteDialogOpen}
        onClose={onCloseDeleteDialog}
      >
        <DialogTitle>{t('common.confirm')}</DialogTitle>
        <DialogContent>
          <DialogContentText>
            {t('runHistory.deleteConfirm')}
          </DialogContentText>
        </DialogContent>
        <DialogActions>
          <Button onClick={onCloseDeleteDialog} disabled={deleteLoading}>
            {t('common.cancel')}
          </Button>
          <Button onClick={onDeleteAllRuns} color="error" disabled={deleteLoading}>
            {deleteLoading ? t('common.loading') : t('common.delete')}
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog
        open={scheduleDialogOpen}
        onClose={onCloseScheduleDialog}
        maxWidth="sm"
        fullWidth
      >
        <DialogTitle>Schedule Job</DialogTitle>
        <DialogContent>
          <TextField
            fullWidth
            label="Schedule Name"
            value={scheduleName}
            onChange={onScheduleNameChange}
            margin="normal"
            inputRef={scheduleNameInputRef}
          />
          <TextField
            fullWidth
            label="Cron Expression"
            value={cronExpression}
            onChange={onCronExpressionChange}
            margin="normal"
            helperText="Example: '0 0 * * *' for daily at midnight"
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={onCloseScheduleDialog}>Cancel</Button>
          <Button onClick={onScheduleJob} variant="contained" color="primary">
            Schedule
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog
        open={deleteRunDialogOpen}
        onClose={onCloseDeleteRunDialog}
      >
        <DialogTitle>{t('common.confirm')}</DialogTitle>
        <DialogContent>
          <DialogContentText>
            {t('runHistory.deleteRunConfirm')}
          </DialogContentText>
        </DialogContent>
        <DialogActions>
          <Button onClick={onCloseDeleteRunDialog} disabled={deleteLoading}>
            {t('common.cancel')}
          </Button>
          <Button onClick={onDeleteRun} color="error" disabled={deleteLoading}>
            {deleteLoading ? t('common.loading') : t('common.delete')}
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
};

export default RunDialogs; 