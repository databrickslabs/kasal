/**
 * Per-run recipe curation — the control that switches workflow reuse on.
 *
 * Completed crew runs are mined into reusable recipes automatically, but a mined
 * recipe only proves a crew FINISHED; it says nothing about whether the output
 * was right. So nothing is ever reused until a human marks a recipe good, and
 * until someone does, the whole feature is inert by design.
 *
 * It is a CELL in the run list's "Reusable" column, deliberately beside Result
 * and Trace. Judging a crew reusable is a claim about its OUTPUT, so the control
 * has to sit where the output is one click away. A standalone recipe library was
 * tried and removed: it listed recipes with no way to see what they produced,
 * which is asking someone to vote on something they cannot see.
 *
 * Runs that were never mined (canvas runs, chat runs, anything still running)
 * render nothing at all rather than a disabled control, so the column stays
 * quiet instead of filling with dead affordances.
 */

import React, { useCallback, useEffect, useState } from 'react';
import {
  Button,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
  Divider,
  IconButton,
  ListItemIcon,
  ListItemText,
  Menu,
  MenuItem,
  Tooltip,
  Typography,
} from '@mui/material';
import BookmarkIcon from '@mui/icons-material/Bookmark';
import BookmarkBorderIcon from '@mui/icons-material/BookmarkBorder';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import BlockIcon from '@mui/icons-material/Block';
import VisibilityOffIcon from '@mui/icons-material/VisibilityOff';
import ClearIcon from '@mui/icons-material/Clear';
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutline';

import {
  RecipeCuration,
  RecipeJobEntry,
  WorkflowRecipeService,
} from '../../api/workflow/WorkflowRecipeService';
import {
  invalidateRecipeIndex,
  loadRecipeIndex,
  subscribeToRecipeIndex,
} from './recipeIndexCache';

const CURATION_LABEL: Record<string, string> = {
  good: 'Reusable',
  bad: 'Not good',
  hidden: 'Hidden',
};

interface Props {
  jobId: string;
}

export const RecipeCurationButton: React.FC<Props> = ({ jobId }) => {
  const [entry, setEntry] = useState<RecipeJobEntry | null>(null);
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);
  const [saving, setSaving] = useState(false);
  const [confirmDelete, setConfirmDelete] = useState(false);

  const refresh = useCallback(() => {
    let cancelled = false;
    loadRecipeIndex().then((index) => {
      if (!cancelled) setEntry(index[jobId] || null);
    });
    return () => {
      cancelled = true;
    };
  }, [jobId]);

  useEffect(() => {
    const cancel = refresh();
    const unsubscribe = subscribeToRecipeIndex(() => refresh());
    return () => {
      cancel();
      unsubscribe();
    };
  }, [refresh]);

  const setCuration = async (curation: RecipeCuration) => {
    if (!entry) return;
    setAnchorEl(null);
    setSaving(true);
    try {
      await WorkflowRecipeService.curate(entry.recipe_id, curation);
      invalidateRecipeIndex();
    } catch {
      // Curation is advisory; a failed write leaves the previous mark in place
      // and the chip re-reads it on the next load.
    } finally {
      setSaving(false);
    }
  };

  const deleteRecipe = async () => {
    if (!entry) return;
    setSaving(true);
    try {
      await WorkflowRecipeService.deleteRecipe(entry.recipe_id);
      invalidateRecipeIndex();
      setConfirmDelete(false);
    } catch {
      // Leave the dialog open on failure: the recipe is still there, and
      // closing would report a deletion that did not happen.
    } finally {
      setSaving(false);
    }
  };

  // Not mined — nothing to curate, and nothing worth occupying the row with.
  if (!entry) return null;

  const curated = entry.curation === 'good';
  const suppressed = entry.curation === 'bad' || entry.curation === 'hidden';

  return (
    <>
      <Tooltip
        title={
          entry.curation
            ? `Recipe: ${CURATION_LABEL[entry.curation]} — ${entry.intent_text}`
            : `Mined as a reusable recipe (${entry.run_count} run${
                entry.run_count === 1 ? '' : 's'
              }). Mark it good to let future crews learn from it.`
        }
      >
        <span>
          <IconButton
            size="small"
            onClick={(e) => setAnchorEl(e.currentTarget)}
            disabled={saving}
            color={curated ? 'success' : suppressed ? 'default' : 'primary'}
          >
            {saving ? (
              <CircularProgress size={16} />
            ) : curated ? (
              <BookmarkIcon fontSize="small" />
            ) : (
              <BookmarkBorderIcon
                fontSize="small"
                sx={suppressed ? { opacity: 0.4 } : undefined}
              />
            )}
          </IconButton>
        </span>
      </Tooltip>

      <Menu
        anchorEl={anchorEl}
        open={Boolean(anchorEl)}
        onClose={() => setAnchorEl(null)}
      >
        <MenuItem disabled sx={{ opacity: '1 !important' }}>
          <Typography variant="caption" color="text.secondary">
            Reuse this crew&apos;s shape for similar requests?
          </Typography>
        </MenuItem>
        <MenuItem onClick={() => setCuration('good')} selected={curated}>
          <ListItemIcon>
            <CheckCircleIcon fontSize="small" color="success" />
          </ListItemIcon>
          <ListItemText
            primary="Good — reuse this"
            secondary="Offered as an example when generating similar crews"
          />
        </MenuItem>
        <MenuItem
          onClick={() => setCuration('bad')}
          selected={entry.curation === 'bad'}
        >
          <ListItemIcon>
            <BlockIcon fontSize="small" color="error" />
          </ListItemIcon>
          <ListItemText
            primary="Not good"
            secondary="Never suggest this crew again"
          />
        </MenuItem>
        <MenuItem
          onClick={() => setCuration('hidden')}
          selected={entry.curation === 'hidden'}
        >
          <ListItemIcon>
            <VisibilityOffIcon fontSize="small" />
          </ListItemIcon>
          <ListItemText primary="Hide" secondary="Stop offering me this" />
        </MenuItem>
        {entry.curation && (
          <MenuItem onClick={() => setCuration(null)}>
            <ListItemIcon>
              <ClearIcon fontSize="small" />
            </ListItemIcon>
            <ListItemText primary="Clear mark" />
          </MenuItem>
        )}
        <Divider />
        <MenuItem
          onClick={() => {
            setAnchorEl(null);
            setConfirmDelete(true);
          }}
        >
          <ListItemIcon>
            <DeleteOutlineIcon fontSize="small" color="error" />
          </ListItemIcon>
          <ListItemText
            primary="Delete recipe"
            secondary="Removes it from the library; the run stays"
          />
        </MenuItem>
      </Menu>

      <Dialog open={confirmDelete} onClose={() => setConfirmDelete(false)}>
        <DialogTitle>Delete this recipe?</DialogTitle>
        <DialogContent>
          <DialogContentText>
            &ldquo;{entry.intent_text}&rdquo; leaves the reuse library and stops being
            offered when generating similar crews. The run it was mined from is not
            affected. This cannot be undone — to stop being offered a recipe without
            losing it, mark it &ldquo;Not good&rdquo; or &ldquo;Hide&rdquo; instead.
          </DialogContentText>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setConfirmDelete(false)} disabled={saving}>
            Cancel
          </Button>
          <Button onClick={deleteRecipe} color="error" disabled={saving}>
            {saving ? 'Deleting…' : 'Delete'}
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
};

export default RecipeCurationButton;
