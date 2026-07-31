import React, { useState } from 'react';
import { IconButton, Tooltip } from '@mui/material';
import PublicIcon from '@mui/icons-material/Public';
import PublicOffIcon from '@mui/icons-material/PublicOff';

import PublishDialog from './PublishDialog';
import { PublishableEntity } from '../../../types/workflow/publication';

interface PublishButtonProps {
  entityType: PublishableEntity;
  entityId: string;
  entityName: string;
  /** Its nodes, so the dialog can derive the input fields it declares. */
  nodes?: unknown[];
  /** Whether it is currently published, so the icon can say so at a glance. */
  published?: boolean;
  onChanged?: (published: boolean) => void;
}

/**
 * The publish control for one crew or flow in the catalog.
 *
 * The icon carries the state deliberately. The worst outcome of this feature is
 * a crew reachable from outside the workspace without anyone inside realising —
 * so "published" has to be visible in the list itself, not only inside a dialog
 * someone has to open.
 */
const PublishButton: React.FC<PublishButtonProps> = ({
  entityType,
  entityId,
  entityName,
  nodes,
  published = false,
  onChanged,
}) => {
  const [open, setOpen] = useState(false);
  const [isPublished, setIsPublished] = useState(published);

  React.useEffect(() => setIsPublished(published), [published]);

  const handleChanged = (nowPublished: boolean) => {
    setIsPublished(nowPublished);
    onChanged?.(nowPublished);
  };

  return (
    <>
      <Tooltip
        title={
          isPublished
            ? `Published. Click to edit where it is reachable from, or unpublish.`
            : `Publish this ${entityType} so chat or an external agent can run it`
        }
      >
        <IconButton
          size="small"
          color={isPublished ? 'success' : 'default'}
          onClick={(event) => {
            // The row itself is clickable (it loads the crew/flow), so the
            // click must not also select it.
            event.stopPropagation();
            setOpen(true);
          }}
          aria-label={isPublished ? 'Edit publication' : 'Publish'}
        >
          {isPublished ? (
            <PublicIcon fontSize="small" />
          ) : (
            <PublicOffIcon fontSize="small" />
          )}
        </IconButton>
      </Tooltip>

      {open && (
        <PublishDialog
          open={open}
          onClose={() => setOpen(false)}
          entityType={entityType}
          entityId={entityId}
          entityName={entityName}
          nodes={nodes}
          onChanged={handleChanged}
        />
      )}
    </>
  );
};

export default PublishButton;
