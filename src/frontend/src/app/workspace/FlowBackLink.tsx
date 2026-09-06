import React, { useState } from 'react';
import { Box, Chip, Menu, MenuItem, ListItemText } from '@mui/material';
import AccountTreeIcon from '@mui/icons-material/AccountTree';
import { useTabManagerStore } from '../../store/tabManager';
import { useUILayoutStore } from '../../store/uiLayout';
import { flowsContainingCrew } from '../../utils/flowsContainingCrew';

/**
 * A way back to the flow a crew belongs to.
 *
 * The counterpart to opening a crew from a flow node — but it cannot be a link
 * on a node, because on this canvas a crew is a dozen agent and task nodes and
 * none of them belongs to the flow. The CREW does, so the link belongs to the
 * canvas and appears once.
 *
 * Renders nothing unless it applies: an unsaved crew has no id for a flow to
 * reference, and a crew whose flows are all closed has nowhere to go back to.
 * When more than one open flow uses the crew it offers the choice rather than
 * silently picking one.
 */
const FlowBackLink: React.FC = () => {
  const tabs = useTabManagerStore((state) => state.tabs);
  const activeTabId = useTabManagerStore((state) => state.activeTabId);
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);

  const activeTab = tabs.find((tab) => tab.id === activeTabId);
  const flows = flowsContainingCrew(
    tabs.filter((tab) => tab.group_id === activeTab?.group_id),
    activeTab?.savedCrewId,
  );

  if (flows.length === 0) return null;

  const goTo = (tabId: string) => {
    setAnchorEl(null);
    // Point the tab at its flow canvas BEFORE switching to it. The tab-switch
    // effect restores whichever canvas the tab last remembered, and runs after
    // this handler — so a flow tab last left on its crew side would otherwise
    // win the race and land us on a crew canvas, having just asked for a flow.
    useTabManagerStore.getState().updateTabViewMode(tabId, 'flow');
    useTabManagerStore.getState().setActiveTab(tabId);
    useUILayoutStore.getState().setAppMode('flow');
  };

  const label =
    flows.length === 1 ? `In flow: ${flows[0].name}` : `In ${flows.length} flows`;

  return (
    <Box sx={{ position: 'absolute', top: 8, left: 8, zIndex: 10 }}>
      <Chip
        size="small"
        icon={<AccountTreeIcon fontSize="small" />}
        label={label}
        onClick={(event) =>
          flows.length === 1 ? goTo(flows[0].tabId) : setAnchorEl(event.currentTarget)
        }
        sx={{
          maxWidth: 260,
          bgcolor: 'background.paper',
          border: 1,
          borderColor: 'divider',
          boxShadow: 1,
          '&:hover': { bgcolor: 'action.hover' },
        }}
      />

      <Menu anchorEl={anchorEl} open={Boolean(anchorEl)} onClose={() => setAnchorEl(null)}>
        {flows.map((flow) => (
          <MenuItem key={flow.tabId} onClick={() => goTo(flow.tabId)}>
            <ListItemText primary={flow.name} />
          </MenuItem>
        ))}
      </Menu>
    </Box>
  );
};

export default FlowBackLink;
