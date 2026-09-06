import React, { useEffect, useRef } from 'react';
import { Paper, List, ListItemButton, ListItemText, Typography } from '@mui/material';
import { SlashCommand } from '../utils/chatHelpers';

interface SlashCommandMenuProps {
  commands: SlashCommand[];
  selectedIndex: number;
  onSelect: (command: SlashCommand) => void;
}

const SlashCommandMenu: React.FC<SlashCommandMenuProps> = ({ commands, selectedIndex, onSelect }) => {
  const listRef = useRef<HTMLUListElement>(null);

  useEffect(() => {
    const selected = listRef.current?.children[selectedIndex] as HTMLElement | undefined;
    selected?.scrollIntoView({ block: 'nearest' });
  }, [selectedIndex]);

  if (commands.length === 0) return null;

  return (
    <Paper
      elevation={4}
      sx={{
        position: 'absolute',
        bottom: '100%',
        left: 0,
        right: 0,
        mb: 0.5,
        maxHeight: 260,
        overflow: 'auto',
        zIndex: 20,
      }}
    >
      <List dense ref={listRef} sx={{ py: 0.5 }}>
        {commands.map((cmd, index) => (
          <ListItemButton
            key={cmd.command}
            selected={index === selectedIndex}
            onMouseDown={(e) => {
              e.preventDefault(); // prevent input blur
              onSelect(cmd);
            }}
            sx={{ py: 0.5, px: 1.5 }}
          >
            <ListItemText
              primary={
                <Typography variant="body2" sx={{ fontFamily: 'monospace', fontWeight: 500 }}>
                  {cmd.command}
                </Typography>
              }
              secondary={cmd.description}
            />
          </ListItemButton>
        ))}
      </List>
    </Paper>
  );
};

export default SlashCommandMenu;
