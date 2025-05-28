import React, { useMemo } from 'react';
import { 
  Box, 
  IconButton, 
  Typography, 
  Fade, 
  Paper, 
  useTheme, 
  Zoom, 
  ClickAwayListener,
  alpha,
  Divider
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import KeyboardIcon from '@mui/icons-material/Keyboard';
import { ShortcutConfig } from '../types/shortcuts';
import { useShortcutsStore } from '../store/shortcuts';

interface ShortcutsCircleProps {
  shortcuts?: ShortcutConfig[];
}

const ShortcutsCircle: React.FC<ShortcutsCircleProps> = ({ shortcuts }) => {
  const { setShortcutsVisible, shortcuts: storeShortcuts, showShortcuts } = useShortcutsStore();
  const theme = useTheme();
  
  // Use provided shortcuts or fall back to store shortcuts
  const allShortcuts = shortcuts || storeShortcuts;

  const handleClose = () => {
    setShortcutsVisible(false);
  };

  // Group shortcuts by action to avoid duplicates
  const uniqueShortcuts = useMemo(() => {
    const shortcutMap = new Map<string, ShortcutConfig>();
    
    allShortcuts.forEach((shortcut: ShortcutConfig) => {
      if (!shortcutMap.has(shortcut.action)) {
        shortcutMap.set(shortcut.action, shortcut);
      }
    });
    
    return Array.from(shortcutMap.values());
  }, [allShortcuts]);

  // Group shortcuts by category
  const groupedShortcuts = useMemo(() => {
    const result: Record<string, ShortcutConfig[]> = {
      'Canvas': [],
      'Creation': [],
      'Execution': [],
      'Management': []
    };

    uniqueShortcuts.forEach(shortcut => {
      const action = shortcut.action;
      
      if (action.includes('zoom') || action.includes('fit') || action.includes('clear') || 
          action.includes('delete') || action.includes('select') || action === 'undo' || 
          action === 'redo' || action === 'copy' || action === 'paste') {
        result['Canvas'].push(shortcut);
      } else if (action.includes('open') || action.includes('generate')) {
        result['Creation'].push(shortcut);
      } else if (action.includes('execute')) {
        result['Execution'].push(shortcut);
      } else {
        result['Management'].push(shortcut);
      }
    });

    return result;
  }, [uniqueShortcuts]);

  // Don't render anything if shortcuts shouldn't be shown
  if (!showShortcuts) {
    return null;
  }

  return (
    <Box
      sx={{
        position: 'fixed',
        top: 16,
        left: 16,
        zIndex: 1000,
      }}
    >
      <ClickAwayListener onClickAway={handleClose}>
        <Fade in={true} timeout={300}>
          <Paper
            elevation={6}
            sx={{
              width: 400,
              borderRadius: 3,
              overflow: 'hidden',
              backgroundColor: theme.palette.background.paper,
              boxShadow: `0 8px 32px ${alpha(theme.palette.primary.main, 0.15)}`,
              border: `1px solid ${alpha(theme.palette.divider, 0.08)}`,
            }}
          >
            {/* Header */}
            <Box
              sx={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                p: 2,
                backgroundColor: alpha(theme.palette.primary.main, 0.05),
                borderBottom: `1px solid ${alpha(theme.palette.divider, 0.1)}`,
              }}
            >
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
                <KeyboardIcon color="primary" />
                <Typography variant="h6" sx={{ fontWeight: 500 }}>
                  Keyboard Shortcuts
                </Typography>
              </Box>
              <IconButton 
                onClick={handleClose} 
                size="small"
                sx={{
                  color: theme.palette.text.secondary,
                  '&:hover': {
                    backgroundColor: alpha(theme.palette.divider, 0.1),
                  }
                }}
              >
                <CloseIcon fontSize="small" />
              </IconButton>
            </Box>

            {/* Content */}
            <Box
              sx={{
                maxHeight: 'calc(100vh - 150px)',
                overflowY: 'auto',
                p: 2,
              }}
            >
              {Object.entries(groupedShortcuts).map(([category, shortcuts]) => {
                if (shortcuts.length === 0) return null;
                
                return (
                  <Zoom in key={category} style={{ transitionDelay: '50ms' }}>
                    <Box sx={{ mb: 3 }}>
                      <Typography 
                        variant="subtitle2" 
                        sx={{ 
                          color: theme.palette.primary.main, 
                          mb: 1,
                          fontWeight: 600,
                          textTransform: 'uppercase',
                          letterSpacing: '0.5px',
                          fontSize: '0.75rem'
                        }}
                      >
                        {category}
                      </Typography>
                      <Divider sx={{ mb: 1.5 }} />
                      
                      {shortcuts.map((shortcut, index) => (
                        <Box
                          key={`${shortcut.action}-${index}`}
                          sx={{
                            display: 'flex',
                            justifyContent: 'space-between',
                            alignItems: 'center',
                            mb: 1,
                            py: 0.75,
                            px: 1,
                            borderRadius: 1,
                            '&:hover': {
                              backgroundColor: alpha(theme.palette.action.hover, 0.5),
                            },
                          }}
                        >
                          <Typography variant="body2" sx={{ color: theme.palette.text.primary }}>
                            {shortcut.description}
                          </Typography>
                          <Box 
                            sx={{ 
                              display: 'flex', 
                              gap: 0.5,
                              flexWrap: 'wrap', 
                              justifyContent: 'flex-end'
                            }}
                          >
                            {shortcut.keys.map((key: string, keyIndex: number) => (
                              <Typography
                                key={`${key}-${keyIndex}`}
                                variant="caption"
                                component="span"
                                sx={{
                                  fontFamily: 'monospace',
                                  backgroundColor: alpha(theme.palette.primary.main, 0.1),
                                  color: theme.palette.primary.main,
                                  px: 0.75,
                                  py: 0.4,
                                  borderRadius: 0.75,
                                  fontWeight: 500,
                                  border: `1px solid ${alpha(theme.palette.primary.main, 0.2)}`,
                                  minWidth: '1.5rem',
                                  textAlign: 'center',
                                  fontSize: '0.7rem',
                                  boxShadow: `0 1px 2px ${alpha(theme.palette.common.black, 0.05)}`,
                                  display: 'inline-block'
                                }}
                              >
                                {key === 'Control' ? 'Ctrl' : key === ' ' ? 'Space' : key}
                              </Typography>
                            ))}
                          </Box>
                        </Box>
                      ))}
                    </Box>
                  </Zoom>
                );
              })}
            </Box>
          </Paper>
        </Fade>
      </ClickAwayListener>
    </Box>
  );
};

export default ShortcutsCircle; 