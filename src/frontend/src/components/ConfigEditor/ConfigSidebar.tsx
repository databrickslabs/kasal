/**
 * Config Sidebar — left panel listing all 26 keys with status badges.
 */

import React from 'react';
import {
  Box,
  List,
  ListItemButton,
  ListItemText,
  Typography,
  Divider,
  Chip,
  useTheme,
  alpha,
} from '@mui/material';
import {
  CONFIG_KEY_CATEGORIES,
  CONFIG_KEY_LABELS,
  getKeyStatus,
  countTodos,
  type ConfigKeyStatus,
  type PipelineConfig,
} from '../../types/configEditor';

interface ConfigSidebarProps {
  config: PipelineConfig | null;
  selectedKey: string | null;
  onSelectKey: (key: string) => void;
}

const STATUS_COLORS: Record<ConfigKeyStatus, string> = {
  auto: '#4caf50',
  todo: '#ff9800',
  empty: '#f44336',
  null: '#9e9e9e',
};

const STATUS_EMOJI: Record<ConfigKeyStatus, string> = {
  auto: '\u{1F7E2}',  // 🟢
  todo: '\u{1F7E0}',  // 🟠
  empty: '\u{1F534}',  // 🔴
  null: '\u26AA',       // ⚪
};

function entryCountLabel(value: unknown): string {
  if (value === null || value === undefined) return '';
  if (typeof value === 'string') return value.length > 0 ? `${value.length} chars` : '';
  if (Array.isArray(value)) return `${value.length}`;
  if (typeof value === 'object') return `${Object.keys(value as object).length}`;
  return '';
}

const ConfigSidebar: React.FC<ConfigSidebarProps> = ({
  config,
  selectedKey,
  onSelectKey,
}) => {
  const theme = useTheme();

  if (!config) {
    return (
      <Box sx={{ p: 2 }}>
        <Typography variant="body2" color="text.secondary">
          Load a config to see keys
        </Typography>
      </Box>
    );
  }

  // Compute summary counts
  const allKeys = Object.keys(config);
  const statusCounts: Record<ConfigKeyStatus, number> = { auto: 0, todo: 0, empty: 0, null: 0 };
  let totalTodos = 0;

  allKeys.forEach((key) => {
    const status = getKeyStatus(config[key]);
    statusCounts[status]++;
    totalTodos += countTodos(config[key]);
  });

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      <Box sx={{ flex: 1, overflowY: 'auto' }}>
        {Object.entries(CONFIG_KEY_CATEGORIES).map(([category, keys]) => {
          // Only show keys that exist in the loaded config
          const presentKeys = keys.filter((k) => k in config);
          if (presentKeys.length === 0) return null;

          return (
            <Box key={category}>
              <Typography
                variant="caption"
                sx={{
                  px: 2,
                  pt: 1.5,
                  pb: 0.5,
                  display: 'block',
                  fontWeight: 600,
                  textTransform: 'uppercase',
                  letterSpacing: '0.5px',
                  fontSize: '0.65rem',
                  color: theme.palette.primary.main,
                }}
              >
                {category}
              </Typography>
              <List dense disablePadding>
                {presentKeys.map((key) => {
                  const status = getKeyStatus(config[key]);
                  const todoCount = countTodos(config[key]);
                  const count = entryCountLabel(config[key]);
                  const isSelected = selectedKey === key;

                  return (
                    <ListItemButton
                      key={key}
                      selected={isSelected}
                      onClick={() => onSelectKey(key)}
                      sx={{
                        py: 0.5,
                        px: 2,
                        borderLeft: isSelected
                          ? `3px solid ${theme.palette.primary.main}`
                          : '3px solid transparent',
                        '&.Mui-selected': {
                          backgroundColor: alpha(theme.palette.primary.main, 0.08),
                        },
                      }}
                    >
                      <Typography
                        sx={{ mr: 1, fontSize: '0.75rem', lineHeight: 1 }}
                      >
                        {STATUS_EMOJI[status]}
                      </Typography>
                      <ListItemText
                        primary={
                          <Typography
                            variant="body2"
                            sx={{
                              fontSize: '0.8rem',
                              fontWeight: isSelected ? 600 : 400,
                              overflow: 'hidden',
                              textOverflow: 'ellipsis',
                              whiteSpace: 'nowrap',
                            }}
                          >
                            {CONFIG_KEY_LABELS[key] || key}
                          </Typography>
                        }
                        secondary={
                          count ? (
                            <Typography variant="caption" color="text.secondary">
                              {count} entries
                              {todoCount > 0 && (
                                <Chip
                                  label={`${todoCount} TODO`}
                                  size="small"
                                  sx={{
                                    ml: 0.5,
                                    height: 16,
                                    fontSize: '0.6rem',
                                    backgroundColor: alpha(STATUS_COLORS.todo, 0.15),
                                    color: STATUS_COLORS.todo,
                                  }}
                                />
                              )}
                            </Typography>
                          ) : undefined
                        }
                      />
                    </ListItemButton>
                  );
                })}
              </List>
              <Divider />
            </Box>
          );
        })}

        {/* Show any keys not in categories (unknown extra keys) */}
        {(() => {
          const categorizedKeys = new Set(
            Object.values(CONFIG_KEY_CATEGORIES).flat(),
          );
          const extraKeys = allKeys.filter((k) => !categorizedKeys.has(k));
          if (extraKeys.length === 0) return null;

          return (
            <Box>
              <Typography
                variant="caption"
                sx={{
                  px: 2, pt: 1.5, pb: 0.5, display: 'block',
                  fontWeight: 600, textTransform: 'uppercase',
                  letterSpacing: '0.5px', fontSize: '0.65rem',
                  color: theme.palette.warning.main,
                }}
              >
                Other Keys
              </Typography>
              <List dense disablePadding>
                {extraKeys.map((key) => {
                  const status = getKeyStatus(config[key]);
                  const isSelected = selectedKey === key;
                  return (
                    <ListItemButton
                      key={key}
                      selected={isSelected}
                      onClick={() => onSelectKey(key)}
                      sx={{ py: 0.5, px: 2 }}
                    >
                      <Typography sx={{ mr: 1, fontSize: '0.75rem' }}>
                        {STATUS_EMOJI[status]}
                      </Typography>
                      <ListItemText
                        primary={<Typography variant="body2" sx={{ fontSize: '0.8rem' }}>{key}</Typography>}
                      />
                    </ListItemButton>
                  );
                })}
              </List>
              <Divider />
            </Box>
          );
        })()}
      </Box>

      {/* Summary bar */}
      <Box
        sx={{
          p: 1.5,
          borderTop: `1px solid ${theme.palette.divider}`,
          backgroundColor: alpha(theme.palette.background.default, 0.5),
        }}
      >
        <Typography variant="caption" sx={{ fontWeight: 600, display: 'block', mb: 0.5 }}>
          Summary ({allKeys.length} keys)
        </Typography>
        <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap' }}>
          {statusCounts.auto > 0 && (
            <Chip
              label={`${STATUS_EMOJI.auto} ${statusCounts.auto} auto`}
              size="small"
              sx={{ height: 20, fontSize: '0.7rem', backgroundColor: alpha(STATUS_COLORS.auto, 0.12) }}
            />
          )}
          {statusCounts.todo > 0 && (
            <Chip
              label={`${STATUS_EMOJI.todo} ${statusCounts.todo} TODO`}
              size="small"
              sx={{ height: 20, fontSize: '0.7rem', backgroundColor: alpha(STATUS_COLORS.todo, 0.12) }}
            />
          )}
          {statusCounts.empty > 0 && (
            <Chip
              label={`${STATUS_EMOJI.empty} ${statusCounts.empty} empty`}
              size="small"
              sx={{ height: 20, fontSize: '0.7rem', backgroundColor: alpha(STATUS_COLORS.empty, 0.12) }}
            />
          )}
          {statusCounts.null > 0 && (
            <Chip
              label={`${STATUS_EMOJI.null} ${statusCounts.null} null`}
              size="small"
              sx={{ height: 20, fontSize: '0.7rem', backgroundColor: alpha(STATUS_COLORS.null, 0.12) }}
            />
          )}
        </Box>
        {totalTodos > 0 && (
          <Typography variant="caption" color="warning.main" sx={{ mt: 0.5, display: 'block' }}>
            {totalTodos} TODO marker{totalTodos > 1 ? 's' : ''} total
          </Typography>
        )}
      </Box>
    </Box>
  );
};

export default ConfigSidebar;
