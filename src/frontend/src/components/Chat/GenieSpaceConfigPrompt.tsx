/**
 * Inline prompt for configuring GenieTool space after crew generation.
 *
 * For each task that uses GenieTool, shows:
 *  - Suggested space with Approve / Choose Different / Skip actions
 *  - Or a selector when no suggestion exists or user wants to pick differently
 *
 * The entire component is wrapped in an event-isolation boundary so that
 * keyboard events (typing in the Autocomplete) do not bubble up to the
 * parent chat input handler, and the chat's aggressive focus-restoration
 * timers cannot steal focus from the selector.
 */

import React, { useState, useCallback } from 'react';
import { Box, Typography, Button, Chip, Stack } from '@mui/material';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import { GenieSpaceSelector } from '../Common/GenieSpaceSelector';
import { TaskService } from '../../api/TaskService';
import { useWorkflowStore } from '../../store/workflow';
import type { ToolConfigNeededData } from '../../hooks/global/useCrewGenerationSSE';

interface GenieSpaceConfigPromptProps {
  configs: ToolConfigNeededData[];
}

interface TaskConfigState {
  status: 'pending' | 'selecting' | 'configured' | 'skipped';
  spaceName?: string;
}

/** Stop keyboard / mouse events from propagating to the chat layer. */
const stopPropagation = (e: React.SyntheticEvent) => e.stopPropagation();

const GenieSpaceConfigPromptInner: React.FC<GenieSpaceConfigPromptProps> = ({ configs }) => {
  const setNodes = useWorkflowStore((s) => s.setNodes);

  const [taskStates, setTaskStates] = useState<Record<string, TaskConfigState>>(() => {
    const initial: Record<string, TaskConfigState> = {};
    configs.forEach((c) => {
      initial[c.task_id] = { status: 'pending' };
    });
    return initial;
  });

  const updateState = useCallback((taskId: string, update: Partial<TaskConfigState>) => {
    setTaskStates((prev) => ({
      ...prev,
      [taskId]: { ...prev[taskId], ...update },
    }));
  }, []);

  /** Sync the GenieTool tool_configs into the corresponding ReactFlow task node. */
  const syncNodeToolConfigs = useCallback(
    (taskId: string, genieToolConfig: Record<string, unknown>) => {
      const nodeId = `task-${taskId}`;
      setNodes((prev) =>
        prev.map((n) => {
          if (n.id === nodeId) {
            const existing = (n.data.tool_configs || {}) as Record<string, unknown>;
            return {
              ...n,
              data: {
                ...n.data,
                tool_configs: { ...existing, GenieTool: genieToolConfig },
              },
            };
          }
          return n;
        })
      );
    },
    [setNodes]
  );

  const handleApprove = useCallback(async (config: ToolConfigNeededData) => {
    if (!config.suggested_space) return;
    const genieConfig = { spaceId: config.suggested_space.id, spaceName: config.suggested_space.name };
    try {
      await TaskService.patchTaskToolConfigs(config.task_id, {
        GenieTool: genieConfig,
      });
      syncNodeToolConfigs(config.task_id, genieConfig);
      updateState(config.task_id, {
        status: 'configured',
        spaceName: config.suggested_space.name,
      });
    } catch (err) {
      console.error('Failed to configure GenieTool space:', err);
    }
  }, [updateState, syncNodeToolConfigs]);

  const handleSpaceSelected = useCallback(async (config: ToolConfigNeededData, spaceId: string, spaceName?: string) => {
    if (!spaceId) return;
    const genieConfig = { spaceId, spaceName: spaceName || spaceId };
    try {
      await TaskService.patchTaskToolConfigs(config.task_id, {
        GenieTool: genieConfig,
      });
      syncNodeToolConfigs(config.task_id, genieConfig);
      updateState(config.task_id, {
        status: 'configured',
        spaceName: spaceName || spaceId,
      });
    } catch (err) {
      console.error('Failed to configure GenieTool space:', err);
    }
  }, [updateState, syncNodeToolConfigs]);

  return (
    // Event-isolation wrapper: prevents keyboard/focus/click events from
    // reaching the chat's own handlers which would steal focus or trigger
    // unintended actions (like sending a message on Enter).
    <Box
      onKeyDown={stopPropagation}
      onKeyUp={stopPropagation}
      onKeyPress={stopPropagation}
      onFocus={stopPropagation}
      onClick={stopPropagation}
      sx={{ mt: 1 }}
    >
      <Typography variant="body2" sx={{ fontWeight: 600, mb: 1 }}>
        GenieTool Configuration
      </Typography>
      {configs.map((config) => {
        const state = taskStates[config.task_id] || { status: 'pending' };

        if (state.status === 'configured') {
          return (
            <Box key={config.task_id} sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.5 }}>
              <CheckCircleIcon sx={{ fontSize: 16, color: 'success.main' }} />
              <Typography variant="body2">
                <strong>{config.task_name}</strong> — Configured: <em>{state.spaceName}</em>
              </Typography>
            </Box>
          );
        }

        if (state.status === 'skipped') {
          return (
            <Box key={config.task_id} sx={{ mb: 0.5 }}>
              <Typography variant="body2" color="text.secondary">
                <strong>{config.task_name}</strong> — Skipped (configure later via task settings)
              </Typography>
            </Box>
          );
        }

        if (state.status === 'selecting') {
          return (
            <Box key={config.task_id} sx={{ mb: 1 }}>
              <Typography variant="body2" sx={{ mb: 0.5 }}>
                <strong>{config.task_name}</strong> — Select a Genie space:
              </Typography>
              <Box sx={{ maxWidth: 350 }}>
                <GenieSpaceSelector
                  value={null}
                  onChange={(value, spaceName) => {
                    if (value && typeof value === 'string') {
                      handleSpaceSelected(config, value, spaceName);
                    }
                  }}
                  label=""
                  placeholder="Search Genie spaces..."
                  fullWidth
                />
              </Box>
              <Button
                size="small"
                sx={{ mt: 0.5, textTransform: 'none', fontSize: '0.75rem' }}
                onClick={() => updateState(config.task_id, { status: 'skipped' })}
              >
                Skip
              </Button>
            </Box>
          );
        }

        // Default: pending state
        return (
          <Box key={config.task_id} sx={{ mb: 1 }}>
            <Typography variant="body2" sx={{ mb: 0.5 }}>
              <strong>{config.task_name}</strong> uses GenieTool
              {config.suggested_space ? (
                <> — Suggested: <Chip label={config.suggested_space.name} size="small" sx={{ mx: 0.5 }} /></>
              ) : (
                <> — No space detected</>
              )}
            </Typography>
            <Stack direction="row" spacing={1}>
              {config.suggested_space && (
                <Button
                  size="small"
                  variant="contained"
                  sx={{ textTransform: 'none', fontSize: '0.75rem' }}
                  onClick={() => handleApprove(config)}
                >
                  Approve
                </Button>
              )}
              <Button
                size="small"
                variant="outlined"
                sx={{ textTransform: 'none', fontSize: '0.75rem' }}
                onClick={() => updateState(config.task_id, { status: 'selecting' })}
              >
                {config.suggested_space ? 'Choose Different' : 'Select Space'}
              </Button>
              <Button
                size="small"
                sx={{ textTransform: 'none', fontSize: '0.75rem', color: 'text.secondary' }}
                onClick={() => updateState(config.task_id, { status: 'skipped' })}
              >
                Skip
              </Button>
            </Stack>
          </Box>
        );
      })}
    </Box>
  );
};

export const GenieSpaceConfigPrompt = React.memo(GenieSpaceConfigPromptInner);
