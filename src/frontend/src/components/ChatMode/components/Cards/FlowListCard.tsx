import React from 'react';
import Box from '@mui/material/Box';
import { FlowListResult } from '../../types/dispatcher';
import { buttonResetSx } from '../../chatSx';

interface FlowListCardProps {
  data: FlowListResult;
  onCommand?: (command: string) => void;
}

const FlowListCard: React.FC<FlowListCardProps> = ({ data, onCommand }) => {
  if (!data.flows || data.flows.length === 0) {
    return (
      <Box sx={{ fontSize: 14, my: 1, color: 'text.disabled' }}>
        No flows found.
      </Box>
    );
  }

  return (
    <Box sx={{ my: 1.5, display: 'flex', flexDirection: 'column', gap: 0.75 }}>
      {data.flows.map((flow) => (
        <Box
          key={flow.id}
          sx={{
            display: 'flex',
            alignItems: 'center',
            gap: 1.5,
            borderRadius: '8px',
            px: 1.5,
            py: 1.25,
            transition: 'background-color 0.15s, border-color 0.15s, color 0.15s',
            cursor: 'default',
            backgroundColor: 'background.paper',
            border: 1,
            borderColor: 'divider',
            '&:hover .flow-run-btn': { opacity: 1 },
          }}
        >
          {/* Icon */}
          <Box
            sx={{
              width: 32,
              height: 32,
              borderRadius: '8px',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              flexShrink: 0,
              backgroundColor: (t) => t.chat.bgSecondary,
            }}
          >
            <Box
              component="svg"
              sx={{ width: 16, height: 16, color: 'primary.main' }}
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
              strokeWidth={1.8}
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                d="M7.5 21L3 16.5m0 0L7.5 12M3 16.5h13.5m0-13.5L21 7.5m0 0L16.5 12M21 7.5H7.5"
              />
            </Box>
          </Box>

          {/* Name + badge */}
          <Box sx={{ flex: 1, minWidth: 0 }}>
            <Box
              sx={{
                fontSize: 14,
                fontWeight: 500,
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
                color: 'text.primary',
              }}
            >
              {flow.name}
            </Box>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mt: 0.25 }}>
              <Box component="span" sx={{ fontSize: 11, lineHeight: 1.25, color: 'text.disabled' }}>
                {flow.node_count ?? 0} node{(flow.node_count ?? 0) !== 1 ? 's' : ''}
              </Box>
            </Box>
          </Box>

          {/* Play button */}
          <Box
            component="button"
            className="flow-run-btn"
            onClick={() => onCommand?.(`/run flow ${flow.name}`)}
            title={`Run ${flow.name}`}
            sx={{
              ...buttonResetSx,
              flexShrink: 0,
              width: 32,
              height: 32,
              borderRadius: '9999px',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              transition: 'all 0.15s',
              opacity: 0.6,
              backgroundColor: 'primary.main',
              color: '#fff',
              '&:hover': { transform: 'scale(1.1)' },
            }}
          >
            <Box component="svg" sx={{ width: 14, height: 14, ml: 0.25 }} viewBox="0 0 24 24" fill="currentColor">
              <path d="M8 5.14v14l11-7-11-7z" />
            </Box>
          </Box>
        </Box>
      ))}
    </Box>
  );
};

export default FlowListCard;
