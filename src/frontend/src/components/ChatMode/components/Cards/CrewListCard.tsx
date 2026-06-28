import React from 'react';
import Box from '@mui/material/Box';
import { CatalogListResult } from '../../types/dispatcher';
import { buttonResetSx } from '../../chatSx';

interface CrewListCardProps {
  data: CatalogListResult;
  onCommand?: (command: string) => void;
}

const CrewListCard: React.FC<CrewListCardProps> = ({ data, onCommand }) => {
  if (!data.plans || data.plans.length === 0) {
    return (
      <Box sx={{ fontSize: 14, my: 1, color: 'text.disabled' }}>
        No crews found.
      </Box>
    );
  }

  return (
    <Box sx={{ my: 1.5, display: 'flex', flexDirection: 'column', gap: 0.75 }}>
      {data.plans.map((plan) => (
        <Box
          key={plan.id}
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
            '&:hover .crew-run-btn': { opacity: 1 },
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
                d="M18 18.72a9.094 9.094 0 003.741-.479 3 3 0 00-4.682-2.72m.94 3.198l.001.031c0 .225-.012.447-.037.666A11.944 11.944 0 0112 21c-2.17 0-4.207-.576-5.963-1.584A6.062 6.062 0 016 18.719m12 0a5.971 5.971 0 00-.941-3.197m0 0A5.995 5.995 0 0012 12.75a5.995 5.995 0 00-5.058 2.772m0 0a3 3 0 00-4.681 2.72 8.986 8.986 0 003.74.477m.94-3.197a5.971 5.971 0 00-.94 3.197M15 6.75a3 3 0 11-6 0 3 3 0 016 0zm6 3a2.25 2.25 0 11-4.5 0 2.25 2.25 0 014.5 0zm-13.5 0a2.25 2.25 0 11-4.5 0 2.25 2.25 0 014.5 0z"
              />
            </Box>
          </Box>

          {/* Name + badges */}
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
              {plan.name}
            </Box>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mt: 0.25 }}>
              <Box component="span" sx={{ fontSize: 11, lineHeight: 1.25, color: 'text.disabled' }}>
                {plan.agent_count ?? 0} agent{(plan.agent_count ?? 0) !== 1 ? 's' : ''}
              </Box>
              <Box component="span" sx={{ color: 'divider' }}>&middot;</Box>
              <Box component="span" sx={{ fontSize: 11, lineHeight: 1.25, color: 'text.disabled' }}>
                {plan.task_count ?? 0} task{(plan.task_count ?? 0) !== 1 ? 's' : ''}
              </Box>
            </Box>
          </Box>

          {/* Play button */}
          <Box
            component="button"
            className="crew-run-btn"
            onClick={() => onCommand?.(`/run crew ${plan.name}`)}
            title={`Run ${plan.name}`}
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

export default CrewListCard;
