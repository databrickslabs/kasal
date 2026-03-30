import { Box, Typography, CircularProgress } from '@mui/material';
import { CrewResponse } from '../../types/crews';

interface CrewSelectionStepProps {
  crews: CrewResponse[];
  selectedCrewIds: string[];
  onCrewSelect: (crew: CrewResponse) => void;
}

const CrewSelectionStep = ({ crews, selectedCrewIds, onCrewSelect }: CrewSelectionStepProps) => {
  if (!crews || crews.length === 0) {
    return (
      <Box display="flex" justifyContent="center" p={3}>
        <CircularProgress />
      </Box>
    );
  }

  return (
    <Box sx={{ pt: 2, pb: 2 }}>
      <Typography variant="body1" gutterBottom>
        Select crews to add to the canvas:
      </Typography>
      <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 2, mt: 2 }}>
        {crews.map(crew => (
          <Box
            key={crew.id}
            onClick={() => onCrewSelect(crew)}
            sx={{
              border: '2px solid',
              borderColor: selectedCrewIds.includes(crew.id) ? 'primary.main' : 'grey.300',
              borderRadius: '8px',
              padding: 2,
              cursor: 'pointer',
              bgcolor: selectedCrewIds.includes(crew.id) ? 'primary.light' : 'background.paper',
              transition: 'all 0.2s ease',
              width: 'calc(50% - 8px)',
              boxSizing: 'border-box',
              '&:hover': {
                bgcolor: selectedCrewIds.includes(crew.id) ? 'primary.light' : 'grey.100'
              }
            }}
          >
            <Typography variant="subtitle1">{crew.name}</Typography>
            <Typography variant="body2" color="text.secondary">
              {`ID: ${crew.id}`}
            </Typography>
          </Box>
        ))}
      </Box>
    </Box>
  );
};

export default CrewSelectionStep; 