import React from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Alert
} from '@mui/material';
import SmartToyIcon from '@mui/icons-material/SmartToy';

function DSPyConfiguration(): JSX.Element {
  return (
    <Box>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={3}>
        <Typography variant="h6" sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <SmartToyIcon />
          DSPy Prompt Optimization
        </Typography>
      </Box>

      {/* Disabled State Card */}
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Box>
            <Typography variant="h6" gutterBottom>
              DSPy Configuration
            </Typography>
            <Typography variant="body2" color="textSecondary" sx={{ mb: 2 }}>
              DSPy automatically optimizes prompts for your AI agents using production traces.
              This feature is currently disabled for development.
            </Typography>

            <Alert severity="info">
              <Typography variant="body2">
                DSPy optimization is temporarily disabled. This feature will be re-enabled in a future update
                to provide automatic prompt optimization and continuous learning from production usage.
              </Typography>
            </Alert>
          </Box>
        </CardContent>
      </Card>

    </Box>
  );
}

export default DSPyConfiguration;