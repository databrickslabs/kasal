/**
 * Converter Page
 * Main page combining converter configuration and dashboard
 */

import React from 'react';
import { Box, Grid, Typography } from '@mui/material';
import { MeasureConverterConfig } from './MeasureConverterConfig';
import { ConverterDashboard } from './ConverterDashboard';

export const ConverterPage: React.FC = () => {
  return (
    <Box sx={{ p: 3 }}>
      <Typography variant="h4" gutterBottom>
        Measure Conversion Pipeline
      </Typography>
      <Typography variant="body1" color="text.secondary" paragraph>
        Convert measures between different formats (Power BI, YAML, DAX, SQL, Unity Catalog Metrics)
      </Typography>

      <Grid container spacing={3}>
        {/* Main Converter Form */}
        <Grid item xs={12} lg={6}>
          <MeasureConverterConfig />
        </Grid>

        {/* Dashboard */}
        <Grid item xs={12} lg={6}>
          <ConverterDashboard />
        </Grid>
      </Grid>
    </Box>
  );
};

export default ConverterPage;
