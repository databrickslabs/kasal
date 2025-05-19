import React, { useState, lazy, Suspense, useEffect } from 'react';
import { Box, CircularProgress } from '@mui/material';
import { Routes, Route, Navigate } from 'react-router-dom';
import ThemeProvider from '../../config/theme/ThemeProvider';
import RunHistory from '../../components/Jobs/ExecutionHistory';
import WorkflowDesigner from '../../components/WorkflowDesigner';
import ToolForm from '../../components/Tools/ToolForm';
import { UCToolsService, UCTool } from '../../api';
import ShortcutsCircle from '../../components/ShortcutsCircle';
import { LanguageService } from '../../api/LanguageService';
import { DatabricksService } from '../../api/DatabricksService';
import { WorkflowTest } from '../../components/WorkflowTest';
import '../../config/i18n/config';

// Lazy load components
const UCTools = lazy(() => import('../../components/Tools/UCTools'));

// Loading component
const LoadingComponent = () => (
  <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100vh' }}>
    <CircularProgress />
  </Box>
);

function App() {
  // UC Tools state
  const [ucTools, setUCTools] = useState<UCTool[]>([]);
  const [ucToolsLoading, setUCToolsLoading] = useState(true);
  const [ucToolsError, setUCToolsError] = useState<string | null>(null);

  useEffect(() => {
    const initialize = async () => {
      // Initialize language
      const languageService = LanguageService.getInstance();
      await languageService.initializeLanguage();

      // Initialize UC Tools
      try {
        const ucToolsService = UCToolsService.getInstance();
        const databricksService = DatabricksService.getInstance();
        
        // First check if Databricks is enabled
        const databricksConfig = await databricksService.getDatabricksConfig();
        
        if (databricksConfig && databricksConfig.enabled) {
          const toolsData = await ucToolsService.getUCTools();
          setUCTools(toolsData);
          setUCToolsError(null);
        } else {
          setUCTools([]);
          setUCToolsError('Databricks integration is disabled. Please enable it in the Configuration page.');
        }
      } catch (error) {
        console.error('Error fetching UC tools:', error);
        setUCToolsError(error instanceof Error ? error.message : 'An unexpected error occurred');
      } finally {
        setUCToolsLoading(false);
      }
    };

    initialize();
  }, []);

  return (
    <ThemeProvider>
      <ShortcutsCircle />
      <Box
        component="main"
        sx={{
          flexGrow: 1,
          width: '100%',
          height: '100vh',
          overflow: 'hidden'
        }}
      >
        <Routes>
          <Route path="/" element={<Navigate to="/workflow" replace />} />
          <Route path="/workflow" element={<WorkflowDesigner />} />
          <Route path="/nemo" element={<Navigate to="/workflow" replace />} />
          <Route path="/runs" element={<RunHistory />} />
          <Route path="/tools" element={<ToolForm />} />
          <Route path="/workflow-test" element={<WorkflowTest />} />
          <Route 
            path="/uc-tools" 
            element={
              <Suspense fallback={<LoadingComponent />}>
                <UCTools 
                  tools={ucTools}
                  loading={ucToolsLoading}
                  error={ucToolsError}
                />
              </Suspense>
            } 
          />
        </Routes>
      </Box>
    </ThemeProvider>
  );
}

export default App; 