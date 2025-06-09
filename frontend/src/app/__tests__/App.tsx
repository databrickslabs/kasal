import { useEffect } from 'react';
import { Box } from '@mui/material';
import { Routes, Route, Navigate } from 'react-router-dom';
import { Toaster } from 'react-hot-toast';
import ThemeProvider from '../../config/theme/ThemeProvider';
import RunHistory from '../../components/Jobs/ExecutionHistory';
import WorkflowDesigner from '../../components/WorkflowDesigner';
import ToolForm from '../../components/Tools/ToolForm';
import ShortcutsCircle from '../../components/ShortcutsCircle';
import { LanguageService } from '../../api/LanguageService';
import { WorkflowTest } from '../../components/WorkflowTest';
import '../../config/i18n/config';

function App() {
  useEffect(() => {
    const initialize = async () => {
      // Initialize language
      const languageService = LanguageService.getInstance();
      await languageService.initializeLanguage();
    };

    initialize();
  }, []);

  return (
    <ThemeProvider>
      <Toaster 
        position="top-center"
        toastOptions={{
          duration: 6000,
          style: {
            maxWidth: '500px',
          },
        }}
      />
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
        </Routes>
      </Box>
    </ThemeProvider>
  );
}

export default App;