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
import { Documentation } from '../../components/Documentation';
import DatabaseManagementService from '../../api/DatabaseManagementService';
import '../../config/i18n/config';

// Cache for Database Management permission to avoid repeated API calls
let databaseManagementPermissionCache: {
  hasPermission: boolean;
  checked: boolean;
} = {
  hasPermission: false,
  checked: false
};

// Export getter for the cache
export const getDatabaseManagementPermission = () => databaseManagementPermissionCache;

function App() {
  useEffect(() => {
    const initialize = async () => {
      // Initialize language
      const languageService = LanguageService.getInstance();
      await languageService.initializeLanguage();
      
      // Check Database Management permission early and cache it
      if (!databaseManagementPermissionCache.checked) {
        try {
          const permissionResult = await DatabaseManagementService.checkPermission();
          databaseManagementPermissionCache = {
            hasPermission: permissionResult.has_permission,
            checked: true
          };
          console.log('Database Management permission cached:', permissionResult.has_permission);
        } catch (error) {
          console.error('Failed to check database management permission:', error);
          // Default to true on error for backward compatibility
          databaseManagementPermissionCache = {
            hasPermission: true,
            checked: true
          };
        }
      }
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
          <Route path="/docs/*" element={<Documentation />} />
        </Routes>
      </Box>
    </ThemeProvider>
  );
}

export default App;