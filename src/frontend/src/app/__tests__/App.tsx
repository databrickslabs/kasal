import { useEffect, lazy, Suspense } from 'react';
import { Box, CircularProgress } from '@mui/material';
import { Routes, Route, Navigate } from 'react-router-dom';
import { Toaster } from 'react-hot-toast';
import ThemeProvider from '../../config/theme/ThemeProvider';
import ShortcutsCircle from '../../components/ShortcutsCircle';
import { LanguageService } from '../../api/LanguageService';
import DatabaseManagementService from '../../api/DatabaseManagementService';
import { usePermissionLoader } from '../../hooks/usePermissions';
import '../../config/i18n/config';

// Lazy load heavy components to reduce initial bundle size
const RunHistory = lazy(() => import('../../components/Jobs/ExecutionHistory'));
const WorkflowDesigner = lazy(() => import('../../components/WorkflowDesigner'));
const ToolForm = lazy(() => import('../../components/Tools/ToolForm'));
const WorkflowTest = lazy(() => import('../../components/WorkflowTest').then(module => ({ default: module.WorkflowTest })));
const Documentation = lazy(() => import('../../components/Documentation').then(module => ({ default: module.Documentation })));

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
  // Load and maintain user permissions throughout the app
  usePermissionLoader();

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
        <Suspense
          fallback={
            <Box
              display="flex"
              justifyContent="center"
              alignItems="center"
              minHeight="100vh"
            >
              <CircularProgress />
            </Box>
          }
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
        </Suspense>
      </Box>
    </ThemeProvider>
  );
}

export default App;