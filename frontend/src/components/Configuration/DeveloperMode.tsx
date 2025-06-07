import React, { useState, useEffect } from 'react';
import {
  Box,
  Typography,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Alert,
  Card,
  CardContent,
  Chip,
  Button,
  TextField,
} from '@mui/material';
import {
  Code as CodeIcon,
  Person as PersonIcon,
} from '@mui/icons-material';

interface MockUser {
  email: string;
  name: string;
  group: string;
  color: 'primary' | 'secondary' | 'success' | 'warning' | 'error';
}

const MOCK_USERS: MockUser[] = [
  {
    email: 'alice@acme-corp.com',
    name: 'Alice (Acme Corp)',
    group: 'acme_corp_com',
    color: 'primary'
  },
  {
    email: 'bob@tech-startup.io',
    name: 'Bob (Tech Startup)',
    group: 'tech_startup_io',
    color: 'secondary'
  },
  {
    email: 'charlie@big-enterprise.com',
    name: 'Charlie (Big Enterprise)',
    group: 'big_enterprise_com',
    color: 'success'
  },
  {
    email: 'diana@small-business.net',
    name: 'Diana (Small Business)',
    group: 'small_business_net',
    color: 'warning'
  },
];

const DeveloperMode: React.FC = () => {
  const [currentUser, setCurrentUser] = useState<string>('');
  const [customEmail, setCustomEmail] = useState<string>('');
  const [showCustom, setShowCustom] = useState<boolean>(false);

  useEffect(() => {
    // Load current mock user from localStorage
    const mockUserEmail = localStorage.getItem('mockUserEmail');
    if (mockUserEmail) {
      setCurrentUser(mockUserEmail);
    }
  }, []);

  const handleUserChange = (email: string) => {
    setCurrentUser(email);
    localStorage.setItem('mockUserEmail', email);
    
    // Show notification
    console.log(`Switched to mock user: ${email}`);
    
    // Optionally reload the page to apply changes immediately
    // window.location.reload();
  };

  const handleCustomUserSubmit = () => {
    if (customEmail && customEmail.includes('@')) {
      handleUserChange(customEmail);
      setShowCustom(false);
      setCustomEmail('');
    }
  };

  const clearUser = () => {
    setCurrentUser('');
    localStorage.removeItem('mockUserEmail');
  };

  const getCurrentUserInfo = () => {
    const user = MOCK_USERS.find(u => u.email === currentUser);
    return user || { email: currentUser, name: 'Custom User', group: 'unknown', color: 'default' as const };
  };

  // Only show in development mode
  if (process.env.NODE_ENV !== 'development') {
    return null;
  }

  const currentUserInfo = getCurrentUserInfo();

  return (
    <Card sx={{ mb: 3, border: '2px dashed orange' }}>
      <CardContent>
        <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
          <CodeIcon sx={{ mr: 1, color: 'warning.main' }} />
          <Typography variant="h6" color="warning.main">
            Developer Mode - Mock User Testing
          </Typography>
        </Box>

        <Alert severity="warning" sx={{ mb: 2 }}>
          <Typography variant="body2">
            <strong>Development Only:</strong> This panel allows you to simulate different users 
            for testing tenant isolation. The selected user email will be sent as 
            <code style={{ background: '#f5f5f5', padding: '2px 4px', borderRadius: '4px' }}>
              X-Forwarded-Email
            </code> header with all API requests.
          </Typography>
        </Alert>

        {currentUser && (
          <Box sx={{ mb: 2 }}>
            <Typography variant="body2" gutterBottom>
              <strong>Current User:</strong>
            </Typography>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <PersonIcon fontSize="small" />
              <Chip 
                label={`${currentUserInfo.name} (${currentUserInfo.email})`}
                color={currentUserInfo.color}
                size="medium"
              />
              <Button size="small" onClick={clearUser} color="error">
                Clear
              </Button>
            </Box>
          </Box>
        )}

        <FormControl fullWidth sx={{ mb: 2 }}>
          <InputLabel>Select Mock User</InputLabel>
          <Select
            value={currentUser}
            label="Select Mock User"
            onChange={(e) => handleUserChange(e.target.value)}
          >
            <MenuItem value="">
              <em>No User (Headers not sent)</em>
            </MenuItem>
            {MOCK_USERS.map((user) => (
              <MenuItem key={user.email} value={user.email}>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                  <PersonIcon fontSize="small" />
                  {user.name} ({user.email})
                </Box>
              </MenuItem>
            ))}
            <MenuItem value="custom" onClick={() => setShowCustom(true)}>
              <em>Custom Email...</em>
            </MenuItem>
          </Select>
        </FormControl>

        {showCustom && (
          <Box sx={{ display: 'flex', gap: 1, mb: 2 }}>
            <TextField
              fullWidth
              size="small"
              label="Custom Email"
              value={customEmail}
              onChange={(e) => setCustomEmail(e.target.value)}
              placeholder="user@domain.com"
            />
            <Button 
              variant="contained" 
              size="small" 
              onClick={handleCustomUserSubmit}
              disabled={!customEmail.includes('@')}
            >
              Set
            </Button>
            <Button size="small" onClick={() => setShowCustom(false)}>
              Cancel
            </Button>
          </Box>
        )}

        <Typography variant="caption" display="block" sx={{ mt: 1, color: 'text.secondary' }}>
          💡 <strong>Testing Tip:</strong> Switch between users and create tenants, agents, or workflows 
          to verify that data is properly isolated between tenants.
        </Typography>
      </CardContent>
    </Card>
  );
};

export default DeveloperMode;