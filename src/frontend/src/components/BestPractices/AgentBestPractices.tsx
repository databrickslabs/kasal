import React from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Typography,
  Box,
  Paper,
  IconButton,
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
  Alert,
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import LightbulbIcon from '@mui/icons-material/Lightbulb';
import WarningIcon from '@mui/icons-material/Warning';
import PsychologyIcon from '@mui/icons-material/Psychology';
import GroupWorkIcon from '@mui/icons-material/GroupWork';
import SettingsIcon from '@mui/icons-material/Settings';

interface AgentBestPracticesProps {
  open: boolean;
  onClose: () => void;
}

const AgentBestPractices: React.FC<AgentBestPracticesProps> = ({ open, onClose }) => {
  return (
    <Dialog
      open={open}
      onClose={onClose}
      maxWidth="md"
      fullWidth
      PaperProps={{
        sx: {
          borderRadius: 2,
          minHeight: '80vh',
        },
      }}
    >
      <DialogTitle>
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <PsychologyIcon color="primary" />
            <Typography variant="h6">Agent Best Practices</Typography>
          </Box>
          <IconButton onClick={onClose} size="small">
            <CloseIcon />
          </IconButton>
        </Box>
      </DialogTitle>
      
      <DialogContent dividers>
        <Box sx={{ py: 2 }}>
          {/* Introduction */}
          <Alert severity="info" sx={{ mb: 3 }}>
            Agents are autonomous units that perform specific tasks. They combine a role, goal, backstory, 
            and tools to accomplish their objectives effectively.
          </Alert>

          {/* Core Principles */}
          <Paper elevation={0} sx={{ p: 3, mb: 3, bgcolor: 'background.default' }}>
            <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <LightbulbIcon color="primary" />
              Core Principles
            </Typography>
            <List>
              <ListItem>
                <ListItemIcon>
                  <CheckCircleIcon color="success" />
                </ListItemIcon>
                <ListItemText
                  primary="Single Responsibility"
                  secondary="Each agent should focus on one specific area of expertise. Avoid creating agents that try to do everything."
                />
              </ListItem>
              <ListItem>
                <ListItemIcon>
                  <CheckCircleIcon color="success" />
                </ListItemIcon>
                <ListItemText
                  primary="Clear Role Definition"
                  secondary="Define the agent's role precisely. For example: 'Senior Data Analyst' instead of just 'Analyst'."
                />
              </ListItem>
              <ListItem>
                <ListItemIcon>
                  <CheckCircleIcon color="success" />
                </ListItemIcon>
                <ListItemText
                  primary="Actionable Goals"
                  secondary="Set specific, measurable goals that the agent can achieve. Avoid vague objectives."
                />
              </ListItem>
            </List>
          </Paper>

          {/* Writing Effective Agent Descriptions */}
          <Paper elevation={0} sx={{ p: 3, mb: 3, bgcolor: 'background.default' }}>
            <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <SettingsIcon color="primary" />
              Writing Effective Agent Descriptions
            </Typography>
            
            <Box sx={{ mt: 2 }}>
              <Typography variant="subtitle1" fontWeight="medium" gutterBottom>
                Role
              </Typography>
              <Typography variant="body2" color="text.secondary" paragraph>
                Define WHO the agent is. Be specific about their expertise level and domain.
              </Typography>
              <Box sx={{ bgcolor: 'grey.100', p: 2, borderRadius: 1, mb: 2 }}>
                <Typography variant="body2" fontFamily="monospace">
                  ✅ Good: {'"Senior Python Developer specializing in API development"'}<br />
                  ❌ Poor: {'"Developer"'}
                </Typography>
              </Box>
            </Box>

            <Box sx={{ mt: 2 }}>
              <Typography variant="subtitle1" fontWeight="medium" gutterBottom>
                Goal
              </Typography>
              <Typography variant="body2" color="text.secondary" paragraph>
                Define WHAT the agent should achieve. Make it specific and measurable.
              </Typography>
              <Box sx={{ bgcolor: 'grey.100', p: 2, borderRadius: 1, mb: 2 }}>
                <Typography variant="body2" fontFamily="monospace">
                  ✅ Good: {'"Design and implement RESTful APIs with proper error handling and documentation"'}<br />
                  ❌ Poor: {'"Write code"'}
                </Typography>
              </Box>
            </Box>

            <Box sx={{ mt: 2 }}>
              <Typography variant="subtitle1" fontWeight="medium" gutterBottom>
                Backstory
              </Typography>
              <Typography variant="body2" color="text.secondary" paragraph>
                Provide context that helps the agent understand their approach and constraints.
              </Typography>
              <Box sx={{ bgcolor: 'grey.100', p: 2, borderRadius: 1, mb: 2 }}>
                <Typography variant="body2" fontFamily="monospace">
                  ✅ Good: {'"You have 10 years of experience building scalable systems. You prioritize clean, maintainable code and always consider security implications."'}<br />
                  ❌ Poor: {'"You are experienced"'}
                </Typography>
              </Box>
            </Box>
          </Paper>

          {/* Tool Selection */}
          <Paper elevation={0} sx={{ p: 3, mb: 3, bgcolor: 'background.default' }}>
            <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <GroupWorkIcon color="primary" />
              Tool Selection
            </Typography>
            <List>
              <ListItem>
                <ListItemIcon>
                  <CheckCircleIcon color="success" />
                </ListItemIcon>
                <ListItemText
                  primary="Minimal Tool Set"
                  secondary="Only assign tools that are essential for the agent's tasks. Too many tools can confuse the agent."
                />
              </ListItem>
              <ListItem>
                <ListItemIcon>
                  <CheckCircleIcon color="success" />
                </ListItemIcon>
                <ListItemText
                  primary="Tool Relevance"
                  secondary="Ensure each tool directly relates to the agent's goal and role."
                />
              </ListItem>
              <ListItem>
                <ListItemIcon>
                  <CheckCircleIcon color="success" />
                </ListItemIcon>
                <ListItemText
                  primary="Custom Tools"
                  secondary="Create custom tools for specific needs rather than overloading agents with generic ones."
                />
              </ListItem>
            </List>
          </Paper>

          {/* Common Pitfalls */}
          <Paper elevation={0} sx={{ p: 3, mb: 3, bgcolor: 'error.50' }}>
            <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center', gap: 1, color: 'error.main' }}>
              <WarningIcon color="error" />
              Common Pitfalls to Avoid
            </Typography>
            <List>
              <ListItem>
                <ListItemIcon>
                  <WarningIcon color="error" />
                </ListItemIcon>
                <ListItemText
                  primary="Overlapping Responsibilities"
                  secondary="Don't create agents with overlapping roles. This causes confusion and inefficiency."
                />
              </ListItem>
              <ListItem>
                <ListItemIcon>
                  <WarningIcon color="error" />
                </ListItemIcon>
                <ListItemText
                  primary="Vague Instructions"
                  secondary="Avoid ambiguous language in roles and goals. Be explicit about expectations."
                />
              </ListItem>
              <ListItem>
                <ListItemIcon>
                  <WarningIcon color="error" />
                </ListItemIcon>
                <ListItemText
                  primary="Over-complexity"
                  secondary="Don't make agents too complex. Break down complex roles into multiple specialized agents."
                />
              </ListItem>
              <ListItem>
                <ListItemIcon>
                  <WarningIcon color="error" />
                </ListItemIcon>
                <ListItemText
                  primary="Ignoring Dependencies"
                  secondary="Consider how agents will interact. Define clear handoff points and communication patterns."
                />
              </ListItem>
            </List>
          </Paper>

          {/* Best Practices Checklist */}
          <Paper elevation={0} sx={{ p: 3, bgcolor: 'success.50' }}>
            <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center', gap: 1, color: 'success.main' }}>
              <CheckCircleIcon color="success" />
              Quick Checklist
            </Typography>
            <List dense>
              <ListItem>
                <ListItemIcon>
                  <CheckCircleIcon color="success" fontSize="small" />
                </ListItemIcon>
                <ListItemText primary="Is the role specific and clear?" />
              </ListItem>
              <ListItem>
                <ListItemIcon>
                  <CheckCircleIcon color="success" fontSize="small" />
                </ListItemIcon>
                <ListItemText primary="Is the goal measurable and achievable?" />
              </ListItem>
              <ListItem>
                <ListItemIcon>
                  <CheckCircleIcon color="success" fontSize="small" />
                </ListItemIcon>
                <ListItemText primary="Does the backstory provide useful context?" />
              </ListItem>
              <ListItem>
                <ListItemIcon>
                  <CheckCircleIcon color="success" fontSize="small" />
                </ListItemIcon>
                <ListItemText primary="Are all assigned tools necessary?" />
              </ListItem>
              <ListItem>
                <ListItemIcon>
                  <CheckCircleIcon color="success" fontSize="small" />
                </ListItemIcon>
                <ListItemText primary="Is the agent focused on a single responsibility?" />
              </ListItem>
              <ListItem>
                <ListItemIcon>
                  <CheckCircleIcon color="success" fontSize="small" />
                </ListItemIcon>
                <ListItemText primary="Have you avoided overlapping with other agents?" />
              </ListItem>
            </List>
          </Paper>
        </Box>
      </DialogContent>

      <DialogActions sx={{ px: 3, py: 2 }}>
        <Button onClick={onClose} variant="contained" color="primary">
          Close
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default AgentBestPractices;