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
import TaskIcon from '@mui/icons-material/Task';
import TimelineIcon from '@mui/icons-material/Timeline';
import SettingsIcon from '@mui/icons-material/Settings';

interface TaskBestPracticesProps {
  open: boolean;
  onClose: () => void;
}

const TaskBestPractices: React.FC<TaskBestPracticesProps> = ({ open, onClose }) => {
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
            <TaskIcon color="primary" />
            <Typography variant="h6">Task Best Practices</Typography>
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
            Tasks are specific assignments given to agents. They define what needs to be done, 
            the expected output, and how the task connects to the overall workflow.
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
                  primary="Clear and Specific"
                  secondary="Tasks should have unambiguous instructions with clear deliverables."
                />
              </ListItem>
              <ListItem>
                <ListItemIcon>
                  <CheckCircleIcon color="success" />
                </ListItemIcon>
                <ListItemText
                  primary="Atomic Operations"
                  secondary="Break complex operations into smaller, manageable tasks. Each task should do one thing well."
                />
              </ListItem>
              <ListItem>
                <ListItemIcon>
                  <CheckCircleIcon color="success" />
                </ListItemIcon>
                <ListItemText
                  primary="Measurable Outcomes"
                  secondary="Define success criteria that can be objectively evaluated."
                />
              </ListItem>
            </List>
          </Paper>

          {/* Writing Effective Task Descriptions */}
          <Paper elevation={0} sx={{ p: 3, mb: 3, bgcolor: 'background.default' }}>
            <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <SettingsIcon color="primary" />
              Writing Effective Task Descriptions
            </Typography>
            
            <Box sx={{ mt: 2 }}>
              <Typography variant="subtitle1" fontWeight="medium" gutterBottom>
                Description
              </Typography>
              <Typography variant="body2" color="text.secondary" paragraph>
                Provide clear, actionable instructions. Include context, constraints, and specific requirements.
              </Typography>
              <Box sx={{ bgcolor: 'grey.100', p: 2, borderRadius: 1, mb: 2 }}>
                <Typography variant="body2" fontFamily="monospace">
                  ✅ Good: {'"Analyze the customer feedback data from the last quarter. Identify the top 3 pain points mentioned by users. Create a summary report with specific quotes and frequency data."'}<br /><br />
                  ❌ Poor: {'"Look at customer feedback"'}
                </Typography>
              </Box>
            </Box>

            <Box sx={{ mt: 2 }}>
              <Typography variant="subtitle1" fontWeight="medium" gutterBottom>
                Expected Output
              </Typography>
              <Typography variant="body2" color="text.secondary" paragraph>
                Define exactly what the task should produce. Specify format, structure, and content requirements.
              </Typography>
              <Box sx={{ bgcolor: 'grey.100', p: 2, borderRadius: 1, mb: 2 }}>
                <Typography variant="body2" fontFamily="monospace">
                  ✅ Good: {'"A JSON object containing: analysis_date, top_issues (array of 3 items with title, description, frequency), recommendations (array), and raw_data_summary"'}<br /><br />
                  ❌ Poor: {'"A report"'}
                </Typography>
              </Box>
            </Box>

            <Box sx={{ mt: 2 }}>
              <Typography variant="subtitle1" fontWeight="medium" gutterBottom>
                Context Variables
              </Typography>
              <Typography variant="body2" color="text.secondary" paragraph>
                Use context variables to pass dynamic information from previous tasks or user input.
              </Typography>
              <Box sx={{ bgcolor: 'grey.100', p: 2, borderRadius: 1, mb: 2 }}>
                <Typography variant="body2" fontFamily="monospace">
                  Example: {'"Analyze the data for {customer_segment} in {region} market"'}<br />
                  This allows the task to be reusable with different parameters.
                </Typography>
              </Box>
            </Box>
          </Paper>

          {/* Task Dependencies */}
          <Paper elevation={0} sx={{ p: 3, mb: 3, bgcolor: 'background.default' }}>
            <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <TimelineIcon color="primary" />
              Task Dependencies and Flow
            </Typography>
            <List>
              <ListItem>
                <ListItemIcon>
                  <CheckCircleIcon color="success" />
                </ListItemIcon>
                <ListItemText
                  primary="Sequential Dependencies"
                  secondary="Clearly define which tasks must complete before this task can start."
                />
              </ListItem>
              <ListItem>
                <ListItemIcon>
                  <CheckCircleIcon color="success" />
                </ListItemIcon>
                <ListItemText
                  primary="Input/Output Mapping"
                  secondary="Ensure outputs from previous tasks match the expected inputs of dependent tasks."
                />
              </ListItem>
              <ListItem>
                <ListItemIcon>
                  <CheckCircleIcon color="success" />
                </ListItemIcon>
                <ListItemText
                  primary="Error Handling"
                  secondary="Define what should happen if a task fails or produces unexpected output."
                />
              </ListItem>
              <ListItem>
                <ListItemIcon>
                  <CheckCircleIcon color="success" />
                </ListItemIcon>
                <ListItemText
                  primary="Parallel Execution"
                  secondary="Identify tasks that can run simultaneously to optimize workflow performance."
                />
              </ListItem>
            </List>
          </Paper>

          {/* Output Types */}
          <Paper elevation={0} sx={{ p: 3, mb: 3, bgcolor: 'background.default' }}>
            <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <SettingsIcon color="primary" />
              Output Types and Formats
            </Typography>
            <List>
              <ListItem>
                <ListItemIcon>
                  <CheckCircleIcon color="info" />
                </ListItemIcon>
                <ListItemText
                  primary="Structured Data (JSON/XML)"
                  secondary="Best for data that will be processed by other tasks or systems."
                />
              </ListItem>
              <ListItem>
                <ListItemIcon>
                  <CheckCircleIcon color="info" />
                </ListItemIcon>
                <ListItemText
                  primary="Plain Text"
                  secondary="Suitable for summaries, reports, or human-readable content."
                />
              </ListItem>
              <ListItem>
                <ListItemIcon>
                  <CheckCircleIcon color="info" />
                </ListItemIcon>
                <ListItemText
                  primary="Markdown"
                  secondary="Ideal for formatted documentation or reports with structure."
                />
              </ListItem>
              <ListItem>
                <ListItemIcon>
                  <CheckCircleIcon color="info" />
                </ListItemIcon>
                <ListItemText
                  primary="Code"
                  secondary="For generated scripts, configurations, or program files."
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
                  primary="Overly Complex Tasks"
                  secondary="Don't try to accomplish too much in a single task. Break it down into smaller steps."
                />
              </ListItem>
              <ListItem>
                <ListItemIcon>
                  <WarningIcon color="error" />
                </ListItemIcon>
                <ListItemText
                  primary="Ambiguous Success Criteria"
                  secondary="Avoid vague outcomes like 'improve the process' without defining what improvement means."
                />
              </ListItem>
              <ListItem>
                <ListItemIcon>
                  <WarningIcon color="error" />
                </ListItemIcon>
                <ListItemText
                  primary="Missing Context"
                  secondary="Don't assume the agent knows background information. Provide all necessary context."
                />
              </ListItem>
              <ListItem>
                <ListItemIcon>
                  <WarningIcon color="error" />
                </ListItemIcon>
                <ListItemText
                  primary="Ignoring Edge Cases"
                  secondary="Consider what happens with empty inputs, errors, or unexpected data formats."
                />
              </ListItem>
              <ListItem>
                <ListItemIcon>
                  <WarningIcon color="error" />
                </ListItemIcon>
                <ListItemText
                  primary="Circular Dependencies"
                  secondary="Ensure tasks don't create dependency loops that prevent execution."
                />
              </ListItem>
            </List>
          </Paper>

          {/* Task Templates */}
          <Paper elevation={0} sx={{ p: 3, mb: 3, bgcolor: 'info.50' }}>
            <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center', gap: 1, color: 'info.main' }}>
              <LightbulbIcon color="info" />
              Task Template Examples
            </Typography>
            
            <Box sx={{ mt: 2 }}>
              <Typography variant="subtitle2" fontWeight="medium" gutterBottom>
                Data Analysis Task
              </Typography>
              <Box sx={{ bgcolor: 'white', p: 2, borderRadius: 1, mb: 2, fontFamily: 'monospace' }}>
                <Typography variant="body2" component="pre" sx={{ whiteSpace: 'pre-wrap' }}>
{`Description: "Analyze sales data for Q4 2024. Calculate total revenue, 
top 5 products by sales volume, and month-over-month growth rate."

Expected Output: "JSON object with structure:
{
  quarter: 'Q4 2024',
  total_revenue: number,
  top_products: [{name, units_sold, revenue}],
  growth_rate: number,
  summary: string
}"`}
                </Typography>
              </Box>
            </Box>

            <Box sx={{ mt: 2 }}>
              <Typography variant="subtitle2" fontWeight="medium" gutterBottom>
                Content Generation Task
              </Typography>
              <Box sx={{ bgcolor: 'white', p: 2, borderRadius: 1, mb: 2, fontFamily: 'monospace' }}>
                <Typography variant="body2" component="pre" sx={{ whiteSpace: 'pre-wrap' }}>
{`Description: "Create a technical blog post about {topic}. 
Include an introduction, 3 main sections with examples, 
and a conclusion with key takeaways."

Expected Output: "Markdown formatted article with:
- Title (H1)
- Introduction (150-200 words)
- 3 sections with code examples
- Conclusion with bullet points
- Total length: 1500-2000 words"`}
                </Typography>
              </Box>
            </Box>
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
                <ListItemText primary="Is the task description clear and specific?" />
              </ListItem>
              <ListItem>
                <ListItemIcon>
                  <CheckCircleIcon color="success" fontSize="small" />
                </ListItemIcon>
                <ListItemText primary="Is the expected output well-defined?" />
              </ListItem>
              <ListItem>
                <ListItemIcon>
                  <CheckCircleIcon color="success" fontSize="small" />
                </ListItemIcon>
                <ListItemText primary="Are all dependencies identified?" />
              </ListItem>
              <ListItem>
                <ListItemIcon>
                  <CheckCircleIcon color="success" fontSize="small" />
                </ListItemIcon>
                <ListItemText primary="Is the task appropriately sized (not too complex)?" />
              </ListItem>
              <ListItem>
                <ListItemIcon>
                  <CheckCircleIcon color="success" fontSize="small" />
                </ListItemIcon>
                <ListItemText primary="Have you provided necessary context?" />
              </ListItem>
              <ListItem>
                <ListItemIcon>
                  <CheckCircleIcon color="success" fontSize="small" />
                </ListItemIcon>
                <ListItemText primary="Is the output format specified?" />
              </ListItem>
              <ListItem>
                <ListItemIcon>
                  <CheckCircleIcon color="success" fontSize="small" />
                </ListItemIcon>
                <ListItemText primary="Have you considered error scenarios?" />
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

export default TaskBestPractices;