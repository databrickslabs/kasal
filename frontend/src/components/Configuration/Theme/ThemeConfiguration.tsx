import React from 'react';
import { 
  Box, 
  FormControl, 
  InputLabel, 
  Select, 
  MenuItem, 
  SelectChangeEvent,
  Typography,
  Card,
  CardContent,
  Grid,
  useTheme
} from '@mui/material';
import { useThemeStore } from '../../../store/theme';

const ThemeConfiguration: React.FC = () => {
  const { currentTheme, changeTheme } = useThemeStore();
  const theme = useTheme();

  const handleThemeChange = (event: SelectChangeEvent<string>) => {
    changeTheme(event.target.value);
  };

  const themeOptions = [
    { 
      value: 'professional', 
      name: 'Professional', 
      description: 'A clean, professional blue-based palette with high legibility and contrast',
      benefits: 'Great for dashboards, analytics, and business applications',
    },
    { 
      value: 'calmEarth', 
      name: 'Calm Earth', 
      description: 'A soothing, nature-inspired palette with green and earth tones',
      benefits: 'Perfect for environmental, wellness, and natural products apps',
    },
    { 
      value: 'deepOcean', 
      name: 'Deep Ocean', 
      description: 'A sophisticated dark palette with deep blue tones, reducing eye strain',
      benefits: 'Ideal for long work sessions and low-light environments',
    },
    { 
      value: 'vibrantCreative', 
      name: 'Vibrant Creative', 
      description: 'A bold, creative palette with purple and pink accents for visual impact',
      benefits: 'Great for creative tools, marketing applications, and engaging user experiences',
    }
  ];

  // Preview colors for each theme
  const themeColorPreviews = {
    professional: ['#1976d2', '#455a64', '#4caf50', '#f44336', '#ff9800'],
    calmEarth: ['#4caf50', '#795548', '#689f38', '#d32f2f', '#f57c00'],
    deepOcean: ['#0277bd', '#263238', '#00c853', '#ff3d00', '#ffd600'],
    vibrantCreative: ['#6200ea', '#ff4081', '#00bfa5', '#ff1744', '#ffab00']
  };

  // Theme UI elements for preview
  const previewUI = (themeName: string) => {
    const isSelected = currentTheme === themeName;
    const colors = themeColorPreviews[themeName as keyof typeof themeColorPreviews];
    
    return (
      <Box sx={{ mt: 1.5 }}>
        {/* Button row preview */}
        <Box sx={{ display: 'flex', gap: 1, mb: 2 }}>
          <Box sx={{ 
            bgcolor: colors[0], 
            color: 'white', 
            px: 1.5, 
            py: 0.5, 
            borderRadius: 1, 
            fontSize: '0.75rem',
            fontWeight: 'medium'
          }}>
            Primary
          </Box>
          <Box sx={{ 
            bgcolor: 'transparent', 
            color: colors[0], 
            border: `1px solid ${colors[0]}`,
            px: 1.5, 
            py: 0.5, 
            borderRadius: 1, 
            fontSize: '0.75rem',
            fontWeight: 'medium'
          }}>
            Secondary
          </Box>
        </Box>
        
        {/* UI element previews */}
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, mb: 1.5 }}>
          <Box sx={{ 
            width: 16, 
            height: 16, 
            borderRadius: '3px',
            border: `1px solid ${colors[1]}`,
            position: 'relative',
            ...(isSelected && {
              '&::after': {
                content: '""',
                position: 'absolute',
                width: 10,
                height: 10,
                top: '50%',
                left: '50%',
                transform: 'translate(-50%, -50%)',
                backgroundColor: colors[0],
                borderRadius: '2px'
              }
            })
          }} />
          <Box sx={{ 
            width: 16, 
            height: 16, 
            borderRadius: '50%',
            border: `1px solid ${colors[1]}`,
            position: 'relative',
            ...(isSelected && {
              '&::after': {
                content: '""',
                position: 'absolute',
                width: 10,
                height: 10,
                top: '50%',
                left: '50%',
                transform: 'translate(-50%, -50%)',
                backgroundColor: colors[0],
                borderRadius: '50%'
              }
            })
          }} />
          <Box sx={{ 
            width: 28, 
            height: 14, 
            borderRadius: '10px',
            backgroundColor: isSelected ? colors[0] : colors[1],
            position: 'relative',
            transition: 'all 0.3s',
            '&::after': {
              content: '""',
              position: 'absolute',
              width: 10,
              height: 10,
              top: '50%',
              transform: 'translateY(-50%)',
              left: isSelected ? 'calc(100% - 12px)' : '2px',
              backgroundColor: 'white',
              borderRadius: '50%',
              transition: 'all 0.3s'
            }
          }} />
        </Box>
        
        {/* Color chips */}
        <Box sx={{ display: 'flex', gap: 0.7, mb: 1 }}>
          {colors.map((color, index) => (
            <Box 
              key={index}
              sx={{ 
                width: 20, 
                height: 20, 
                borderRadius: '4px', 
                bgcolor: color,
                border: '1px solid rgba(0,0,0,0.1)'
              }} 
            />
          ))}
        </Box>
      </Box>
    );
  };

  return (
    <Box sx={{ p: 2 }}>
      <Typography variant="h6" gutterBottom>
        Application Theme
      </Typography>
      
      <FormControl fullWidth sx={{ mb: 4 }}>
        <InputLabel>Theme</InputLabel>
        <Select
          value={currentTheme}
          label="Theme"
          onChange={handleThemeChange}
        >
          {themeOptions.map(option => (
            <MenuItem key={option.value} value={option.value}>
              {option.name}
            </MenuItem>
          ))}
        </Select>
      </FormControl>

      <Typography variant="subtitle1" gutterBottom>
        Theme Preview
      </Typography>

      <Grid container spacing={2}>
        {themeOptions.map(themeOption => (
          <Grid item xs={12} sm={6} md={3} key={themeOption.value}>
            <Card 
              sx={{ 
                height: '100%',
                borderColor: currentTheme === themeOption.value 
                  ? theme.palette.primary.main 
                  : 'transparent',
                borderWidth: 2,
                borderStyle: 'solid',
                boxShadow: currentTheme === themeOption.value 
                  ? `0 0 8px ${theme.palette.primary.main}40` 
                  : undefined,
                transition: 'all 0.3s ease'
              }}
            >
              <CardContent>
                <Typography variant="subtitle1" fontWeight="medium">
                  {themeOption.name}
                </Typography>
                <Typography variant="body2" color="text.secondary" sx={{ mb: 0.5 }}>
                  {themeOption.description}
                </Typography>
                <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 1.5 }}>
                  {themeOption.benefits}
                </Typography>
                
                {previewUI(themeOption.value)}
              </CardContent>
            </Card>
          </Grid>
        ))}
      </Grid>
    </Box>
  );
};

export default ThemeConfiguration; 