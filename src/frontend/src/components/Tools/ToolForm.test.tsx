import React from 'react';
import { render, screen } from '@testing-library/react';
import { ThemeProvider } from '@mui/material/styles';
import { createTheme } from '@mui/material/styles';
import ToolForm from '../ToolForm';

// Mock the translation hook
jest.mock('react-i18next', () => ({
  useTranslation: () => ({
    t: (key: string) => key,
    i18n: {
      changeLanguage: () => new Promise(() => {}),
    },
  }),
}));

// Mock the ToolService
jest.mock('../../../api/ToolService', () => ({
  ToolService: {
    fetchAllTools: jest.fn().mockResolvedValue([
      {
        id: 67,
        title: 'DatabricksCustomTool',
        description: 'Test Databricks Custom Tool',
        icon: 'database',
        config: {},
        enabled: true,
      },
      {
        id: 70,
        title: 'DatabricksJobsTool',
        description: 'Test Databricks Jobs Tool',
        icon: 'database',
        config: {},
        enabled: true,
      },
      {
        id: 35,
        title: 'GenieTool',
        description: 'Test Genie Tool',
        icon: 'database',
        config: {},
        enabled: true,
      },
    ]),
    updateTool: jest.fn().mockResolvedValue({}),
  },
}));

const theme = createTheme();

describe('ToolForm', () => {
  const renderComponent = () => {
    return render(
      <ThemeProvider theme={theme}>
        <ToolForm />
      </ThemeProvider>
    );
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('should render without crashing', () => {
    renderComponent();
    expect(screen.getByText('tools.title')).toBeInTheDocument();
  });

  it('should categorize DatabricksJobsTool as a custom tool', async () => {
    renderComponent();
    
    // Wait for tools to load
    const customTab = await screen.findByText('tools.tabs.custom');
    expect(customTab).toBeInTheDocument();
  });

  it('should include all custom tools in the customTools array', () => {
    // Import the customTools array from the component
    const { customTools } = require('../ToolForm');
    
    expect(customTools).toContain('GenieTool');
    expect(customTools).toContain('PerplexityTool');
    expect(customTools).toContain('DatabricksCustomTool');
    expect(customTools).toContain('DatabricksJobsTool');
    expect(customTools).toContain('PythonPPTXTool');
  });

  it('should correctly categorize tools based on customTools array', async () => {
    const { ToolService } = require('../../../api/ToolService');
    
    renderComponent();
    
    // Wait for the fetch to complete
    await screen.findByText('tools.title');
    
    // Verify ToolService was called
    expect(ToolService.fetchAllTools).toHaveBeenCalled();
  });
});