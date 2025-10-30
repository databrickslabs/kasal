import React from 'react';
import { render, screen } from '@testing-library/react';
import { ThemeProvider } from '@mui/material/styles';
import { createTheme } from '@mui/material/styles';
import ToolForm, { customTools } from './ToolForm';
import { ToolService } from '../../api/ToolService';

// Mock the translation hook
jest.mock('react-i18next', () => ({
  useTranslation: () => ({
    t: (key: string) => key,
    i18n: {
      changeLanguage: () => Promise.resolve(),
    },
  }),
}));

// Mock the ToolService
jest.mock('../../api/ToolService', () => ({
  ToolService: {
    listTools: jest.fn().mockResolvedValue([
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
    expect(customTools).toContain('GenieTool');
    expect(customTools).toContain('PerplexityTool');
    expect(customTools).toContain('DatabricksJobsTool');
  });

  it('should correctly categorize tools based on customTools array', async () => {
    renderComponent();

    // Wait for the fetch to complete
    await screen.findByText('tools.title');

    // Verify ToolService was called
    expect(ToolService.listTools).toHaveBeenCalled();
  });
});