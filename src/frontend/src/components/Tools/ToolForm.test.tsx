import { vi, beforeEach, describe, it, expect } from 'vitest';
import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { ThemeProvider } from '@mui/material/styles';
import { createTheme } from '@mui/material/styles';
import ToolForm, { customTools } from './ToolForm';

// Mock the translation hook
vi.mock('react-i18next', () => ({
  useTranslation: () => ({
    t: (key: string) => key,
    i18n: {
      changeLanguage: () => Promise.resolve(),
    },
  }),
}));

// Mock permission store
vi.mock('../../store/permissions', () => ({
  usePermissionStore: vi.fn((selector) => {
    const state = {
      userRole: 'admin',
      isLoading: false,
    };
    return selector ? selector(state) : state;
  }),
}));

// Mock the ToolService
vi.mock('../../api/ToolService', () => ({
  ToolService: {
    listTools: vi.fn().mockResolvedValue([
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
    updateTool: vi.fn().mockResolvedValue({}),
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
    vi.clearAllMocks();
  });

  it('should render without crashing', async () => {
    renderComponent();

    await waitFor(() => {
      expect(screen.getByText('tools.regular.title')).toBeInTheDocument();
    });
  });

  it('should include all custom tools in the customTools array', () => {
    expect(customTools).toContain('GenieTool');
    expect(customTools).toContain('PerplexityTool');
    expect(customTools).toContain('DatabricksJobsTool');
  });

  it('should show prebuilt tab by default', async () => {
    renderComponent();

    await waitFor(() => {
      expect(screen.getByText('tools.regular.tabs.prebuilt')).toBeInTheDocument();
    });
  });
});
