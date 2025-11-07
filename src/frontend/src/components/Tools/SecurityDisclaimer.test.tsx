import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import { ThemeProvider } from '@mui/material/styles';
import { createTheme } from '@mui/material/styles';
import SecurityDisclaimer, { TOOL_SECURITY_INFO } from './SecurityDisclaimer';
import type { Tool } from '../../types/tool';

const theme = createTheme();

describe('SecurityDisclaimer', () => {
  const mockOnClose = jest.fn();
  const mockOnConfirm = jest.fn();

  const renderComponent = (tool: Tool | null = null) => {
    return render(
      <ThemeProvider theme={theme}>
        <SecurityDisclaimer
          open={true}
          onClose={mockOnClose}
          onConfirm={mockOnConfirm}
          tool={tool}
        />
      </ThemeProvider>
    );
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('should render DatabricksJobsTool security information', () => {
    const databricksJobsTool: Tool = {
      id: '70',
      title: 'DatabricksJobsTool',
      description: 'Test description',
      icon: 'database',
      config: {},
      category: 'Custom',
    };

    renderComponent(databricksJobsTool);

    // Check that the security information is displayed
    expect(screen.getByText(/DatabricksJobsTool/)).toBeInTheDocument();
    expect(screen.getByText(/Manages Databricks Jobs/)).toBeInTheDocument();
    expect(screen.getByText(/HIGH/)).toBeInTheDocument(); // Risk level
  });

  it('should display correct risks for DatabricksJobsTool', () => {
    const databricksJobsTool: Tool = {
      id: '70',
      title: 'DatabricksJobsTool',
      description: 'Test description',
      icon: 'database',
      config: {},
      category: 'Custom',
    };

    renderComponent(databricksJobsTool);

    // Check specific risks are displayed
    expect(screen.getByText(/Creation of arbitrary compute jobs/)).toBeInTheDocument();
    expect(screen.getByText(/Resource consumption through job execution/)).toBeInTheDocument();
    expect(screen.getByText(/Access to job configurations and outputs/)).toBeInTheDocument();
  });

  it('should display correct mitigations for DatabricksJobsTool', () => {
    const databricksJobsTool: Tool = {
      id: '70',
      title: 'DatabricksJobsTool',
      description: 'Test description',
      icon: 'database',
      config: {},
      category: 'Custom',
    };

    renderComponent(databricksJobsTool);

    // Toggle to show mitigations
    const showMitigationsButton = screen.getByText(/Show Recommended Mitigations/);
    fireEvent.click(showMitigationsButton);

    // Check specific mitigations are displayed
    expect(screen.getByText(/Validate job configurations before creation/)).toBeInTheDocument();
    expect(screen.getByText(/Implement job resource limits and quotas/)).toBeInTheDocument();
  });

  it('should show single-tenant information for DatabricksJobsTool', () => {
    const databricksJobsTool: Tool = {
      id: '70',
      title: 'DatabricksJobsTool',
      description: 'Test description',
      icon: 'database',
      config: {},
      category: 'Custom',
    };

    renderComponent(databricksJobsTool);

    // Check single-tenant specific information
    expect(screen.getByText(/Single-tenant with Databricks OBO security model/)).toBeInTheDocument();
    expect(screen.getByText(/MEDIUM/)).toBeInTheDocument(); // Single-tenant risk level
  });

  it('should include DatabricksJobsTool in TOOL_SECURITY_INFO', () => {
    expect(TOOL_SECURITY_INFO).toHaveProperty('DatabricksJobsTool');

    const jobsToolInfo = TOOL_SECURITY_INFO['DatabricksJobsTool'];
    expect(jobsToolInfo.riskLevel).toBe('HIGH');
    expect(jobsToolInfo.singleTenantRiskLevel).toBe('MEDIUM');
    expect(jobsToolInfo.description).toContain('Manages Databricks Jobs');
    expect(jobsToolInfo.deploymentContext).toContain('Databricks OBO security model');
  });
});