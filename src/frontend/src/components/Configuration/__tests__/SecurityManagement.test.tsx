/**
 * Unit tests for SecurityManagement component.
 * 
 * Tests the functionality of the security management interface including
 * security policies, access controls, and audit features.
 */
import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { Provider } from 'react-redux';
import { ThemeProvider } from '@mui/material/styles';
import { BrowserRouter } from 'react-router-dom';
import configureStore from 'redux-mock-store';
import { vi, describe, it, expect, beforeEach, Mock } from 'vitest';

import SecurityManagement from '../SecurityManagement';
import * as SecurityService from '../../../api/SecurityService';
import theme from '../../../theme';

// Mock the SecurityService
vi.mock('../../../api/SecurityService');

const mockStore = configureStore([]);

const renderWithProviders = (component: React.ReactElement, initialState = {}) => {
  const store = mockStore({
    auth: {
      user: {
        id: '123',
        username: 'testuser',
        roles: ['admin'],
        is_superuser: true
      }
    },
    ...initialState
  });

  return render(
    <Provider store={store}>
      <BrowserRouter>
        <ThemeProvider theme={theme}>
          {component}
        </ThemeProvider>
      </BrowserRouter>
    </Provider>
  );
};

const mockSecurityPolicies = [
  {
    id: '1',
    name: 'Password Policy',
    description: 'Password strength and rotation requirements',
    type: 'password',
    enabled: true,
    settings: {
      min_length: 8,
      require_uppercase: true,
      require_lowercase: true,
      require_numbers: true,
      require_special_chars: true,
      max_age_days: 90
    },
    created_at: '2024-01-01T00:00:00Z'
  },
  {
    id: '2',
    name: 'Session Policy',
    description: 'User session management and timeout settings',
    type: 'session',
    enabled: true,
    settings: {
      timeout_minutes: 30,
      max_concurrent_sessions: 3,
      require_logout_confirmation: true
    },
    created_at: '2024-01-02T00:00:00Z'
  }
];

const mockAuditLogs = [
  {
    id: '1',
    user_id: 'user1',
    username: 'john.doe',
    action: 'login',
    resource: 'system',
    ip_address: '192.168.1.100',
    user_agent: 'Mozilla/5.0...',
    timestamp: '2024-01-01T10:00:00Z',
    status: 'success'
  },
  {
    id: '2',
    user_id: 'user2',
    username: 'jane.smith',
    action: 'delete_user',
    resource: 'user:user3',
    ip_address: '192.168.1.101',
    user_agent: 'Mozilla/5.0...',
    timestamp: '2024-01-01T11:00:00Z',
    status: 'success'
  }
];

const mockSecurityMetrics = {
  active_sessions: 25,
  failed_login_attempts_24h: 12,
  security_alerts: 3,
  password_expiring_soon: 8,
  inactive_users: 15,
  privileged_users: 5
};

describe('SecurityManagement', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    (SecurityService.getSecurityPolicies as Mock).mockResolvedValue(mockSecurityPolicies);
    (SecurityService.getAuditLogs as Mock).mockResolvedValue(mockAuditLogs);
    (SecurityService.getSecurityMetrics as Mock).mockResolvedValue(mockSecurityMetrics);
  });

  it('renders security management interface correctly', async () => {
    renderWithProviders(<SecurityManagement />);

    expect(screen.getByText('Security Management')).toBeInTheDocument();
    expect(screen.getByText('Security Policies')).toBeInTheDocument();
    expect(screen.getByText('Audit Logs')).toBeInTheDocument();
    expect(screen.getByText('Security Overview')).toBeInTheDocument();
  });

  it('displays security overview metrics', async () => {
    renderWithProviders(<SecurityManagement />);

    await waitFor(() => {
      expect(screen.getByText('25')).toBeInTheDocument(); // Active sessions
      expect(screen.getByText('12')).toBeInTheDocument(); // Failed login attempts
      expect(screen.getByText('3')).toBeInTheDocument(); // Security alerts
    });
  });

  it('displays security policies in table format', async () => {
    renderWithProviders(<SecurityManagement />);

    await waitFor(() => {
      expect(screen.getByText('Password Policy')).toBeInTheDocument();
      expect(screen.getByText('Session Policy')).toBeInTheDocument();
      expect(screen.getByText('Policy Name')).toBeInTheDocument();
      expect(screen.getByText('Type')).toBeInTheDocument();
      expect(screen.getByText('Status')).toBeInTheDocument();
    });
  });

  it('toggles security policy status', async () => {
    const mockUpdatePolicy = vi.fn().mockResolvedValue(true);
    (SecurityService.updateSecurityPolicy as Mock).mockImplementation(mockUpdatePolicy);

    renderWithProviders(<SecurityManagement />);

    await waitFor(() => {
      expect(screen.getByText('Password Policy')).toBeInTheDocument();
    });

    // Find and click the toggle switch for password policy
    const toggleSwitches = screen.getAllByRole('checkbox');
    const passwordPolicyToggle = toggleSwitches.find(toggle => 
      toggle.getAttribute('aria-label')?.includes('Password Policy')
    );
    
    if (passwordPolicyToggle) {
      fireEvent.click(passwordPolicyToggle);

      await waitFor(() => {
        expect(mockUpdatePolicy).toHaveBeenCalledWith('1', { enabled: false });
      });
    }
  });

  it('opens policy configuration dialog when configure button is clicked', async () => {
    renderWithProviders(<SecurityManagement />);

    await waitFor(() => {
      expect(screen.getByText('Password Policy')).toBeInTheDocument();
    });

    // Find and click configure button
    const configureButtons = screen.getAllByLabelText('Configure policy');
    fireEvent.click(configureButtons[0]);

    await waitFor(() => {
      expect(screen.getByText('Configure Security Policy')).toBeInTheDocument();
      expect(screen.getByText('Password Policy')).toBeInTheDocument();
    });
  });

  it('updates password policy settings', async () => {
    const mockUpdatePolicy = vi.fn().mockResolvedValue(true);
    (SecurityService.updateSecurityPolicy as Mock).mockImplementation(mockUpdatePolicy);

    renderWithProviders(<SecurityManagement />);

    await waitFor(() => {
      expect(screen.getByText('Password Policy')).toBeInTheDocument();
    });

    // Open configuration dialog
    const configureButtons = screen.getAllByLabelText('Configure policy');
    fireEvent.click(configureButtons[0]);

    await waitFor(() => {
      expect(screen.getByText('Configure Security Policy')).toBeInTheDocument();
    });

    // Update minimum length
    const minLengthInput = screen.getByLabelText('Minimum Length');
    fireEvent.change(minLengthInput, { target: { value: '12' } });

    // Save changes
    const saveButton = screen.getByText('Save Changes');
    fireEvent.click(saveButton);

    await waitFor(() => {
      expect(mockUpdatePolicy).toHaveBeenCalledWith('1', {
        settings: expect.objectContaining({
          min_length: 12
        })
      });
    });
  });

  it('displays audit logs with filtering options', async () => {
    renderWithProviders(<SecurityManagement />);

    // Click on Audit Logs tab
    const auditLogsTab = screen.getByText('Audit Logs');
    fireEvent.click(auditLogsTab);

    await waitFor(() => {
      expect(screen.getByText('john.doe')).toBeInTheDocument();
      expect(screen.getByText('jane.smith')).toBeInTheDocument();
      expect(screen.getByText('login')).toBeInTheDocument();
      expect(screen.getByText('delete_user')).toBeInTheDocument();
    });

    // Check filter options
    expect(screen.getByLabelText('Filter by Action')).toBeInTheDocument();
    expect(screen.getByLabelText('Filter by User')).toBeInTheDocument();
    expect(screen.getByLabelText('Date Range')).toBeInTheDocument();
  });

  it('filters audit logs by action', async () => {
    const mockFilteredLogs = mockAuditLogs.filter(log => log.action === 'login');
    (SecurityService.getAuditLogs as Mock).mockResolvedValue(mockFilteredLogs);

    renderWithProviders(<SecurityManagement />);

    // Click on Audit Logs tab
    const auditLogsTab = screen.getByText('Audit Logs');
    fireEvent.click(auditLogsTab);

    await waitFor(() => {
      expect(screen.getByLabelText('Filter by Action')).toBeInTheDocument();
    });

    // Select login action filter
    const actionFilter = screen.getByLabelText('Filter by Action');
    fireEvent.mouseDown(actionFilter);

    await waitFor(() => {
      expect(screen.getByText('Login')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText('Login'));

    await waitFor(() => {
      expect(SecurityService.getAuditLogs).toHaveBeenCalledWith({
        action: 'login'
      });
    });
  });

  it('exports audit logs', async () => {
    const mockExportLogs = vi.fn().mockResolvedValue('audit_logs.csv');
    (SecurityService.exportAuditLogs as Mock).mockImplementation(mockExportLogs);

    renderWithProviders(<SecurityManagement />);

    // Click on Audit Logs tab
    const auditLogsTab = screen.getByText('Audit Logs');
    fireEvent.click(auditLogsTab);

    await waitFor(() => {
      expect(screen.getByText('Export Logs')).toBeInTheDocument();
    });

    // Click export button
    const exportButton = screen.getByText('Export Logs');
    fireEvent.click(exportButton);

    await waitFor(() => {
      expect(mockExportLogs).toHaveBeenCalled();
    });
  });

  it('displays security alerts', async () => {
    const mockSecurityAlerts = [
      {
        id: '1',
        type: 'suspicious_login',
        severity: 'high',
        message: 'Multiple failed login attempts from IP 192.168.1.200',
        timestamp: '2024-01-01T12:00:00Z',
        status: 'active'
      },
      {
        id: '2',
        type: 'privilege_escalation',
        severity: 'medium',
        message: 'User john.doe was granted admin privileges',
        timestamp: '2024-01-01T11:30:00Z',
        status: 'acknowledged'
      }
    ];
    (SecurityService.getSecurityAlerts as Mock).mockResolvedValue(mockSecurityAlerts);

    renderWithProviders(<SecurityManagement />);

    // Click on Security Alerts tab
    const alertsTab = screen.getByText('Security Alerts');
    fireEvent.click(alertsTab);

    await waitFor(() => {
      expect(screen.getByText('Multiple failed login attempts')).toBeInTheDocument();
      expect(screen.getByText('User john.doe was granted admin privileges')).toBeInTheDocument();
    });
  });

  it('acknowledges security alert', async () => {
    const mockAcknowledgeAlert = vi.fn().mockResolvedValue(true);
    (SecurityService.acknowledgeSecurityAlert as Mock).mockImplementation(mockAcknowledgeAlert);

    const mockSecurityAlerts = [
      {
        id: '1',
        type: 'suspicious_login',
        severity: 'high',
        message: 'Multiple failed login attempts from IP 192.168.1.200',
        timestamp: '2024-01-01T12:00:00Z',
        status: 'active'
      }
    ];
    (SecurityService.getSecurityAlerts as Mock).mockResolvedValue(mockSecurityAlerts);

    renderWithProviders(<SecurityManagement />);

    // Click on Security Alerts tab
    const alertsTab = screen.getByText('Security Alerts');
    fireEvent.click(alertsTab);

    await waitFor(() => {
      expect(screen.getByText('Acknowledge')).toBeInTheDocument();
    });

    // Click acknowledge button
    const acknowledgeButton = screen.getByText('Acknowledge');
    fireEvent.click(acknowledgeButton);

    await waitFor(() => {
      expect(mockAcknowledgeAlert).toHaveBeenCalledWith('1');
    });
  });

  it('displays user access review', async () => {
    const mockAccessReview = [
      {
        user_id: 'user1',
        username: 'john.doe',
        roles: ['admin', 'editor'],
        last_login: '2024-01-01T10:00:00Z',
        permissions_count: 15,
        risk_score: 'high'
      },
      {
        user_id: 'user2',
        username: 'jane.smith',
        roles: ['editor'],
        last_login: '2023-12-15T14:30:00Z',
        permissions_count: 8,
        risk_score: 'medium'
      }
    ];
    (SecurityService.getUserAccessReview as Mock).mockResolvedValue(mockAccessReview);

    renderWithProviders(<SecurityManagement />);

    // Click on Access Review tab
    const accessReviewTab = screen.getByText('Access Review');
    fireEvent.click(accessReviewTab);

    await waitFor(() => {
      expect(screen.getByText('john.doe')).toBeInTheDocument();
      expect(screen.getByText('jane.smith')).toBeInTheDocument();
      expect(screen.getByText('High Risk')).toBeInTheDocument();
      expect(screen.getByText('Medium Risk')).toBeInTheDocument();
    });
  });

  it('handles bulk operations on users', async () => {
    const mockBulkDeactivateUsers = vi.fn().mockResolvedValue(true);
    (SecurityService.bulkDeactivateUsers as Mock).mockImplementation(mockBulkDeactivateUsers);

    const mockAccessReview = [
      {
        user_id: 'user1',
        username: 'john.doe',
        roles: ['admin'],
        last_login: '2023-11-01T10:00:00Z',
        permissions_count: 15,
        risk_score: 'high'
      },
      {
        user_id: 'user2',
        username: 'jane.smith',
        roles: ['editor'],
        last_login: '2023-10-15T14:30:00Z',
        permissions_count: 8,
        risk_score: 'medium'
      }
    ];
    (SecurityService.getUserAccessReview as Mock).mockResolvedValue(mockAccessReview);

    renderWithProviders(<SecurityManagement />);

    // Click on Access Review tab
    const accessReviewTab = screen.getByText('Access Review');
    fireEvent.click(accessReviewTab);

    await waitFor(() => {
      expect(screen.getByText('john.doe')).toBeInTheDocument();
    });

    // Select users
    const checkboxes = screen.getAllByRole('checkbox');
    fireEvent.click(checkboxes[1]); // First user
    fireEvent.click(checkboxes[2]); // Second user

    // Should show bulk actions
    await waitFor(() => {
      expect(screen.getByText('Deactivate Selected')).toBeInTheDocument();
    });

    // Perform bulk deactivation
    window.confirm = vi.fn().mockReturnValue(true);
    fireEvent.click(screen.getByText('Deactivate Selected'));

    await waitFor(() => {
      expect(mockBulkDeactivateUsers).toHaveBeenCalledWith(['user1', 'user2']);
    });
  });

  it('creates security policy', async () => {
    const mockCreatePolicy = vi.fn().mockResolvedValue({
      id: '3',
      name: 'API Rate Limiting',
      description: 'Rate limiting for API endpoints',
      type: 'api',
      enabled: true
    });
    (SecurityService.createSecurityPolicy as Mock).mockImplementation(mockCreatePolicy);

    renderWithProviders(<SecurityManagement />);

    // Click create policy button
    const createButton = screen.getByText('Create Policy');
    fireEvent.click(createButton);

    await waitFor(() => {
      expect(screen.getByText('Create Security Policy')).toBeInTheDocument();
    });

    // Fill form
    const nameInput = screen.getByLabelText('Policy Name');
    const descriptionInput = screen.getByLabelText('Description');
    const typeSelect = screen.getByLabelText('Policy Type');

    fireEvent.change(nameInput, { target: { value: 'API Rate Limiting' } });
    fireEvent.change(descriptionInput, { target: { value: 'Rate limiting for API endpoints' } });
    fireEvent.mouseDown(typeSelect);

    await waitFor(() => {
      expect(screen.getByText('API')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText('API'));

    // Submit form
    const createPolicyButton = screen.getByText('Create');
    fireEvent.click(createPolicyButton);

    await waitFor(() => {
      expect(mockCreatePolicy).toHaveBeenCalledWith({
        name: 'API Rate Limiting',
        description: 'Rate limiting for API endpoints',
        type: 'api',
        enabled: true
      });
    });
  });

  it('handles two-factor authentication settings', async () => {
    const mockUpdateTwoFASettings = vi.fn().mockResolvedValue(true);
    (SecurityService.updateTwoFactorSettings as Mock).mockImplementation(mockUpdateTwoFASettings);

    renderWithProviders(<SecurityManagement />);

    // Click on Settings tab
    const settingsTab = screen.getByText('Settings');
    fireEvent.click(settingsTab);

    await waitFor(() => {
      expect(screen.getByText('Two-Factor Authentication')).toBeInTheDocument();
    });

    // Enable 2FA requirement
    const require2FACheckbox = screen.getByLabelText('Require Two-Factor Authentication');
    fireEvent.click(require2FACheckbox);

    // Save settings
    const saveButton = screen.getByText('Save Settings');
    fireEvent.click(saveButton);

    await waitFor(() => {
      expect(mockUpdateTwoFASettings).toHaveBeenCalledWith({
        require_2fa: true
      });
    });
  });

  it('displays compliance report', async () => {
    const mockComplianceReport = {
      overall_score: 85,
      categories: {
        access_control: 90,
        data_protection: 80,
        audit_logging: 95,
        password_policy: 75
      },
      recommendations: [
        'Enable mandatory two-factor authentication',
        'Review and update password complexity requirements',
        'Implement automated user access reviews'
      ]
    };
    (SecurityService.getComplianceReport as Mock).mockResolvedValue(mockComplianceReport);

    renderWithProviders(<SecurityManagement />);

    // Click on Compliance tab
    const complianceTab = screen.getByText('Compliance');
    fireEvent.click(complianceTab);

    await waitFor(() => {
      expect(screen.getByText('Overall Compliance Score')).toBeInTheDocument();
      expect(screen.getByText('85%')).toBeInTheDocument();
      expect(screen.getByText('Access Control: 90%')).toBeInTheDocument();
      expect(screen.getByText('Enable mandatory two-factor authentication')).toBeInTheDocument();
    });
  });

  it('handles loading state correctly', async () => {
    // Delay the API response to test loading state
    (SecurityService.getSecurityPolicies as Mock).mockImplementation(
      () => new Promise(resolve => setTimeout(() => resolve(mockSecurityPolicies), 1000))
    );

    renderWithProviders(<SecurityManagement />);

    // Should show loading indicator
    expect(screen.getByRole('progressbar')).toBeInTheDocument();

    await waitFor(() => {
      expect(screen.getByText('Password Policy')).toBeInTheDocument();
    }, { timeout: 2000 });
  });

  it('handles error state correctly', async () => {
    (SecurityService.getSecurityPolicies as Mock).mockRejectedValue(new Error('Failed to load security policies'));

    renderWithProviders(<SecurityManagement />);

    await waitFor(() => {
      expect(screen.getByText('Error loading security policies')).toBeInTheDocument();
    });
  });

  it('handles permissions correctly for non-admin users', async () => {
    const nonAdminState = {
      auth: {
        user: {
          id: '123',
          username: 'testuser',
          roles: ['security_officer'],
          is_superuser: false
        }
      }
    };

    renderWithProviders(<SecurityManagement />, nonAdminState);

    await waitFor(() => {
      // Should have read-only access - no create/edit buttons
      expect(screen.queryByText('Create Policy')).not.toBeInTheDocument();
      expect(screen.queryByLabelText('Configure policy')).not.toBeInTheDocument();
    });
  });
});