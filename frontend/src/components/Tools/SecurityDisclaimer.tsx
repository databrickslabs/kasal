import React from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Typography,
  Alert,
  Box,
  Chip,
  List,
  ListItem,
  ListItemText,
  Divider
} from '@mui/material';
import WarningIcon from '@mui/icons-material/Warning';
import SecurityIcon from '@mui/icons-material/Security';
import { Tool } from '../../types/tool';

interface SecurityDisclaimerProps {
  open: boolean;
  onClose: () => void;
  onConfirm: () => void;
  tool: Tool | null;
}

// Security risk levels and descriptions for each tool
const TOOL_SECURITY_INFO: Record<string, {
  riskLevel: 'CRITICAL' | 'HIGH' | 'MEDIUM' | 'LOW';
  risks: string[];
  mitigations: string[];
  description: string;
}> = {
  // CRITICAL RISK TOOLS
  'FileReadTool': {
    riskLevel: 'CRITICAL',
    description: 'Can read ANY file on the system without restrictions',
    risks: [
      'Path traversal attacks (../../../etc/passwd)',
      'Access to other users\' sensitive files',
      'Reading system configuration files',
      'Exposure of API keys, passwords, and secrets',
      'No tenant isolation in multi-user environments'
    ],
    mitigations: [
      'Restrict to specific directories only',
      'Implement path validation and sanitization',
      'Use sandboxed file access',
      'Add audit logging for all file reads'
    ]
  },
  'FileWriterTool': {
    riskLevel: 'CRITICAL',
    description: 'Can write/overwrite files anywhere on the system',
    risks: [
      'Overwrite critical system files',
      'Create malicious executable files',
      'Modify other users\' data',
      'Path traversal for unauthorized file creation',
      'No size limits or content validation'
    ],
    mitigations: [
      'Restrict write operations to specific directories',
      'Implement file size and content validation',
      'Use sandboxed file system access',
      'Add malware scanning for uploaded content'
    ]
  },
  'CodeInterpreterTool': {
    riskLevel: 'CRITICAL',
    description: 'Executes arbitrary Python code with potential system access',
    risks: [
      'Arbitrary code execution on host system',
      'Network access to internal systems',
      'File system manipulation',
      'Process spawning and system commands',
      'Installation of malicious packages'
    ],
    mitigations: [
      'Use containerized execution environment',
      'Implement strict resource limits',
      'Block network access and system calls',
      'Use read-only file systems'
    ]
  },
  'DirectoryReadTool': {
    riskLevel: 'CRITICAL',
    description: 'Can read and traverse any directory structure',
    risks: [
      'Exposure of directory structures and file paths',
      'Discovery of hidden files and folders',
      'Access to other users\' directory listings',
      'Information gathering for further attacks'
    ],
    mitigations: [
      'Restrict to user-specific directories',
      'Implement path validation',
      'Add access control checks'
    ]
  },
  'DatabricksCustomTool': {
    riskLevel: 'CRITICAL',
    description: 'Executes unlimited SQL operations on Databricks',
    risks: [
      'SQL injection vulnerabilities',
      'Unauthorized data access across tenants',
      'Data modification or deletion',
      'Performance impact from expensive queries',
      'Access to system metadata tables'
    ],
    mitigations: [
      'Use parameterized queries only',
      'Implement tenant-based data filtering',
      'Add query approval workflows',
      'Set query timeouts and resource limits'
    ]
  },
  'NL2SQLTool': {
    riskLevel: 'CRITICAL',
    description: 'Converts natural language to SQL with injection risks',
    risks: [
      'SQL injection through natural language manipulation',
      'Unrestricted database access',
      'Data exfiltration across tenant boundaries',
      'Expensive query execution'
    ],
    mitigations: [
      'Use parameterized queries',
      'Implement query whitelisting',
      'Add tenant isolation filters',
      'Monitor and limit query complexity'
    ]
  },

  // HIGH RISK TOOLS
  'BrowserUseTool': {
    riskLevel: 'HIGH',
    description: 'Provides automated browser control capabilities',
    risks: [
      'Unauthorized web interactions',
      'Access to authenticated sessions',
      'Potential for malicious website interactions',
      'Network requests to internal systems'
    ],
    mitigations: [
      'Implement URL whitelisting',
      'Use isolated browser sessions',
      'Monitor and log all web interactions',
      'Restrict to approved domains only'
    ]
  },
  'SeleniumScrapingTool': {
    riskLevel: 'HIGH',
    description: 'Full browser automation with web interaction capabilities',
    risks: [
      'Automated interactions with any website',
      'Cookie and session manipulation',
      'Form submission with arbitrary data',
      'Download of potentially malicious content'
    ],
    mitigations: [
      'Implement strict URL validation',
      'Use sandboxed browser environment',
      'Add content filtering and scanning',
      'Limit interaction capabilities'
    ]
  },
  'MySQLSearchTool': {
    riskLevel: 'HIGH',
    description: 'Direct MySQL database access without tenant filtering',
    risks: [
      'Cross-tenant data access',
      'Potential for SQL injection',
      'Unauthorized database operations',
      'Performance impact from expensive queries'
    ],
    mitigations: [
      'Implement tenant-based access controls',
      'Use read-only database connections',
      'Add query validation and sanitization',
      'Monitor database access patterns'
    ]
  },
  'PGSearchTool': {
    riskLevel: 'HIGH',
    description: 'Direct PostgreSQL database access without restrictions',
    risks: [
      'Unrestricted database query execution',
      'Cross-tenant data exposure',
      'Performance degradation from complex queries',
      'Access to system tables and metadata'
    ],
    mitigations: [
      'Implement row-level security',
      'Use dedicated read-only database user',
      'Add query complexity limits',
      'Enable comprehensive audit logging'
    ]
  },

  // MEDIUM RISK TOOLS
  'CSVSearchTool': {
    riskLevel: 'MEDIUM',
    description: 'Searches within CSV files with potential path traversal',
    risks: [
      'Access to CSV files outside intended scope',
      'Exposure of structured data across tenants',
      'Path traversal to unauthorized files'
    ],
    mitigations: [
      'Restrict file access to specific directories',
      'Validate file paths and extensions',
      'Implement user-based access controls'
    ]
  },
  'JSONSearchTool': {
    riskLevel: 'MEDIUM',
    description: 'Accesses JSON files potentially containing sensitive data',
    risks: [
      'Exposure of configuration files',
      'Access to API keys in JSON format',
      'Cross-tenant file access'
    ],
    mitigations: [
      'Sanitize sensitive data from results',
      'Restrict to approved file locations',
      'Add content filtering'
    ]
  },
  'SendPulseEmailTool': {
    riskLevel: 'MEDIUM',
    description: 'Sends emails without recipient restrictions',
    risks: [
      'Spam and phishing email generation',
      'Unauthorized use of email credentials',
      'Reputation damage to email domain'
    ],
    mitigations: [
      'Implement recipient whitelisting',
      'Add email content validation',
      'Monitor sending patterns and limits'
    ]
  },
  'DirectorySearchTool': {
    riskLevel: 'HIGH',
    description: 'Searches directory structures without path restrictions',
    risks: [
      'Discovery of hidden files and directories',
      'Mapping of system file structures',
      'Information gathering for targeted attacks',
      'Cross-tenant directory access'
    ],
    mitigations: [
      'Restrict to user-specific directories only',
      'Implement path validation and sanitization',
      'Add audit logging for directory searches'
    ]
  },
  'TXTSearchTool': {
    riskLevel: 'MEDIUM',
    description: 'Searches text files that may contain sensitive information',
    risks: [
      'Access to log files containing secrets',
      'Reading configuration files with credentials',
      'Cross-tenant file access',
      'Exposure of debugging information'
    ],
    mitigations: [
      'Restrict file access to approved directories',
      'Filter out sensitive content from results',
      'Implement content scanning and sanitization'
    ]
  },
  'PDFSearchTool': {
    riskLevel: 'MEDIUM',
    description: 'Searches PDF documents that may contain sensitive data',
    risks: [
      'Access to confidential documents',
      'Cross-tenant document access',
      'Exposure of embedded metadata',
      'Access to password-protected content'
    ],
    mitigations: [
      'Implement document access controls',
      'Strip metadata from search results',
      'Add content filtering capabilities'
    ]
  },
  'DOCXSearchTool': {
    riskLevel: 'MEDIUM',
    description: 'Searches Word documents potentially containing sensitive data',
    risks: [
      'Access to business-critical documents',
      'Exposure of document metadata and history',
      'Cross-tenant document access',
      'Reading embedded objects and macros'
    ],
    mitigations: [
      'Restrict to approved document repositories',
      'Sanitize document metadata',
      'Implement virus scanning for macros'
    ]
  },
  'XMLSearchTool': {
    riskLevel: 'MEDIUM',
    description: 'Searches XML files including configuration and data files',
    risks: [
      'Access to application configuration files',
      'Exposure of API endpoints and credentials',
      'Reading database connection strings',
      'Access to structured sensitive data'
    ],
    mitigations: [
      'Implement XML content filtering',
      'Restrict to non-sensitive XML files',
      'Sanitize credential information'
    ]
  },
  'SnowflakeSearchTool': {
    riskLevel: 'HIGH',
    description: 'Direct Snowflake data warehouse access without tenant filtering',
    risks: [
      'Cross-tenant data warehouse access',
      'Exposure of enterprise data analytics',
      'Unauthorized query execution on large datasets',
      'Performance impact from expensive operations'
    ],
    mitigations: [
      'Implement row-level security policies',
      'Use role-based access controls',
      'Add query monitoring and limits',
      'Enable comprehensive audit logging'
    ]
  },
  'QdrantVectorSearchTool': {
    riskLevel: 'HIGH',
    description: 'Vector database access for similarity searches',
    risks: [
      'Access to vectorized sensitive data',
      'Cross-tenant vector space access',
      'Potential for data inference attacks',
      'Exposure of ML model embeddings'
    ],
    mitigations: [
      'Implement vector space isolation',
      'Add tenant-based filtering',
      'Monitor similarity search patterns'
    ]
  },
  'WeaviateVectorSearchTool': {
    riskLevel: 'HIGH',
    description: 'Vector database operations with multi-modal search',
    risks: [
      'Multi-modal data access across tenants',
      'Exposure of knowledge graphs',
      'Semantic search across sensitive content',
      'Vector-based data correlation attacks'
    ],
    mitigations: [
      'Implement strict schema-based isolation',
      'Add vector access controls',
      'Monitor cross-modal search patterns'
    ]
  }
};

const SecurityDisclaimer: React.FC<SecurityDisclaimerProps> = ({
  open,
  onClose,
  onConfirm,
  tool
}) => {
  if (!tool) return null;

  const securityInfo = TOOL_SECURITY_INFO[tool.title] || {
    riskLevel: 'MEDIUM' as const,
    description: 'This tool may pose security risks in multi-tenant environments',
    risks: ['Potential for unauthorized access', 'May affect other users'],
    mitigations: ['Monitor usage carefully', 'Implement proper access controls']
  };

  const getRiskColor = (level: string) => {
    switch (level) {
      case 'CRITICAL': return 'error';
      case 'HIGH': return 'warning';
      case 'MEDIUM': return 'info';
      case 'LOW': return 'success';
      default: return 'default';
    }
  };

  return (
    <Dialog 
      open={open} 
      onClose={onClose}
      maxWidth="md"
      fullWidth
      PaperProps={{
        sx: { minHeight: '400px' }
      }}
    >
      <DialogTitle sx={{ display: 'flex', alignItems: 'center', gap: 1, pb: 1 }}>
        <SecurityIcon color="error" />
        <Typography variant="h6">Security Warning</Typography>
        <Chip 
          label={`${securityInfo.riskLevel} RISK`}
          color={getRiskColor(securityInfo.riskLevel)}
          size="small"
          sx={{ ml: 'auto' }}
        />
      </DialogTitle>
      
      <DialogContent>
        <Alert severity="warning" sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>
            <strong>⚠️ You are about to enable: {tool.title}</strong>
          </Typography>
          <Typography variant="body2">
            {securityInfo.description}
          </Typography>
        </Alert>

        <Box sx={{ mb: 3 }}>
          <Typography variant="h6" color="error" gutterBottom sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <WarningIcon /> Security Risks
          </Typography>
          <List dense>
            {securityInfo.risks.map((risk, index) => (
              <ListItem key={index} sx={{ py: 0.5 }}>
                <ListItemText 
                  primary={`• ${risk}`}
                  sx={{ color: 'error.main' }}
                />
              </ListItem>
            ))}
          </List>
        </Box>

        <Divider sx={{ my: 2 }} />

        <Box>
          <Typography variant="h6" color="success.main" gutterBottom>
            🛡️ Recommended Security Mitigations
          </Typography>
          <List dense>
            {securityInfo.mitigations.map((mitigation, index) => (
              <ListItem key={index} sx={{ py: 0.5 }}>
                <ListItemText 
                  primary={`• ${mitigation}`}
                  sx={{ color: 'text.secondary' }}
                />
              </ListItem>
            ))}
          </List>
        </Box>

        <Alert severity="error" sx={{ mt: 3 }}>
          <Typography variant="body2">
            <strong>By enabling this tool, you acknowledge that:</strong>
          </Typography>
          <Typography variant="body2" component="ul" sx={{ mt: 1, mb: 0 }}>
            <li>You understand the security risks associated with this tool</li>
            <li>You will monitor its usage in your environment</li>
            <li>You are responsible for implementing appropriate security controls</li>
            <li>This tool may access sensitive data or system resources</li>
          </Typography>
        </Alert>
      </DialogContent>

      <DialogActions sx={{ px: 3, pb: 3 }}>
        <Button onClick={onClose} variant="outlined">
          Cancel
        </Button>
        <Button 
          onClick={onConfirm} 
          variant="contained" 
          color="warning"
          startIcon={<SecurityIcon />}
        >
          I Understand the Risks - Enable Tool
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default SecurityDisclaimer;