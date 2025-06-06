import React, { useState } from 'react';
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
  Divider,
  Collapse
} from '@mui/material';
import WarningIcon from '@mui/icons-material/Warning';
import SecurityIcon from '@mui/icons-material/Security';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import ExpandLessIcon from '@mui/icons-material/ExpandLess';
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
  singleTenantRiskLevel?: 'CRITICAL' | 'HIGH' | 'MEDIUM' | 'LOW';
  singleTenantRisks?: string[];
  singleTenantMitigations?: string[];
  deploymentContext?: string;
}> = {
  // CRITICAL RISK TOOLS
  'FileReadTool': {
    riskLevel: 'CRITICAL',
    description: 'Can read files within the container filesystem',
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
    ],
    singleTenantRiskLevel: 'LOW',
    singleTenantRisks: [
      'Can read files within user\'s dedicated container',
      'Access to user\'s workspace files and configurations',
      'Reading of temporary files and logs within container'
    ],
    singleTenantMitigations: [
      'Container runs with non-root user privileges',
      'Filesystem isolated to user\'s container only',
      'API keys managed through secure backend service',
      'No access to other users or system files'
    ],
    deploymentContext: 'Single-tenant containerized deployment on Databricks'
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
    ],
    singleTenantRiskLevel: 'LOW',
    singleTenantRisks: [
      'Can write files within user\'s container workspace',
      'Potential to fill container disk space'
    ],
    singleTenantMitigations: [
      'Container filesystem isolation',
      'Non-root user privileges',
      'Disk quotas prevent space exhaustion'
    ],
    deploymentContext: 'Single-tenant containerized deployment'
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
    ],
    singleTenantRiskLevel: 'MEDIUM',
    singleTenantRisks: [
      'Code execution within container sandbox',
      'Resource consumption (CPU, memory)',
      'Network requests to external services'
    ],
    singleTenantMitigations: [
      'Sandboxed execution environment',
      'Resource limits enforced',
      'Network access controlled',
      'Non-persistent container state'
    ],
    deploymentContext: 'Single-tenant containerized deployment'
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
    ],
    singleTenantRiskLevel: 'LOW',
    singleTenantRisks: [
      'Can list directories within user\'s container',
      'Access to container workspace structure'
    ],
    singleTenantMitigations: [
      'Container filesystem isolation',
      'Non-root user privileges',
      'Limited to user\'s workspace'
    ],
    deploymentContext: 'Single-tenant containerized deployment'
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
    ],
    singleTenantRiskLevel: 'MEDIUM',
    singleTenantRisks: [
      'SQL injection if queries not properly sanitized',
      'Access to all data user has permissions for',
      'Potential for expensive queries impacting performance',
      'Data modification within user\'s scope'
    ],
    singleTenantMitigations: [
      'Databricks On-Behalf-Of (OBO) scopes access to user permissions',
      'User can only access data they\'re already authorized for',
      'Query execution limits enforced by Databricks',
      'SQL operations logged and auditable'
    ],
    deploymentContext: 'Single-tenant with Databricks OBO security model'
  },
  'GenieTool': {
    riskLevel: 'HIGH',
    description: 'Natural language interface to Databricks Genie AI assistant',
    risks: [
      'Unrestricted natural language queries to database',
      'Potential for complex data aggregations',
      'Access to sensitive data through conversational interface',
      'Query interpretation may expose unexpected data'
    ],
    mitigations: [
      'Implement query complexity limits',
      'Add data access monitoring',
      'Review query translations before execution',
      'Use read-only database connections where possible'
    ],
    singleTenantRiskLevel: 'LOW',
    singleTenantRisks: [
      'Natural language queries within user\'s data scope',
      'Potential for unintended data exposure through AI interpretation',
      'Performance impact from complex generated queries'
    ],
    singleTenantMitigations: [
      'Databricks OBO ensures queries run with user\'s permissions only',
      'Genie AI respects existing data access controls',
      'Query execution limited to user\'s authorized datasets',
      'All interactions logged and auditable'
    ],
    deploymentContext: 'Single-tenant with Databricks OBO and Genie AI security'
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
    ],
    singleTenantRiskLevel: 'MEDIUM',
    singleTenantRisks: [
      'SQL injection if NL parsing is compromised',
      'Access to database within user permissions',
      'Potential for complex expensive queries'
    ],
    singleTenantMitigations: [
      'Database access limited to user permissions',
      'Query complexity monitoring',
      'Parameterized query generation',
      'Database connection limits'
    ],
    deploymentContext: 'Single-tenant with database access controls'
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
    ],
    singleTenantRiskLevel: 'MEDIUM',
    singleTenantRisks: [
      'Automated web interactions via external API',
      'Network requests to external services',
      'Resource consumption for browser operations'
    ],
    singleTenantMitigations: [
      'External API controls browser access',
      'Network egress controls',
      'API rate limiting',
      'Container resource limits'
    ],
    deploymentContext: 'Single-tenant with external browser API'
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
    ],
    singleTenantRiskLevel: 'MEDIUM',
    singleTenantRisks: [
      'Web scraping within container environment',
      'Network requests to external websites',
      'Local file downloads within container'
    ],
    singleTenantMitigations: [
      'Container network controls',
      'File downloads isolated to container',
      'Browser runs in sandboxed environment',
      'Resource limits prevent abuse'
    ],
    deploymentContext: 'Single-tenant containerized deployment'
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
    ],
    singleTenantRiskLevel: 'MEDIUM',
    singleTenantRisks: [
      'Database access within user permissions',
      'Potential for expensive queries',
      'MySQL connection resource usage'
    ],
    singleTenantMitigations: [
      'Database user permissions control access',
      'Connection pooling and limits',
      'Query timeout enforcement',
      'API key service manages credentials'
    ],
    deploymentContext: 'Single-tenant with API key service'
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
    ],
    singleTenantRiskLevel: 'MEDIUM',
    singleTenantRisks: [
      'PostgreSQL access within user permissions',
      'Complex query execution',
      'Database resource consumption'
    ],
    singleTenantMitigations: [
      'Database role-based access controls',
      'Connection limits and timeouts',
      'Query monitoring and limits',
      'Secure credential management'
    ],
    deploymentContext: 'Single-tenant with API key service'
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
    ],
    singleTenantRiskLevel: 'LOW',
    singleTenantRisks: [
      'Access to CSV files within user\'s container workspace',
      'File content exposure limited to user\'s data'
    ],
    singleTenantMitigations: [
      'Container filesystem isolation',
      'Non-root user privileges',
      'File access limited to user workspace'
    ],
    deploymentContext: 'Single-tenant containerized deployment'
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
    ],
    singleTenantRiskLevel: 'LOW',
    singleTenantRisks: [
      'Access to JSON files within user\'s container',
      'Configuration file exposure limited to user scope'
    ],
    singleTenantMitigations: [
      'API keys managed through secure backend service',
      'Container filesystem isolation',
      'Non-root user privileges'
    ],
    deploymentContext: 'Single-tenant containerized deployment'
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
    ],
    singleTenantRiskLevel: 'MEDIUM',
    singleTenantRisks: [
      'Email sending via external API service',
      'Potential for spam or inappropriate emails',
      'Resource consumption for email operations'
    ],
    singleTenantMitigations: [
      'API key service manages email credentials',
      'External service rate limiting',
      'Network egress controls',
      'Email content monitoring if required'
    ],
    deploymentContext: 'Single-tenant with external email API service'
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
    ],
    singleTenantRiskLevel: 'LOW',
    singleTenantRisks: [
      'Directory structure discovery within user container',
      'Access to user workspace file organization'
    ],
    singleTenantMitigations: [
      'Container filesystem isolation',
      'Non-root user privileges',
      'Limited to user\'s workspace directories'
    ],
    deploymentContext: 'Single-tenant containerized deployment'
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
    ],
    singleTenantRiskLevel: 'LOW',
    singleTenantRisks: [
      'Access to text files within user container',
      'Log file access limited to user\'s processes',
      'Configuration exposure within user scope'
    ],
    singleTenantMitigations: [
      'Container filesystem isolation',
      'API keys managed through secure backend service',
      'Non-root user privileges',
      'Content limited to user workspace'
    ],
    deploymentContext: 'Single-tenant containerized deployment'
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
    ],
    singleTenantRiskLevel: 'LOW',
    singleTenantRisks: [
      'Access to PDF documents within user container',
      'Document metadata exposure limited to user files',
      'Content access within user workspace'
    ],
    singleTenantMitigations: [
      'Container filesystem isolation',
      'Non-root user privileges',
      'Document access limited to user workspace',
      'No cross-tenant document exposure'
    ],
    deploymentContext: 'Single-tenant containerized deployment'
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
    ],
    singleTenantRiskLevel: 'LOW',
    singleTenantRisks: [
      'Access to Word documents within user container',
      'Document metadata exposure limited to user files',
      'Macro execution within sandboxed environment'
    ],
    singleTenantMitigations: [
      'Container filesystem isolation',
      'Non-root user privileges',
      'Document access limited to user workspace',
      'Macro execution in controlled environment'
    ],
    deploymentContext: 'Single-tenant containerized deployment'
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
    ],
    singleTenantRiskLevel: 'LOW',
    singleTenantRisks: [
      'Access to XML files within user container',
      'Configuration file exposure limited to user scope',
      'Structured data access within user workspace'
    ],
    singleTenantMitigations: [
      'Container filesystem isolation',
      'API keys managed through secure backend service',
      'Non-root user privileges',
      'XML content limited to user workspace'
    ],
    deploymentContext: 'Single-tenant containerized deployment'
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
    ],
    singleTenantRiskLevel: 'MEDIUM',
    singleTenantRisks: [
      'Snowflake access within user permissions',
      'Query execution on authorized datasets',
      'Performance impact from complex queries',
      'Data warehouse resource consumption'
    ],
    singleTenantMitigations: [
      'Snowflake role-based access controls',
      'API key service manages credentials',
      'Query timeout and resource limits',
      'Connection pooling and monitoring'
    ],
    deploymentContext: 'Single-tenant with API key service'
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
    ],
    singleTenantRiskLevel: 'MEDIUM',
    singleTenantRisks: [
      'Vector database access within user permissions',
      'Similarity searches on user\'s vector collections',
      'ML model embedding exposure within user scope'
    ],
    singleTenantMitigations: [
      'Vector database access controls',
      'API key service manages credentials',
      'Collection-level access restrictions',
      'Query monitoring and limits'
    ],
    deploymentContext: 'Single-tenant with API key service'
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
    ],
    singleTenantRiskLevel: 'MEDIUM',
    singleTenantRisks: [
      'Multi-modal vector search within user permissions',
      'Knowledge graph access limited to user\'s data',
      'Semantic search on authorized content'
    ],
    singleTenantMitigations: [
      'Weaviate schema-based access controls',
      'API key service manages credentials',
      'User-specific vector collections',
      'Query monitoring and resource limits'
    ],
    deploymentContext: 'Single-tenant with API key service'
  }
};

const SecurityDisclaimer: React.FC<SecurityDisclaimerProps> = ({
  open,
  onClose,
  onConfirm,
  tool
}) => {
  const [showDetails, setShowDetails] = useState(false);
  
  if (!tool) return null;

  const securityInfo = TOOL_SECURITY_INFO[tool.title] || {
    riskLevel: 'MEDIUM' as const,
    description: 'This tool may pose security risks in multi-tenant environments',
    risks: ['Potential for unauthorized access', 'May affect other users'],
    mitigations: ['Monitor usage carefully', 'Implement proper access controls'],
    singleTenantRiskLevel: 'LOW' as const,
    singleTenantRisks: ['Limited risk in containerized single-tenant environment'],
    singleTenantMitigations: ['Container isolation provides security boundaries'],
    deploymentContext: 'Single-tenant containerized deployment'
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
        <Box sx={{ ml: 'auto', display: 'flex', gap: 1 }}>
          <Chip 
            label={`MULTI-TENANT: ${securityInfo.riskLevel}`}
            color={getRiskColor(securityInfo.riskLevel)}
            size="small"
          />
          {securityInfo.singleTenantRiskLevel && (
            <Chip 
              label={`SINGLE-TENANT: ${securityInfo.singleTenantRiskLevel}`}
              color={getRiskColor(securityInfo.singleTenantRiskLevel)}
              size="small"
              variant="outlined"
            />
          )}
        </Box>
      </DialogTitle>
      
      <DialogContent>
        <Alert severity="warning" sx={{ mb: 3 }}>
          <Typography variant="subtitle1" gutterBottom>
            <strong>⚠️ You are about to enable: {tool.title}</strong>
          </Typography>
          <Typography variant="body2" sx={{ mb: 2 }}>
            This tool has security implications. Please review the risk classifications below.
          </Typography>
          
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
            <Typography variant="body2" sx={{ fontWeight: 'bold' }}>
              Security Risk Levels:
            </Typography>
            <Chip 
              label={`Multi-Tenant: ${securityInfo.riskLevel}`}
              color={getRiskColor(securityInfo.riskLevel)}
              size="small"
            />
            <Chip 
              label={`Single-Tenant: ${securityInfo.singleTenantRiskLevel || 'LOW'}`}
              color={getRiskColor(securityInfo.singleTenantRiskLevel || 'LOW')}
              size="small"
              variant="outlined"
            />
          </Box>

          <Button
            onClick={() => setShowDetails(!showDetails)}
            startIcon={showDetails ? <ExpandLessIcon /> : <ExpandMoreIcon />}
            size="small"
            sx={{ mt: 1 }}
          >
            {showDetails ? 'Hide Details' : 'Show Security Details'}
          </Button>
        </Alert>

        <Collapse in={showDetails}>
          <Box sx={{ mb: 3 }}>
            {securityInfo.deploymentContext && (
              <Typography variant="body2" sx={{ mb: 2, fontStyle: 'italic', color: 'text.secondary' }}>
                Deployment Context: {securityInfo.deploymentContext}
              </Typography>
            )}

            {/* Multi-tenant risks */}
            <Box sx={{ mb: 3 }}>
              <Typography variant="h6" color="error" gutterBottom sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                <WarningIcon /> Multi-Tenant Security Risks
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

            {/* Single-tenant risks */}
            {securityInfo.singleTenantRisks && (
              <Box sx={{ mb: 3 }}>
                <Typography variant="h6" color="info.main" gutterBottom sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                  📦 Single-Tenant Containerized Risks
                </Typography>
                <List dense>
                  {securityInfo.singleTenantRisks.map((risk, index) => (
                    <ListItem key={index} sx={{ py: 0.5 }}>
                      <ListItemText 
                        primary={`• ${risk}`}
                        sx={{ color: 'info.main' }}
                      />
                    </ListItem>
                  ))}
                </List>
              </Box>
            )}

            <Divider sx={{ my: 2 }} />

            {/* Multi-tenant mitigations */}
            <Box sx={{ mb: 2 }}>
              <Typography variant="h6" color="success.main" gutterBottom>
                🛡️ General Security Mitigations
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

            {/* Single-tenant mitigations */}
            {securityInfo.singleTenantMitigations && (
              <Box sx={{ mb: 2 }}>
                <Typography variant="h6" color="success.main" gutterBottom>
                  ✅ Container Security Mitigations
                </Typography>
                <List dense>
                  {securityInfo.singleTenantMitigations.map((mitigation, index) => (
                    <ListItem key={index} sx={{ py: 0.5 }}>
                      <ListItemText 
                        primary={`• ${mitigation}`}
                        sx={{ color: 'success.main' }}
                      />
                    </ListItem>
                  ))}
                </List>
              </Box>
            )}
          </Box>
        </Collapse>

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