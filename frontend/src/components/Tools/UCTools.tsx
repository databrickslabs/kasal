import React, { Fragment, useState } from 'react';
import {
  Card,
  CardContent,
  Typography,
  Box,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Chip,
  IconButton,
  Collapse,
  CircularProgress,
  Alert,
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import ExpandLessIcon from '@mui/icons-material/ExpandLess';
import StorageIcon from '@mui/icons-material/Storage';
import { UCToolsProps } from '../../types/tool';
import { useTranslation } from 'react-i18next';

function UCTools({ tools, loading, error }: UCToolsProps): JSX.Element {
  const [expandedTool, setExpandedTool] = useState<string | null>(null);
  const { t } = useTranslation();

  if (loading) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', p: 3 }}>
        <CircularProgress />
      </Box>
    );
  }

  if (error) {
    return (
      <Card sx={{ mt: 8 }}>
        <CardContent>
          <Box sx={{ display: 'flex', alignItems: 'center', mb: 3 }}>
            <StorageIcon sx={{ mr: 1, color: 'error.main' }} />
            <Typography variant="h5">{t('tools.uc.title')}</Typography>
          </Box>
          <Alert severity="error" sx={{ mb: 2 }}>
            {error}
          </Alert>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card sx={{ mt: 8 }}>
      <CardContent>
        <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
          <StorageIcon sx={{ mr: 1, color: 'primary.main' }} />
          <Typography variant="h5">{t('tools.uc.title')}</Typography>
        </Box>

        {tools.length === 0 ? (
          <Alert severity="info">{t('tools.uc.noTools')}</Alert>
        ) : (
          <TableContainer component={Paper} sx={{ mt: 0 }}>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell>{t('tools.uc.columns.name')}</TableCell>
                  <TableCell>{t('tools.uc.columns.catalog')}</TableCell>
                  <TableCell>{t('tools.uc.columns.schema')}</TableCell>
                  <TableCell>{t('tools.uc.columns.description')}</TableCell>
                  <TableCell>{t('tools.uc.columns.actions')}</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {tools.map((tool) => (
                  <Fragment key={tool.name}>
                    <TableRow>
                      <TableCell component="th" scope="row">
                        <Typography variant="subtitle2">{tool.name}</Typography>
                      </TableCell>
                      <TableCell>
                        <Chip 
                          label={tool.catalog} 
                          size="small"
                          color="primary"
                          variant="outlined"
                        />
                      </TableCell>
                      <TableCell>
                        <Chip 
                          label={tool.schema} 
                          size="small"
                          color="secondary"
                          variant="outlined"
                        />
                      </TableCell>
                      <TableCell>
                        {tool.comment ? 
                          (tool.comment.length > 100 ? 
                            `${tool.comment.substring(0, 100)}...` : 
                            tool.comment) :
                          t('tools.uc.details.noDescription')
                        }
                      </TableCell>
                      <TableCell>
                        <IconButton
                          onClick={() => setExpandedTool(expandedTool === tool.full_name ? null : tool.full_name)}
                        >
                          {expandedTool === tool.full_name ? <ExpandLessIcon /> : <ExpandMoreIcon />}
                        </IconButton>
                      </TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell style={{ paddingBottom: 0, paddingTop: 0 }} colSpan={5}>
                        <Collapse in={expandedTool === tool.full_name} timeout="auto" unmountOnExit>
                          <Box sx={{ margin: 1 }}>
                            <Typography variant="subtitle2" gutterBottom component="div">
                              {t('tools.uc.details.title')}:
                            </Typography>
                            <Box sx={{ mb: 2 }}>
                              <Typography variant="body2" component="pre" sx={{ 
                                whiteSpace: 'pre-wrap',
                                backgroundColor: '#f5f5f5',
                                p: 2,
                                borderRadius: 1
                              }}>
                                {JSON.stringify({
                                  fullName: tool.full_name,
                                  returnType: tool.return_type,
                                  parameters: tool.input_params.map(param => ({
                                    name: param.name,
                                    type: param.type,
                                    required: param.required
                                  })),
                                  comment: tool.comment
                                }, null, 2)}
                              </Typography>
                            </Box>
                          </Box>
                        </Collapse>
                      </TableCell>
                    </TableRow>
                  </Fragment>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        )}
      </CardContent>
    </Card>
  );
}

export default UCTools; 