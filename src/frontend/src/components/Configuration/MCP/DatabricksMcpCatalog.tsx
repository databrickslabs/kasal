import React, { useCallback, useEffect, useState } from 'react';
import {
  Box,
  Typography,
  Paper,
  Switch,
  FormControlLabel,
  Chip,
  CircularProgress,
  TextField,
  Collapse,
  Alert,
  Button,
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import ChevronRightIcon from '@mui/icons-material/ChevronRight';
import StorageIcon from '@mui/icons-material/Storage';
import { useTranslation } from 'react-i18next';
import {
  MCPService,
  DatabricksMcpCatalog as Catalog,
  DatabricksMcpOption,
  DatabricksManagedMcpType,
  databricksMcpServerName,
} from '../../../api/MCPService';
import { MCPServerConfig } from './MCPConfiguration';

/**
 * Admin curation surface for the workspace's Databricks MCP servers.
 *
 * Lists everything the workspace exposes — external UC-connection MCPs, managed
 * leaves (Databricks SQL, Unity Catalog Functions), and the two-step Genie /
 * AI Search drill-downs (a workspace can have thousands of Genie spaces, so they
 * are searched on demand, never enumerated up front). Each entry has an
 * enable/disable toggle: enabling registers it as a workspace-scoped Kasal MCP
 * server (so it becomes selectable in the chat picker); disabling flips that
 * registration's `enabled` flag off but KEEPS the row (delete is separate).
 *
 * The toggle state is derived from the already-registered servers passed in, so
 * after any change the parent reloads its server list and this re-renders in sync.
 */

interface DatabricksMcpCatalogProps {
  /** The currently-registered Kasal MCP servers the toggles are derived from:
   *  the workspace's effective servers ('workspace' scope) or the base/global
   *  servers ('global' scope). Admins see all, including disabled ones. */
  registeredServers: MCPServerConfig[];
  /** Reload the parent's server list after an enable/disable so toggles re-sync. */
  onChanged: () => Promise<void> | void;
  /** Whether enabling registers a base/global server (system admin) or a
   *  workspace-scoped one (default). */
  scope?: 'workspace' | 'global';
  /** When true, render without the outer Paper/heading (e.g. inside a dialog
   *  that already provides its own title). */
  embedded?: boolean;
}

const managedLeafOption = (t: DatabricksManagedMcpType): DatabricksMcpOption => ({
  id: t.id,
  kind: t.kind,
  name: t.name,
  description: t.description,
  server_url: t.server_url || '',
});

const DatabricksMcpCatalog: React.FC<DatabricksMcpCatalogProps> = ({
  registeredServers,
  onChanged,
  scope = 'workspace',
  embedded = false,
}) => {
  const { t } = useTranslation();
  const [catalog, setCatalog] = useState<Catalog | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [busyId, setBusyId] = useState<string | null>(null);
  // Optimistic enabled state per option, so a toggle flips instantly without
  // waiting for (or visibly reloading from) the parent's server-list refresh.
  const [pendingEnabled, setPendingEnabled] = useState<Record<string, boolean>>({});
  const [filter, setFilter] = useState('');

  const [expandedType, setExpandedType] = useState<'genie' | 'ai-search' | null>(null);
  const [genieSearch, setGenieSearch] = useState('');
  const [genieOptions, setGenieOptions] = useState<DatabricksMcpOption[]>([]);
  const [genieLoaded, setGenieLoaded] = useState(false);
  const [genieNextToken, setGenieNextToken] = useState<string | null>(null);
  const [aiSearchOptions, setAiSearchOptions] = useState<DatabricksMcpOption[]>([]);
  const [aiSearchLoaded, setAiSearchLoaded] = useState(false);

  // Load the catalog once on mount (the workspace's set of Databricks MCPs does
  // not change as we enable/disable Kasal registrations).
  useEffect(() => {
    let cancelled = false;
    MCPService.getInstance()
      .getDatabricksCatalog()
      .then((c) => {
        if (!cancelled) setCatalog(c);
      })
      .catch((e: unknown) => {
        if (!cancelled) {
          setCatalog({ workspace_url: '', external: [], managed: [] });
          setLoadError(e instanceof Error ? e.message : 'Could not load Databricks MCPs');
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  // Genie spaces (searchable, paginated; debounced on the search box).
  useEffect(() => {
    if (expandedType !== 'genie') return;
    let cancelled = false;
    const timer = window.setTimeout(() => {
      MCPService.getInstance()
        .listGenieSpaces(genieSearch || undefined)
        .then(({ options, next_page_token }) => {
          if (cancelled) return;
          setGenieOptions(options);
          setGenieNextToken(next_page_token);
          setGenieLoaded(true);
        })
        .catch(() => {
          if (cancelled) return;
          setGenieOptions([]);
          setGenieNextToken(null);
          setGenieLoaded(true);
          setActionError('Could not load Genie spaces');
        });
    }, genieLoaded ? 250 : 0);
    return () => {
      cancelled = true;
      window.clearTimeout(timer);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [expandedType, genieSearch]);

  // AI Search indexes (loaded once on first expand).
  useEffect(() => {
    if (expandedType !== 'ai-search' || aiSearchLoaded) return;
    let cancelled = false;
    MCPService.getInstance()
      .listAiSearchIndexes()
      .then((options) => {
        if (cancelled) return;
        setAiSearchOptions(options);
        setAiSearchLoaded(true);
      })
      .catch(() => {
        if (cancelled) return;
        setAiSearchOptions([]);
        setAiSearchLoaded(true);
        setActionError('Could not load AI Search indexes');
      });
    return () => {
      cancelled = true;
    };
  }, [expandedType, aiSearchLoaded]);

  const loadMoreGenie = async (token: string) => {
    try {
      const { options, next_page_token } = await MCPService.getInstance().listGenieSpaces(
        genieSearch || undefined,
        token,
      );
      setGenieOptions((prev) => [...prev, ...options]);
      setGenieNextToken(next_page_token);
    } catch {
      setActionError('Could not load more Genie spaces');
    }
  };

  const matchFor = useCallback(
    (option: DatabricksMcpOption): MCPServerConfig | undefined => {
      const canonical = databricksMcpServerName(option);
      return registeredServers.find(
        (s) =>
          s.name.toLowerCase() === canonical ||
          (!!s.server_url && s.server_url === option.server_url),
      );
    },
    [registeredServers],
  );

  const handleToggle = async (option: DatabricksMcpOption) => {
    setBusyId(option.id);
    setActionError(null);
    try {
      const service = MCPService.getInstance();
      const match = matchFor(option);
      const desired = match ? !match.enabled : true;
      if (match) {
        // Already registered → flip its enabled flag (keep the row). In 'global'
        // scope that's the base server's availability; in 'workspace' scope it's
        // a per-workspace override (created on the fly for inherited globals).
        if (scope === 'global') await service.setGlobalAvailability(match.id, desired);
        else await service.setWorkspaceEnabled(match.id, desired);
      } else {
        // Not registered yet → register + enable it at the requested scope.
        await service.ensureDatabricksServer(option, scope);
      }
      // Flip the toggle optimistically so the row updates in place; the parent
      // then re-syncs silently (no dialog reload) to pick up ids for re-toggles.
      setPendingEnabled((p) => ({ ...p, [option.id]: desired }));
      await onChanged();
    } catch (e) {
      setActionError(e instanceof Error ? e.message : 'Could not update the Databricks MCP server');
    } finally {
      setBusyId(null);
    }
  };

  const query = filter.trim().toLowerCase();
  const nameMatches = (name: string) => !query || name.toLowerCase().includes(query);

  const optionRow = (option: DatabricksMcpOption, indent = false) => {
    const match = matchFor(option);
    // Optimistic override wins until the silent parent refresh catches up.
    const enabled = option.id in pendingEnabled ? pendingEnabled[option.id] : (!!match && match.enabled);
    const busy = busyId === option.id;
    return (
      <Box
        key={option.id}
        sx={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          gap: 2,
          py: 0.75,
          pl: indent ? 4 : 0,
        }}
      >
        <Box sx={{ minWidth: 0 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <Typography variant="body2" noWrap title={option.name}>
              {option.name}
            </Typography>
            <Chip
              label={option.kind.toUpperCase()}
              size="small"
              variant="outlined"
              sx={{ height: 18, fontSize: '0.65rem' }}
            />
          </Box>
          {option.server_url && (
            <Typography variant="caption" color="text.secondary" noWrap sx={{ display: 'block' }}>
              {option.server_url}
            </Typography>
          )}
        </Box>
        <FormControlLabel
          sx={{ flexShrink: 0, mr: 0 }}
          control={
            <Switch
              size="small"
              checked={enabled}
              disabled={busy}
              onChange={() => handleToggle(option)}
              inputProps={{ 'aria-label': `Enable ${option.name}` }}
            />
          }
          label={
            busy ? (
              <CircularProgress size={12} />
            ) : (
              <Typography variant="caption" sx={{ fontSize: '0.75rem' }}>
                {enabled
                  ? t('common.enabled', { defaultValue: 'Enabled' })
                  : t('common.disabled', { defaultValue: 'Disabled' })}
              </Typography>
            )
          }
        />
      </Box>
    );
  };

  const drillRow = (kind: 'genie' | 'ai-search', label: string) => (
    <Button
      fullWidth
      onClick={() => setExpandedType((prev) => (prev === kind ? null : kind))}
      startIcon={expandedType === kind ? <ExpandMoreIcon /> : <ChevronRightIcon />}
      sx={{ justifyContent: 'flex-start', textTransform: 'none', color: 'text.primary', py: 0.75 }}
    >
      {label}
    </Button>
  );

  const external = (catalog?.external ?? []).filter((o) => nameMatches(o.name));
  const managed = catalog?.managed ?? [];
  const visibleManaged = managed.filter((mt) => mt.expandable || nameMatches(mt.name));

  const inner = (
    <>
      {!embedded && (
        <>
          <Box sx={{ display: 'flex', alignItems: 'center', mb: 0.5 }}>
            <StorageIcon sx={{ mr: 1, color: 'primary.main', fontSize: '1.2rem' }} />
            <Typography variant="subtitle1" fontWeight="medium">
              {t('configuration.mcp.databricksCatalog', { defaultValue: 'Databricks MCP Catalog' })}
            </Typography>
          </Box>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            {t('configuration.mcp.databricksCatalogHelp', {
              defaultValue:
                'All MCP servers available in this Databricks workspace. Enable one to register it for this workspace and make it selectable in chat; disabling keeps it configured but hides it from the picker.',
            })}
          </Typography>
        </>
      )}

      {loadError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {loadError}
        </Alert>
      )}
      {actionError && (
        <Alert severity="error" sx={{ mb: 2 }} onClose={() => setActionError(null)}>
          {actionError}
        </Alert>
      )}

      {catalog === null ? (
        <Box sx={{ display: 'flex', alignItems: 'center', py: 2 }}>
          <CircularProgress size={18} />
          <Typography variant="body2" sx={{ ml: 1.5 }} color="text.secondary">
            {t('configuration.mcp.loadingCatalog', { defaultValue: 'Loading Databricks MCPs…' })}
          </Typography>
        </Box>
      ) : external.length === 0 && managed.length === 0 ? (
        <Typography variant="body2" color="text.secondary" align="center" sx={{ py: 3 }}>
          {t('configuration.mcp.noDatabricksMcps', {
            defaultValue: 'No Databricks MCP servers are available in this workspace.',
          })}
        </Typography>
      ) : (
        <>
          <TextField
            size="small"
            fullWidth
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            placeholder={t('configuration.mcp.searchCatalog', {
              defaultValue: 'Search Databricks MCPs…',
            })}
            inputProps={{ 'aria-label': 'Search Databricks MCPs' }}
            sx={{ mb: 1 }}
          />

          <Box sx={{ display: 'flex', flexDirection: 'column' }}>
            {/* External UC-connection MCPs + managed leaves are directly toggleable. */}
            {external.map((option) => optionRow(option))}
            {visibleManaged.map((mt) =>
              mt.expandable ? (
                <Box key={mt.id}>
                  {drillRow(mt.kind as 'genie' | 'ai-search', mt.name)}
                  <Collapse in={expandedType === mt.kind} unmountOnExit>
                    {mt.kind === 'genie' ? (
                      <Box sx={{ pl: 4, pb: 1 }}>
                        <TextField
                          size="small"
                          fullWidth
                          value={genieSearch}
                          onChange={(e) => setGenieSearch(e.target.value)}
                          placeholder={t('configuration.mcp.searchGenie', {
                            defaultValue: 'Search Genie spaces…',
                          })}
                          inputProps={{ 'aria-label': 'Search Genie spaces' }}
                          sx={{ mb: 1 }}
                        />
                        {!genieLoaded ? (
                          <Typography variant="caption" color="text.secondary">
                            {t('common.loading', { defaultValue: 'Loading…' })}
                          </Typography>
                        ) : genieOptions.length === 0 ? (
                          <Typography variant="caption" color="text.secondary">
                            {t('configuration.mcp.noGenieSpaces', { defaultValue: 'No spaces found' })}
                          </Typography>
                        ) : (
                          <>
                            {genieOptions.map((option) => optionRow(option, true))}
                            {genieNextToken && (
                              <Button
                                size="small"
                                onClick={() => void loadMoreGenie(genieNextToken)}
                                sx={{ textTransform: 'none', mt: 0.5 }}
                              >
                                {t('common.loadMore', { defaultValue: 'Load more…' })}
                              </Button>
                            )}
                          </>
                        )}
                      </Box>
                    ) : (
                      <Box sx={{ pl: 4, pb: 1 }}>
                        {!aiSearchLoaded ? (
                          <Typography variant="caption" color="text.secondary">
                            {t('common.loading', { defaultValue: 'Loading…' })}
                          </Typography>
                        ) : aiSearchOptions.length === 0 ? (
                          <Typography variant="caption" color="text.secondary">
                            {t('configuration.mcp.noAiSearchIndexes', {
                              defaultValue: 'No indexes found',
                            })}
                          </Typography>
                        ) : (
                          aiSearchOptions.map((option) => optionRow(option, true))
                        )}
                      </Box>
                    )}
                  </Collapse>
                </Box>
              ) : (
                optionRow(managedLeafOption(mt))
              ),
            )}
          </Box>
        </>
      )}
    </>
  );

  return embedded ? (
    <Box>{inner}</Box>
  ) : (
    <Paper variant="outlined" sx={{ p: 3, mb: 3, bgcolor: 'background.paper', borderRadius: 2 }}>
      {inner}
    </Paper>
  );
};

export default DatabricksMcpCatalog;
