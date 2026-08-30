/**
 * Access — ONE home for "who can do what", combining the two screens that used
 * to split the question:
 *
 * - **People** (default lens): one row per user with the global platform
 *   flags (System Admin, Personal Space Manager) inline AND the user's
 *   teamspace memberships as editable role chips — the whole answer to
 *   "what can this user do?" in a single glance. Editing a chip changes the
 *   role in that teamspace (which is also what gates the Agent/Flow Builder
 *   surfaces: operators are chat-only); the "+" adds the user to a teamspace.
 * - **Teamspaces** lens: the existing space-lifecycle management
 *   (GroupManagement) unchanged — creating/deleting spaces is space-centric
 *   and stays that way.
 *
 * Data comes from APIs that already existed: UserService (users + global
 * flags), GroupService (groups, members, assign/update/remove). The
 * membership map is aggregated client-side (groups × members) — fine at
 * teamspace counts this product sees; revisit with a dedicated endpoint if a
 * deployment ever has hundreds of spaces.
 */
import React, { useCallback, useEffect, useMemo, useState } from 'react';
import {
  Alert,
  Box,
  Chip,
  CircularProgress,
  IconButton,
  Menu,
  MenuItem,
  ListItemIcon,
  ListItemText,
  Snackbar,
  Switch,
  Tab,
  Tabs,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Tooltip,
  Typography,
  Paper,
} from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import CheckIcon from '@mui/icons-material/Check';
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutline';
import GroupsIcon from '@mui/icons-material/Groups';
import PersonIcon from '@mui/icons-material/Person';
import { GroupService, Group, GroupUser } from '../../api/groups/GroupService';
import { UserService, User } from '../../api/groups/UserService';
import GroupManagement from './GroupManagement';

type Membership = { group: Group; member: GroupUser };
type RoleOption = 'admin' | 'editor' | 'operator';

const ROLE_OPTIONS: RoleOption[] = ['admin', 'editor', 'operator'];

const roleChipColor = (role: string): 'error' | 'primary' | 'success' | 'default' => {
  switch (role) {
    case 'admin':
      return 'error';
    case 'editor':
      return 'primary';
    case 'operator':
      return 'success';
    default:
      return 'default';
  }
};

const AccessManagement: React.FC = () => {
  const [lens, setLens] = useState<'people' | 'teamspaces'>('people');
  const [users, setUsers] = useState<User[]>([]);
  const [groups, setGroups] = useState<Group[]>([]);
  const [membersByGroup, setMembersByGroup] = useState<Record<string, GroupUser[]>>({});
  const [loading, setLoading] = useState(true);
  const [busyKey, setBusyKey] = useState<string | null>(null);
  const [notice, setNotice] = useState<{ message: string; severity: 'success' | 'error' } | null>(null);

  // Chip menu (change role / remove) + add-to-teamspace menu state.
  const [chipMenu, setChipMenu] = useState<{
    anchor: HTMLElement;
    user: User;
    membership: Membership;
  } | null>(null);
  const [addMenu, setAddMenu] = useState<{ anchor: HTMLElement; user: User } | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const [usersData, groupsData] = await Promise.all([
        UserService.getInstance().getUsers(),
        GroupService.getInstance().getGroups(),
      ]);
      const memberLists = await Promise.all(
        groupsData.map((g) =>
          GroupService.getInstance()
            .getGroupUsers(g.id)
            .catch(() => [] as GroupUser[]),
        ),
      );
      setUsers(usersData);
      setGroups(groupsData);
      setMembersByGroup(
        Object.fromEntries(groupsData.map((g, i) => [g.id, memberLists[i]])),
      );
    } catch (error) {
      console.error('Failed to load access data:', error);
      setNotice({ message: 'Failed to load access data', severity: 'error' });
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const membershipsByEmail = useMemo(() => {
    const map: Record<string, Membership[]> = {};
    for (const group of groups) {
      for (const member of membersByGroup[group.id] || []) {
        const key = member.email.toLowerCase();
        (map[key] ||= []).push({ group, member });
      }
    }
    return map;
  }, [groups, membersByGroup]);

  const refreshGroupMembers = useCallback(async (groupId: string) => {
    const members = await GroupService.getInstance()
      .getGroupUsers(groupId)
      .catch(() => [] as GroupUser[]);
    setMembersByGroup((prev) => ({ ...prev, [groupId]: members }));
  }, []);

  const toggleGlobalFlag = async (
    user: User,
    flag: 'is_system_admin' | 'is_personal_workspace_manager',
    value: boolean,
  ) => {
    setBusyKey(`${user.id}:${flag}`);
    try {
      const updated = await UserService.getInstance().updateUserPermissions(user.id, {
        [flag]: value,
      });
      setUsers((prev) => prev.map((u) => (u.id === user.id ? updated : u)));
    } catch (error) {
      console.error('Failed to update permission:', error);
      setNotice({ message: 'Failed to update permission', severity: 'error' });
    } finally {
      setBusyKey(null);
    }
  };

  const changeRole = async (membership: Membership, role: RoleOption) => {
    setChipMenu(null);
    if (membership.member.role === role) return;
    setBusyKey(membership.member.id);
    try {
      await GroupService.getInstance().updateGroupUser(
        membership.group.id,
        membership.member.user_id,
        // Choosing a role re-establishes intent: explicitly reset the
        // capability overrides to the role's defaults so a leftover override
        // cannot contradict the role the admin just picked.
        {
          role,
          allow_agent_builder: role !== 'operator',
          allow_flow_builder: role !== 'operator',
        },
      );
      await refreshGroupMembers(membership.group.id);
    } catch (error) {
      console.error('Failed to change role:', error);
      setNotice({ message: 'Failed to change the role', severity: 'error' });
    } finally {
      setBusyKey(null);
    }
  };

  const removeFromTeamspace = async (membership: Membership) => {
    setChipMenu(null);
    setBusyKey(membership.member.id);
    try {
      await GroupService.getInstance().removeUserFromGroup(
        membership.group.id,
        membership.member.user_id,
      );
      await refreshGroupMembers(membership.group.id);
    } catch (error) {
      console.error('Failed to remove member:', error);
      setNotice({ message: 'Failed to remove from the teamspace', severity: 'error' });
    } finally {
      setBusyKey(null);
    }
  };

  const addToTeamspace = async (user: User, group: Group) => {
    setAddMenu(null);
    setBusyKey(`${user.id}:add`);
    try {
      await GroupService.getInstance().assignUserToGroup(group.id, {
        user_email: user.email,
        role: 'operator',
      });
      await refreshGroupMembers(group.id);
    } catch (error) {
      console.error('Failed to add member:', error);
      setNotice({ message: 'Failed to add to the teamspace', severity: 'error' });
    } finally {
      setBusyKey(null);
    }
  };

  // Effective capability for a membership: override wins, else role-derived
  // (operator -> no builders). Mirrors the backend's my-groups computation.
  const effectiveCap = (
    member: GroupUser,
    field: 'allow_agent_builder' | 'allow_flow_builder',
  ): boolean => member[field] ?? member.role !== 'operator';

  const toggleCapability = async (
    membership: Membership,
    field: 'allow_agent_builder' | 'allow_flow_builder',
    value: boolean,
  ) => {
    setBusyKey(membership.member.id);
    try {
      await GroupService.getInstance().updateGroupUser(
        membership.group.id,
        membership.member.user_id,
        { [field]: value },
      );
      await refreshGroupMembers(membership.group.id);
      const surface = field === 'allow_agent_builder' ? 'Agent Builder' : 'Flow Builder';
    } catch (error) {
      console.error('Failed to update capability:', error);
      setNotice({ message: 'Failed to update the capability', severity: 'error' });
    } finally {
      setBusyKey(null);
    }
  };

  // The LIVE row behind the open chip menu. The menu captures a membership
  // snapshot on open; after a toggle the lists refresh but the snapshot does
  // not, so the switch appeared dead. Resolve against current state instead.
  const liveMenuMember = chipMenu
    ? (membersByGroup[chipMenu.membership.group.id] || []).find(
        (m) => m.id === chipMenu.membership.member.id,
      ) ?? chipMenu.membership.member
    : null;

  const groupsNotJoined = (user: User): Group[] => {
    const joined = new Set(
      (membershipsByEmail[user.email.toLowerCase()] || []).map((m) => m.group.id),
    );
    return groups.filter((g) => !joined.has(g.id));
  };

  return (
    <Box>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
        <Typography variant="h5" sx={{ fontWeight: 600 }}>
          Access
        </Typography>
      </Box>
      <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
        Who can do what — platform flags and per-teamspace roles in one place.
        Operators are chat-only: they see neither the Agent Builder nor the Flow
        Builder in that teamspace.
      </Typography>

      <Tabs
        value={lens}
        onChange={(_e, v) => setLens(v)}
        sx={{ mb: 2, borderBottom: 1, borderColor: 'divider' }}
      >
        <Tab icon={<PersonIcon fontSize="small" />} iconPosition="start" label="People" value="people" />
        <Tab icon={<GroupsIcon fontSize="small" />} iconPosition="start" label="Teamspaces" value="teamspaces" />
      </Tabs>

      {lens === 'teamspaces' && <GroupManagement />}

      {lens === 'people' && (
        loading ? (
          <Box sx={{ display: 'flex', justifyContent: 'center', py: 6 }}>
            <CircularProgress size={28} />
          </Box>
        ) : (
        <TableContainer component={Paper} variant="outlined" sx={{ borderRadius: 2 }}>
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>User</TableCell>
                <TableCell>Teamspaces &amp; roles</TableCell>
                <TableCell align="center">System Admin</TableCell>
                <TableCell align="center">Personal Space Manager</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {users.map((user) => {
                const memberships = membershipsByEmail[user.email.toLowerCase()] || [];
                return (
                  <TableRow key={user.id} hover>
                    <TableCell sx={{ whiteSpace: 'nowrap' }}>
                      <Typography variant="body2" sx={{ fontWeight: 600 }}>
                        {user.email}
                      </Typography>
                      <Typography variant="caption" color="text.secondary">
                        {user.last_login ? `Last login ${new Date(user.last_login).toLocaleDateString()}` : 'Never logged in'}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.75, alignItems: 'center' }}>
                        {memberships.map((m) => (
                          <Chip
                            key={m.member.id}
                            size="small"
                            label={`${m.group.name}: ${m.member.role}${
                              effectiveCap(m.member, 'allow_agent_builder') ||
                              effectiveCap(m.member, 'allow_flow_builder')
                                ? ''
                                : ' · chat only'
                            }`}
                            color={roleChipColor(m.member.role)}
                            variant="outlined"
                            disabled={busyKey === m.member.id}
                            onClick={(e) =>
                              setChipMenu({ anchor: e.currentTarget, user, membership: m })
                            }
                          />
                        ))}
                        {memberships.length === 0 && (
                          <Typography variant="caption" color="text.secondary">
                            No teamspaces
                          </Typography>
                        )}
                        {groupsNotJoined(user).length > 0 && (
                          <Tooltip title="Add to a teamspace">
                            <IconButton
                              size="small"
                              aria-label={`Add ${user.email} to a teamspace`}
                              onClick={(e) => setAddMenu({ anchor: e.currentTarget, user })}
                            >
                              <AddIcon fontSize="small" />
                            </IconButton>
                          </Tooltip>
                        )}
                      </Box>
                    </TableCell>
                    <TableCell align="center">
                      <Switch
                        size="small"
                        color="error"
                        checked={user.is_system_admin}
                        disabled={busyKey === `${user.id}:is_system_admin`}
                        onChange={(_e, v) => toggleGlobalFlag(user, 'is_system_admin', v)}
                        inputProps={{ 'aria-label': `System Admin for ${user.email}` }}
                      />
                    </TableCell>
                    <TableCell align="center">
                      <Switch
                        size="small"
                        checked={user.is_personal_workspace_manager}
                        disabled={busyKey === `${user.id}:is_personal_workspace_manager`}
                        onChange={(_e, v) =>
                          toggleGlobalFlag(user, 'is_personal_workspace_manager', v)
                        }
                        inputProps={{ 'aria-label': `Personal Space Manager for ${user.email}` }}
                      />
                    </TableCell>
                  </TableRow>
                );
              })}
              {users.length === 0 && (
                <TableRow>
                  <TableCell colSpan={4}>
                    <Typography variant="body2" color="text.secondary" sx={{ py: 2, textAlign: 'center' }}>
                      No users yet — users appear after their first login.
                    </Typography>
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </TableContainer>
        )
      )}

      {/* Role chip menu: pick a role or remove the membership */}
      <Menu
        anchorEl={chipMenu?.anchor ?? null}
        open={Boolean(chipMenu)}
        onClose={() => setChipMenu(null)}
      >
        {/* The permissions the user actually asked about, by NAME — effective
            state shown, click to override independent of the role. */}
        {chipMenu &&
          (
            [
              ['allow_agent_builder', 'Agent Builder'],
              ['allow_flow_builder', 'Flow Builder'],
            ] as const
          ).map(([field, label]) => {
            const member = liveMenuMember ?? chipMenu.membership.member;
            const enabled = effectiveCap(member, field);
            return (
              <MenuItem
                key={field}
                onClick={() => toggleCapability(chipMenu.membership, field, !enabled)}
              >
                <ListItemIcon sx={{ minWidth: 30 }}>
                  <Switch size="small" checked={enabled} readOnly tabIndex={-1} />
                </ListItemIcon>
                <ListItemText
                  primary={label}
                  secondary={enabled ? 'Allowed' : 'Hidden for this user'}
                />
              </MenuItem>
            );
          })}
        <MenuItem disabled sx={{ opacity: 0.7, fontSize: 12, minHeight: 28 }}>
          Role — sets the defaults above
        </MenuItem>
        {ROLE_OPTIONS.map((role) => (
          <MenuItem
            key={role}
            onClick={() => chipMenu && changeRole(chipMenu.membership, role)}
          >
            <ListItemIcon sx={{ minWidth: 30 }}>
              {(liveMenuMember ?? chipMenu?.membership.member)?.role === role ? (
                <CheckIcon fontSize="small" />
              ) : null}
            </ListItemIcon>
            <ListItemText
              primary={role.charAt(0).toUpperCase() + role.slice(1)}
              secondary={role === 'operator' ? 'Chat only — no builders' : undefined}
            />
          </MenuItem>
        ))}
        <MenuItem onClick={() => chipMenu && removeFromTeamspace(chipMenu.membership)}>
          <ListItemIcon sx={{ minWidth: 30 }}>
            <DeleteOutlineIcon fontSize="small" color="error" />
          </ListItemIcon>
          <ListItemText primary="Remove from teamspace" />
        </MenuItem>
      </Menu>

      {/* Add-to-teamspace menu */}
      <Menu
        anchorEl={addMenu?.anchor ?? null}
        open={Boolean(addMenu)}
        onClose={() => setAddMenu(null)}
      >
        {addMenu &&
          groupsNotJoined(addMenu.user).map((group) => (
            <MenuItem key={group.id} onClick={() => addToTeamspace(addMenu.user, group)}>
              <ListItemIcon sx={{ minWidth: 30 }}>
                <GroupsIcon fontSize="small" />
              </ListItemIcon>
              <ListItemText primary={group.name} secondary="Joins as operator" />
            </MenuItem>
          ))}
      </Menu>

      <Snackbar
        open={Boolean(notice)}
        autoHideDuration={4000}
        onClose={() => setNotice(null)}
      >
        <Alert severity={notice?.severity ?? 'success'} onClose={() => setNotice(null)}>
          {notice?.message}
        </Alert>
      </Snackbar>
    </Box>
  );
};

export default AccessManagement;
