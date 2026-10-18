import { isPersonalWorkspaceId } from '../../../store/user';

/**
 * Whether `groupId` is someone's Personal Space: exactly one of the users'
 * stored `personal_group_id`s, the rule the backend applies when it refuses to
 * duplicate one. Never a `user_` prefix: a team can have an id like that too.
 */
export function isAnyonesPersonalGroup(
  groupId: string,
  personalGroupIds: ReadonlyArray<string | null | undefined>,
): boolean {
  return personalGroupIds.some((personalGroupId) => isPersonalWorkspaceId(groupId, personalGroupId));
}
