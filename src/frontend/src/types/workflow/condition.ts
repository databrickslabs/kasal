export interface ConditionFormData {
  conditionType?: 'none' | 'and' | 'or' | 'router';
  targetNodes?: string[];
  routerCondition?: string;
}
