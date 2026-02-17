/**
 * Shared color utilities for entity graph visualization.
 * Provides consistent color mappings for entity types and relationship types,
 * plus helpers for gradient rendering.
 */

export const ENTITY_TYPE_COLORS: Record<string, string> = {
  person: '#68CCE5',
  organization: '#94D82D',
  system: '#FCC940',
  concept: '#F25A29',
  location: '#AD4BAC',
  event: '#D62728',
  conference: '#E91E63',
  meetup: '#FF5722',
  document: '#8FBC8F',
  project: '#FFB366',
  technology: '#00BCD4',
  tool: '#9C27B0',
  framework: '#3F51B5',
  unknown: '#C5C5C5',
};

export const RELATIONSHIP_TYPE_COLORS: Record<string, string> = {
  related_to: '#78909C',
  works_with: '#42A5F5',
  part_of: '#66BB6A',
  depends_on: '#FFA726',
  belongs_to: '#AB47BC',
  created_by: '#EF5350',
  manages: '#5C6BC0',
  uses: '#26A69A',
  reports_to: '#8D6E63',
  collaborates_with: '#29B6F6',
  attended: '#EC407A',
  presented_at: '#FF7043',
  located_in: '#7E57C2',
  authored: '#D4E157',
  contributed_to: '#FFCA28',
};

const DEFAULT_RELATIONSHIP_COLOR = '#90A4AE';

export function getEntityColor(type: string): string {
  return ENTITY_TYPE_COLORS[type.toLowerCase()] || ENTITY_TYPE_COLORS.unknown;
}

export function getRelationshipColor(type: string): string {
  return (
    RELATIONSHIP_TYPE_COLORS[type.toLowerCase()] || DEFAULT_RELATIONSHIP_COLOR
  );
}

/**
 * Lighten a hex color by a given amount (0-1).
 */
export function lightenColor(hex: string, amount: number): string {
  const num = parseInt(hex.replace('#', ''), 16);
  const r = Math.min(255, ((num >> 16) & 0xff) + Math.round(255 * amount));
  const g = Math.min(
    255,
    ((num >> 8) & 0xff) + Math.round(255 * amount),
  );
  const b = Math.min(255, (num & 0xff) + Math.round(255 * amount));
  return `#${((r << 16) | (g << 8) | b).toString(16).padStart(6, '0')}`;
}

/**
 * Darken a hex color by a given amount (0-1).
 */
export function darkenColor(hex: string, amount: number): string {
  const num = parseInt(hex.replace('#', ''), 16);
  const r = Math.max(0, ((num >> 16) & 0xff) - Math.round(255 * amount));
  const g = Math.max(0, ((num >> 8) & 0xff) - Math.round(255 * amount));
  const b = Math.max(0, (num & 0xff) - Math.round(255 * amount));
  return `#${((r << 16) | (g << 8) | b).toString(16).padStart(6, '0')}`;
}
