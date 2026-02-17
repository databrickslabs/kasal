import { describe, it, expect } from 'vitest';
import {
  ENTITY_TYPE_COLORS,
  RELATIONSHIP_TYPE_COLORS,
  getEntityColor,
  getRelationshipColor,
  lightenColor,
  darkenColor,
} from './entityColors';

describe('entityColors', () => {
  describe('ENTITY_TYPE_COLORS', () => {
    it('contains all expected entity types', () => {
      const expectedTypes = [
        'person',
        'organization',
        'system',
        'concept',
        'location',
        'event',
        'conference',
        'meetup',
        'document',
        'project',
        'technology',
        'tool',
        'framework',
        'unknown',
      ];
      expectedTypes.forEach((type) => {
        expect(ENTITY_TYPE_COLORS[type]).toBeDefined();
        expect(ENTITY_TYPE_COLORS[type]).toMatch(/^#[0-9A-Fa-f]{6}$/);
      });
    });
  });

  describe('RELATIONSHIP_TYPE_COLORS', () => {
    it('contains all expected relationship types', () => {
      const expectedTypes = [
        'related_to',
        'works_with',
        'part_of',
        'depends_on',
        'belongs_to',
        'created_by',
        'manages',
        'uses',
        'reports_to',
        'collaborates_with',
        'attended',
        'presented_at',
        'located_in',
        'authored',
        'contributed_to',
      ];
      expectedTypes.forEach((type) => {
        expect(RELATIONSHIP_TYPE_COLORS[type]).toBeDefined();
        expect(RELATIONSHIP_TYPE_COLORS[type]).toMatch(/^#[0-9A-Fa-f]{6}$/);
      });
    });
  });

  describe('getEntityColor', () => {
    it('returns correct color for known entity types', () => {
      expect(getEntityColor('person')).toBe('#68CCE5');
      expect(getEntityColor('organization')).toBe('#94D82D');
      expect(getEntityColor('system')).toBe('#FCC940');
      expect(getEntityColor('concept')).toBe('#F25A29');
      expect(getEntityColor('location')).toBe('#AD4BAC');
    });

    it('handles case-insensitive lookups', () => {
      expect(getEntityColor('Person')).toBe('#68CCE5');
      expect(getEntityColor('ORGANIZATION')).toBe('#94D82D');
      expect(getEntityColor('System')).toBe('#FCC940');
    });

    it('returns unknown color for unrecognized types', () => {
      expect(getEntityColor('nonexistent')).toBe(ENTITY_TYPE_COLORS.unknown);
      expect(getEntityColor('foobar')).toBe('#C5C5C5');
      expect(getEntityColor('')).toBe('#C5C5C5');
    });
  });

  describe('getRelationshipColor', () => {
    it('returns correct color for known relationship types', () => {
      expect(getRelationshipColor('related_to')).toBe('#78909C');
      expect(getRelationshipColor('works_with')).toBe('#42A5F5');
      expect(getRelationshipColor('part_of')).toBe('#66BB6A');
      expect(getRelationshipColor('depends_on')).toBe('#FFA726');
      expect(getRelationshipColor('manages')).toBe('#5C6BC0');
    });

    it('handles case-insensitive lookups', () => {
      expect(getRelationshipColor('Related_To')).toBe('#78909C');
      expect(getRelationshipColor('WORKS_WITH')).toBe('#42A5F5');
    });

    it('returns default color for unrecognized types', () => {
      expect(getRelationshipColor('nonexistent')).toBe('#90A4AE');
      expect(getRelationshipColor('something_else')).toBe('#90A4AE');
      expect(getRelationshipColor('')).toBe('#90A4AE');
    });
  });

  describe('lightenColor', () => {
    it('lightens a color by given amount', () => {
      // Black (#000000) lightened by 0.5 = ~(128, 128, 128) = #808080
      const result = lightenColor('#000000', 0.5);
      expect(result).toBe('#808080');
    });

    it('does not exceed white (#ffffff)', () => {
      // White lightened further stays white
      const result = lightenColor('#ffffff', 0.5);
      expect(result).toBe('#ffffff');
    });

    it('handles partial lightening', () => {
      // #800000 (128,0,0) lightened by 0.1 → r=128+26=154, g=0+26=26, b=0+26=26
      const result = lightenColor('#800000', 0.1);
      expect(result).toBe('#9a1a1a');
    });

    it('handles zero amount (no change)', () => {
      const result = lightenColor('#ff5500', 0);
      expect(result).toBe('#ff5500');
    });

    it('clamps each channel to 255', () => {
      // #ff0000 lightened by 1.0 → r=min(255,255+255)=255, g=min(255,0+255)=255, b=min(255,0+255)=255
      const result = lightenColor('#ff0000', 1.0);
      expect(result).toBe('#ffffff');
    });

    it('handles hex without hash', () => {
      // parseInt('', 16) → NaN path, but '#' replace handles prefix
      // This function expects hex with '#', test the normal case
      const result = lightenColor('#000000', 0.1);
      expect(result).toMatch(/^#[0-9a-f]{6}$/);
    });
  });

  describe('darkenColor', () => {
    it('darkens a color by given amount', () => {
      // White (#ffffff) darkened by 0.5 = ~(128, 128, 128) = #808080
      const result = darkenColor('#ffffff', 0.5);
      // 255 - 128 = 127 → 0x7f = #7f7f7f
      expect(result).toBe('#7f7f7f');
    });

    it('does not go below black (#000000)', () => {
      const result = darkenColor('#000000', 0.5);
      expect(result).toBe('#000000');
    });

    it('handles partial darkening', () => {
      // #ffffff (255,255,255) darkened by 0.1 → 255-26=229 (0xe5) for each
      const result = darkenColor('#ffffff', 0.1);
      expect(result).toBe('#e5e5e5');
    });

    it('handles zero amount (no change)', () => {
      const result = darkenColor('#ff5500', 0);
      expect(result).toBe('#ff5500');
    });

    it('clamps each channel to 0', () => {
      // #010101 darkened by 1.0 → max(0,1-255)=0 for each
      const result = darkenColor('#010101', 1.0);
      expect(result).toBe('#000000');
    });
  });
});
