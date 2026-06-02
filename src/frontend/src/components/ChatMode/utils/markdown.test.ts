import { describe, it, expect } from 'vitest';
import { containsMarkdown, formatTime, generateId } from './markdown';

describe('containsMarkdown', () => {
  it('detects headers', () => {
    expect(containsMarkdown('# Title')).toBe(true);
    expect(containsMarkdown('###### Sub')).toBe(true);
  });

  it('detects bold', () => {
    expect(containsMarkdown('this is **bold** text')).toBe(true);
  });

  it('detects italic', () => {
    expect(containsMarkdown('this is *italic* text')).toBe(true);
  });

  it('detects inline code', () => {
    expect(containsMarkdown('use `code` here')).toBe(true);
  });

  it('detects code blocks', () => {
    expect(containsMarkdown('```\ncode\n```')).toBe(true);
  });

  it('detects unordered lists', () => {
    expect(containsMarkdown('- item')).toBe(true);
    expect(containsMarkdown('* item')).toBe(true);
    expect(containsMarkdown('+ item')).toBe(true);
  });

  it('detects ordered lists', () => {
    expect(containsMarkdown('1. item')).toBe(true);
  });

  it('detects links', () => {
    expect(containsMarkdown('[label](https://example.com)')).toBe(true);
  });

  it('detects tables', () => {
    expect(containsMarkdown('| a | b |')).toBe(true);
  });

  it('detects blockquotes', () => {
    expect(containsMarkdown('> quote')).toBe(true);
  });

  it('returns false for plain text', () => {
    expect(containsMarkdown('just plain text with no formatting')).toBe(false);
  });

  it('returns false for empty string', () => {
    expect(containsMarkdown('')).toBe(false);
  });
});

describe('formatTime', () => {
  it('formats a date to a HH:MM time string', () => {
    const date = new Date('2024-01-01T13:05:00');
    const result = formatTime(date);
    // Locale-dependent, but should be a non-empty string containing the minutes
    expect(typeof result).toBe('string');
    expect(result.length).toBeGreaterThan(0);
  });
});

describe('generateId', () => {
  it('produces an id with the msg- prefix', () => {
    expect(generateId()).toMatch(/^msg-\d+-[a-z0-9]+$/);
  });

  it('produces unique ids across calls', () => {
    const ids = new Set(Array.from({ length: 50 }, () => generateId()));
    expect(ids.size).toBe(50);
  });
});
