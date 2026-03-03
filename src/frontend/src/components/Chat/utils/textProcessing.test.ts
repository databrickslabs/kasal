import { describe, it, expect } from 'vitest';
import { isHtmlDocument, isMarkdown, stripAnsiEscapes, urlPattern } from './textProcessing';

describe('textProcessing', () => {
  // =========================================================================
  // isHtmlDocument
  // =========================================================================

  describe('isHtmlDocument', () => {
    it('returns true for <!doctype html> prefix', () => {
      expect(isHtmlDocument('<!doctype html><html></html>')).toBe(true);
    });

    it('returns true for <!DOCTYPE HTML> (case insensitive)', () => {
      expect(isHtmlDocument('<!DOCTYPE HTML><html></html>')).toBe(true);
    });

    it('returns true for <html> prefix', () => {
      expect(isHtmlDocument('<html lang="en"><head></head></html>')).toBe(true);
    });

    it('returns true for <HTML> prefix (case insensitive)', () => {
      expect(isHtmlDocument('<HTML><body></body></HTML>')).toBe(true);
    });

    it('returns true with leading whitespace', () => {
      expect(isHtmlDocument('   \n  <!doctype html><html></html>')).toBe(true);
    });

    it('returns false for plain text', () => {
      expect(isHtmlDocument('Hello world')).toBe(false);
    });

    it('returns false for HTML fragment without doctype or html tag', () => {
      expect(isHtmlDocument('<div>not a document</div>')).toBe(false);
    });

    it('returns false for markdown with HTML-like content', () => {
      expect(isHtmlDocument('# Title\n<p>paragraph</p>')).toBe(false);
    });

    it('returns false for empty string', () => {
      expect(isHtmlDocument('')).toBe(false);
    });
  });

  // =========================================================================
  // isMarkdown
  // =========================================================================

  describe('isMarkdown', () => {
    it('detects headers', () => {
      expect(isMarkdown('# Heading')).toBe(true);
    });

    it('detects bold text', () => {
      expect(isMarkdown('Some **bold** text')).toBe(true);
    });

    it('detects italic text', () => {
      expect(isMarkdown('Some _italic_ text')).toBe(true);
    });

    it('detects links', () => {
      expect(isMarkdown('[link](https://example.com)')).toBe(true);
    });

    it('detects unordered lists', () => {
      expect(isMarkdown('- item one\n- item two')).toBe(true);
    });

    it('detects ordered lists', () => {
      expect(isMarkdown('1. first\n2. second')).toBe(true);
    });

    it('detects code blocks', () => {
      expect(isMarkdown('```\ncode\n```')).toBe(true);
    });

    it('detects blockquotes', () => {
      expect(isMarkdown('> quoted text')).toBe(true);
    });

    it('returns false for plain text', () => {
      expect(isMarkdown('Hello world')).toBe(false);
    });
  });

  // =========================================================================
  // stripAnsiEscapes
  // =========================================================================

  describe('stripAnsiEscapes', () => {
    it('returns empty string for empty input', () => {
      expect(stripAnsiEscapes('')).toBe('');
    });

    it('returns plain text unchanged', () => {
      expect(stripAnsiEscapes('hello world')).toBe('hello world');
    });

    it('strips ANSI color codes', () => {
      const ESC = String.fromCharCode(27);
      const input = `${ESC}[31mred text${ESC}[0m`;
      expect(stripAnsiEscapes(input)).toBe('red text');
    });

    it('strips multiple ANSI sequences', () => {
      const ESC = String.fromCharCode(27);
      const input = `${ESC}[1m${ESC}[32mbold green${ESC}[0m normal`;
      expect(stripAnsiEscapes(input)).toBe('bold green normal');
    });
  });

  // =========================================================================
  // urlPattern
  // =========================================================================

  describe('urlPattern', () => {
    it('matches http URLs', () => {
      const text = 'Visit http://example.com now';
      const matches = text.match(urlPattern);
      expect(matches).toEqual(['http://example.com']);
    });

    it('matches https URLs', () => {
      const text = 'Visit https://example.com now';
      const matches = text.match(urlPattern);
      expect(matches).toEqual(['https://example.com']);
    });

    it('returns null for text without URLs', () => {
      const text = 'No links here';
      const matches = text.match(urlPattern);
      expect(matches).toBeNull();
    });
  });
});
