import { describe, it, expect } from 'vitest';
import {
  buildDirective,
  buildPartialDirective,
  optionSpecs,
  optionVal,
  THEME_PRESETS,
  DEFAULT_THEME,
} from './uiConfigShared';

describe('buildPartialDirective — only phrases changed keys', () => {
  it('returns empty string when nothing changed', () => {
    expect(buildPartialDirective('presentation', {})).toBe('');
    expect(buildPartialDirective('presentation', undefined)).toBe('');
  });

  it('phrases only the keys present, capitalized and period-terminated', () => {
    // Just the slide count was changed → directive mentions only that.
    expect(buildPartialDirective('presentation', { slides: 10 })).toBe('Aim for about 10 slides.');
  });

  it('joins multiple changed clauses with semicolons', () => {
    const out = buildPartialDirective('presentation', { slides: 12, bullets: 3 });
    expect(out).toBe('Aim for about 12 slides; at most 3 bullet points per slide.');
  });

  it('ignores keys that are not options of the given type', () => {
    expect(buildPartialDirective('presentation', { bogus: 5 } as never)).toBe('');
  });

  it('returns empty for a type with no options (e.g. default)', () => {
    expect(buildPartialDirective('default', { anything: 1 } as never)).toBe('');
  });
});

describe('buildDirective — phrases every spec with defaults (workspace config)', () => {
  it('fills defaults for all specs of a type', () => {
    const out = buildDirective('quiz', {});
    expect(out.startsWith('Write exactly 5 questions')).toBe(true);
    expect(out.endsWith('.')).toBe(true);
  });
});

describe('shared specs sanity', () => {
  it('every option default round-trips through optionVal', () => {
    for (const type of ['presentation', 'dashboard', 'album', 'quiz', 'flashcards', 'report', 'genie', 'mindmap']) {
      for (const s of optionSpecs(type)) {
        expect(optionVal(undefined, s)).toBe(s.default);
      }
    }
  });

  it('the first preset is the Default palette', () => {
    expect(THEME_PRESETS[0].theme).toBe(DEFAULT_THEME);
  });
});
