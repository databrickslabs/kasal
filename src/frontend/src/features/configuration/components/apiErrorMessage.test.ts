import { describe, expect, it } from 'vitest';

import { apiErrorMessage } from './apiErrorMessage';

describe('apiErrorMessage', () => {
  it('reads detail, which is what a KasalError such as a 400 refusal sends', () => {
    const err = { response: { status: 400, data: { detail: 'Export is not available for this database' } } };
    expect(apiErrorMessage(err, 'Failed to export database')).toBe('Export is not available for this database');
  });

  it('falls back to error, then to the fallback', () => {
    expect(apiErrorMessage({ response: { data: { error: 'Volume not found' } } }, 'x')).toBe('Volume not found');
    expect(apiErrorMessage({ response: { data: {} } }, 'Failed to import database')).toBe('Failed to import database');
    expect(apiErrorMessage(new Error('network'), 'Failed to load backups')).toBe('Failed to load backups');
  });

  it('never returns a 422 list of validation issues, which is not text', () => {
    const err = { response: { status: 422, data: { detail: [{ loc: ['body', 'path'], msg: 'field required' }] } } };
    expect(apiErrorMessage(err, 'Failed to import database')).toBe('Failed to import database');
  });
});
