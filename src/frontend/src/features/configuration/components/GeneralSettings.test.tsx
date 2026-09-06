import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, expect, it, vi } from 'vitest';
import GeneralSettings from './GeneralSettings';
import { LanguageService } from '../../../api/config/LanguageService';
vi.mock('../../../api/config/LanguageService', () => {
  const service = { getCurrentLanguage: vi.fn().mockResolvedValue('en'), setLanguage: vi.fn().mockResolvedValue(undefined) };
  return { LanguageService: { getInstance: () => service } };
});
vi.mock('../../../config/i18n/config', () => ({ LANGUAGES: { en: { nativeName: 'English' }, fr: { nativeName: 'Français' } } }));
vi.mock('react-i18next', () => ({ useTranslation: () => ({ t: (_key: string, options: { defaultValue?: string }) => options.defaultValue }) }));
beforeEach(() => vi.clearAllMocks());
it('saves language only on Save changes and allows cancelling a draft', async () => {
  render(<GeneralSettings />);
  await waitFor(() => expect(screen.getByRole('combobox', { name: 'Language' })).toHaveTextContent('English'));
  fireEvent.mouseDown(screen.getByRole('combobox', { name: 'Language' }));
  fireEvent.click(screen.getByRole('option', { name: 'Français' }));
  expect(LanguageService.getInstance().setLanguage).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole('button', { name: 'Cancel' }));
  expect(screen.getByRole('combobox', { name: 'Language' })).toHaveTextContent('English');
  fireEvent.mouseDown(screen.getByRole('combobox', { name: 'Language' }));
  fireEvent.click(screen.getByRole('option', { name: 'Français' }));
  fireEvent.click(screen.getByRole('button', { name: 'Save changes' }));
  await screen.findByText('Changes saved');
  expect(LanguageService.getInstance().setLanguage).toHaveBeenCalledWith('fr');
});
