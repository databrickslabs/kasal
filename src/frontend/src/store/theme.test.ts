import { beforeEach, describe, expect, it } from 'vitest';
import { useThemeStore } from './theme';
import { getThemeOptions } from '../theme/theme';
import { createTheme } from '@mui/material/styles';
beforeEach(() => { localStorage.clear(); useThemeStore.setState({ currentTheme: 'professional', isDarkMode: false }); });
describe('Shared Kasal appearance', () => {
  it('switches immediately and restores the saved appearance', async () => {
    const toggle = useThemeStore.getState().toggleTheme();
    expect(useThemeStore.getState().isDarkMode).toBe(true);
    await toggle;
    useThemeStore.setState({ currentTheme: 'professional', isDarkMode: false });
    await useThemeStore.getState().initializeTheme();
    expect(useThemeStore.getState().isDarkMode).toBe(true);
    await useThemeStore.getState().toggleTheme();
    expect(useThemeStore.getState().isDarkMode).toBe(false);
  });
  it('maps retired color themes to the light appearance', async () => {
    localStorage.setItem('APP_THEME', JSON.stringify({ theme: 'vibrantCreative' }));
    await useThemeStore.getState().initializeTheme();
    expect(useThemeStore.getState().currentTheme).toBe('professional');
  });
  it('uses matching chat backgrounds and correct MUI mode for menus and dialogs', () => {
    const dark = createTheme(getThemeOptions('deepOcean'));
    const light = createTheme(getThemeOptions('professional'));
    expect(dark.palette.mode).toBe('dark');
    expect(dark.palette.background.paper).toBe('#1B1F23');
    expect(light.palette.mode).toBe('light');
    expect(light.palette.text.primary).toBe('#1B1F23');
  });
});
