import { useMemo, type ReactNode } from 'react';
import { Box } from '@mui/material';
import { kasalStageSurface } from '../../../theme/kasalSurfaces';
import { alpha, createTheme, ThemeProvider, useTheme } from '@mui/material/styles';

/** A shared presentation layer for settings and the dialogs opened from them.
 * Scoped here so builders, chat, and standalone configuration tools keep their
 * own presentation. Explicit page-title markers avoid hiding subsection headings.
 */
export default function SettingsContent({ children }: { children: ReactNode }) {
  const outer = useTheme();
  const theme = useMemo(() => {
    const ink = outer.palette.text.primary;
    const dark = outer.palette.mode === 'dark';
    const surface = 'transparent';
    const field = alpha(ink, 0.035);
    const hover = alpha(ink, 0.06);
    const focus = {
      outline: `2px solid ${outer.palette.primary.main}`,
      outlineOffset: 3,
    };
    return createTheme(outer, {
      typography: {
        h4: { fontSize: 20, fontWeight: 600, letterSpacing: '-0.02em' },
        h5: { fontSize: 18, fontWeight: 600, letterSpacing: '-0.015em' },
        h6: { fontSize: 16, fontWeight: 600, letterSpacing: '-0.01em' },
        subtitle1: { fontSize: 14, fontWeight: 600 },
        body1: { fontSize: 14, lineHeight: 1.65 },
        body2: { fontSize: 13, lineHeight: 1.65 },
        caption: { fontSize: 12, lineHeight: 1.6 },
      },
      components: {
        MuiPaper: { defaultProps: { elevation: 0 }, styleOverrides: { root: {
          '&&': { border: 0, borderRadius: 16, backgroundImage: 'none', boxShadow: 'none', backgroundColor: surface },
          '& .MuiPaper-root': { backgroundColor: 'transparent' },
          '&.MuiDialog-paper, &.MuiPopover-paper, &.MuiMenu-paper': kasalStageSurface(dark),
          // Autocomplete lists float over the form; transparent section surfaces
          // would let the fields underneath show through their options.
          '&&.MuiAutocomplete-paper': {
            ...kasalStageSurface(dark),
            marginBlock: 6,
            borderRadius: 12,
            boxShadow: `0 8px 24px ${alpha('#000', dark ? 0.3 : 0.12)}`,
          },
        } } },
        MuiCardContent: { styleOverrides: { root: { padding: 24, '&:last-child': { paddingBottom: 24 } } } },
        MuiButton: { defaultProps: { disableElevation: true }, styleOverrides: { root: {
          borderRadius: 10, padding: '8px 14px', fontSize: 13, fontWeight: 550,
          textTransform: 'none', '&.Mui-focusVisible': focus,
        }, outlined: { borderColor: 'transparent', backgroundColor: hover,
          '&:hover': { borderColor: 'transparent', backgroundColor: alpha(ink, 0.1) } },
        sizeSmall: { padding: '6px 10px', fontSize: 12 } } },
        MuiIconButton: { styleOverrides: { root: { borderRadius: 10, '&.Mui-focusVisible': focus } } },
        MuiTextField: { defaultProps: { size: 'small', variant: 'outlined' } },
        MuiFormControl: { defaultProps: { size: 'small' } },
        MuiOutlinedInput: { styleOverrides: { root: {
          borderRadius: 10, backgroundColor: field, fontSize: 13,
          '& .MuiOutlinedInput-notchedOutline': { borderColor: 'transparent' },
          '&:hover .MuiOutlinedInput-notchedOutline': { borderColor: alpha(ink, 0.2) },
          '&.Mui-focused .MuiOutlinedInput-notchedOutline': { borderColor: outer.palette.primary.main, borderWidth: 1 },
          '&.Mui-error .MuiOutlinedInput-notchedOutline': { borderColor: outer.palette.error.main },
          '&.Mui-disabled': { backgroundColor: alpha(ink, 0.02) },
          '&.Mui-disabled .MuiOutlinedInput-notchedOutline': { borderColor: 'transparent' },
        } } },
        MuiFormHelperText: { styleOverrides: { root: { marginLeft: 2, marginTop: 6, fontSize: 12, lineHeight: 1.55 } } },
        MuiInputLabel: { styleOverrides: { root: { fontSize: 13 } } },
        MuiTabs: { styleOverrides: { root: { '&&': { minHeight: 42, borderBottom: 0, marginBottom: 24 } },
          flexContainer: { gap: 6 }, indicator: { display: 'none' } } },
        MuiTab: { styleOverrides: { root: { minHeight: 40, padding: '8px 14px',
          minWidth: 0, borderRadius: 10, fontSize: 13, textTransform: 'none',
          '&.Mui-selected': { backgroundColor: hover, color: ink, fontWeight: 600 },
          '&.Mui-focusVisible': focus } } },
        MuiTableContainer: { styleOverrides: { root: { borderRadius: 14 } } },
        MuiTableCell: { styleOverrides: { root: { padding: '14px 16px', fontSize: 13, borderBottom: `1px solid ${alpha(ink, 0.045)}` },
          head: { backgroundColor: 'transparent', fontSize: 11, fontWeight: 600, letterSpacing: '0.025em', color: outer.palette.text.secondary } } },
        MuiTableRow: { styleOverrides: { root: { '&:last-child td': { borderBottom: 0 }, '&.MuiTableRow-hover:hover': { backgroundColor: hover } } } },
        MuiChip: { styleOverrides: { root: { borderRadius: 7, fontWeight: 500, fontSize: 11 },
          outlined: { borderColor: alpha(ink, 0.1) } } },
        MuiAccordion: { defaultProps: { disableGutters: true, elevation: 0 }, styleOverrides: {
          root: { '&&': { marginBottom: 12, borderRadius: 16 }, '&:before': { display: 'none' } },
        } },
        MuiAccordionSummary: { styleOverrides: { root: { padding: '4px 20px', minHeight: 58, '&.Mui-focusVisible': focus },
          content: { gap: 8, '& .MuiBox-root': { flexWrap: 'wrap' } } } },
        MuiAccordionDetails: { styleOverrides: { root: { padding: '4px 20px 24px' } } },
        MuiDivider: { styleOverrides: { root: { borderColor: alpha(ink, 0.06) } } },
        MuiAlert: { styleOverrides: { root: { borderRadius: 12, border: 0, fontSize: 13,
          [outer.breakpoints.down('sm')]: { flexWrap: 'wrap', '& .MuiAlert-message': { flex: 1 },
            '& .MuiAlert-action': { width: '100%', margin: '4px 0 0 28px', padding: 0 } } },
          icon: { fontSize: 19, alignItems: 'flex-start', paddingTop: 9 } } },
        MuiDialog: { styleOverrides: { paper: { '&&': { backgroundColor: outer.palette.background.paper, borderRadius: 20 } } } },
        MuiDialogTitle: { styleOverrides: { root: { padding: '24px 24px 12px', fontSize: 18, fontWeight: 600 } } },
        MuiDialogContent: { styleOverrides: { root: { padding: '16px 24px 24px' } } },
        MuiDialogActions: { styleOverrides: { root: { padding: '16px 24px 24px', gap: 8 } } },
        MuiMenu: { styleOverrides: { paper: { '&&': { backgroundColor: outer.palette.background.paper,
          boxShadow: `0 8px 32px ${alpha('#000', outer.palette.mode === 'dark' ? 0.3 : 0.12)}` } } } },
        MuiMenuItem: { styleOverrides: { root: { margin: '2px 6px', borderRadius: 8, fontSize: 13, minHeight: 36 } } },
        MuiListItemText: { defaultProps: { primaryTypographyProps: { fontSize: 13, fontWeight: 550 }, secondaryTypographyProps: { fontSize: 12, lineHeight: 1.6 } } },
      },
    });
  }, [outer]);

  return <ThemeProvider theme={theme}>
    <Box data-settings-content sx={{
      '& > .MuiBox-root': { p: 0 },
      '& [data-settings-page-title]': { display: 'none' },
      '& [data-settings-toolbar]': { gap: 2, flexWrap: 'wrap', alignItems: 'center' },
      '& [data-settings-section]': { p: { xs: 2, sm: 3 }, borderRadius: '16px', bgcolor: 'transparent' },
      '& pre': { overflowX: 'auto', borderRadius: 2, fontSize: 12 },
      '& .MuiFormControlLabel-label': { fontSize: 13 },
    }}>{children}</Box>
  </ThemeProvider>;
}
