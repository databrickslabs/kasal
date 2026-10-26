import { useTheme } from '@mui/material/styles';
import useMediaQuery from '@mui/material/useMediaQuery';

export function useResponsiveLayout() {
  const theme = useTheme();
  const isCompact = useMediaQuery(theme.breakpoints.down('md')); // < 900px
  const isMobile = useMediaQuery(theme.breakpoints.down('sm'));  // < 600px
  return { isCompact, isMobile };
}

export default useResponsiveLayout;
