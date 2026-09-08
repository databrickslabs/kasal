// Exact stage stops and surface shadows used by ChatMode/chat.css.
export const kasalStageBackground = (dark: boolean) => dark
  ? 'radial-gradient(ellipse 80% 62% at 50% 58%, #21272E 0%, #1B1F23 42%, #101317 100%)'
  : 'radial-gradient(ellipse 80% 62% at 50% 58%, #FFFFFF 0%, #FFFFFF 42%, #E7EBF2 100%)';

export const kasalSurfaceShadow = (dark: boolean) => dark
  ? '0 1px 2px rgba(0, 0, 0, 0.3), 0 4px 16px rgba(0, 0, 0, 0.35)'
  : '0 1px 2px rgba(16, 24, 40, 0.05), 0 4px 16px rgba(16, 24, 40, 0.06)';

// Align every workspace surface to the same viewport-sized stage, so adjoining
// panels do not restart the gradient and create visible seams.
export const kasalStageSurface = (dark: boolean) => ({
  backgroundColor: dark ? '#1B1F23' : '#FFFFFF',
  backgroundImage: kasalStageBackground(dark),
  backgroundAttachment: 'fixed' as const,
  backgroundPosition: '0 0',
  backgroundSize: '100vw 100vh',
  backgroundRepeat: 'no-repeat',
});

// Low-saturation accents echo Kasal's warm brand and neutral workspace surfaces.
export const kasalNodePalette = (dark: boolean, kind: 'agent' | 'task' | 'flow') => kind === 'agent'
  ? { surface: dark ? '#352F2D' : '#F2E7E2', badge: dark ? '#4A3832' : '#E7CCC0', accent: dark ? '#DFB4A4' : '#8D5342', chip: dark ? '#423936' : '#FAF3EF' }
  : kind === 'flow'
    ? { surface: dark ? '#302D38' : '#EAE5EF', badge: dark ? '#443C50' : '#D8CEE3', accent: dark ? '#C5B6D5' : '#6A577D', chip: dark ? '#3D3547' : '#F4F0F8' }
  : { surface: dark ? '#2B3430' : '#E8EEEA', badge: dark ? '#394B42' : '#CFDED5', accent: dark ? '#B0CBBB' : '#4D695C', chip: dark ? '#36413B' : '#F3F7F4' };

export const kasalNodeSurface = (dark: boolean, kind: 'agent' | 'task' | 'flow') => ({
  background: kasalNodePalette(dark, kind).surface,
  boxShadow: dark
    ? '0 6px 20px rgba(0,0,0,.22)'
    : '0 6px 18px rgba(27,31,35,.075), 0 1px 3px rgba(27,31,35,.04)',
});
