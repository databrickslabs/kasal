import { useEffect } from 'react';
import { useUILayoutStore } from '../../store/uiLayout';

/** Reframe the visible canvas after its column resizes; never change node positions. */
export function useCanvasViewportResize(fitCrew: () => void, fitFlow: () => void) {
  const mode = useUILayoutStore(state => state.appMode);
  useEffect(() => {
    if (mode === 'chat' || typeof ResizeObserver === 'undefined') return;
    let observer: ResizeObserver | undefined;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let frame: number;
    let attempts = 0;
    const attach = () => {
      const element = Array.from(document.querySelectorAll<HTMLElement>('.react-flow')).find(el => el.getBoundingClientRect().width > 0);
      if (!element) { if (++attempts < 30) frame = requestAnimationFrame(attach); return; }
      let previous = '';
      observer = new ResizeObserver(entries => {
        const rect = entries[0]?.contentRect;
        if (!rect?.width || !rect.height) return;
        const size = `${Math.round(rect.width)}:${Math.round(rect.height)}`;
        if (size === previous) return;
        previous = size;
        clearTimeout(timer);
        timer = setTimeout(mode === 'flow' ? fitFlow : fitCrew, 120);
      });
      observer.observe(element);
    };
    frame = requestAnimationFrame(attach);
    return () => { cancelAnimationFrame(frame); clearTimeout(timer); observer?.disconnect(); };
  }, [mode, fitCrew, fitFlow]);
}
