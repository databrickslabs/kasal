import React, { useLayoutEffect, useState } from 'react';

// Width of the composer pop-up menus (matches Tailwind `w-72` = 18rem).
const MENU_WIDTH = 288;

/**
 * Anchored fixed-position style for a composer pop-up menu.
 *
 * Shared by every pill in the composer's control row rather than owned by
 * ChatInput: each menu must anchor to ITS OWN trigger. Reusing another pill's
 * computed style positions the menu against the wrong element — and against a
 * stale rect while that pill is closed, which puts it off-screen.
 *
 * The pills sit inside the chat's `overflow-hidden` layout containers (the
 * <main> column + the chat scroll wrapper). An `absolute` menu that extends
 * past those bounds — which happens once the sidebar narrows <main> — gets
 * CLIPPED, so the menu appears to vanish "behind" the sidebar. `position: fixed`
 * is positioned against the viewport and is NOT clipped by an ancestor's
 * overflow, while keeping the menu a DOM child of the picker wrapper (so the
 * outside-click `contains()` checks and the #kasal-chat-root theme/Tailwind
 * scope both still apply). We compute the coords from the trigger: right-edge
 * aligned to it, opening up or down per `placement`, clamped to the viewport.
 */
export function useAnchoredFixedStyle(
  open: boolean,
  anchorRef: React.RefObject<HTMLElement>,
  placement: 'up' | 'down',
): React.CSSProperties {
  const [style, setStyle] = useState<React.CSSProperties>({ position: 'fixed' });
  useLayoutEffect(() => {
    const el = anchorRef.current;
    if (!open || !el) return;
    const update = () => {
      const r = el.getBoundingClientRect();
      const left = Math.max(8, Math.min(r.right - MENU_WIDTH, window.innerWidth - MENU_WIDTH - 8));
      setStyle(
        placement === 'down'
          ? { position: 'fixed', left, top: r.bottom + 8, width: MENU_WIDTH }
          : { position: 'fixed', left, bottom: window.innerHeight - r.top + 8, width: MENU_WIDTH },
      );
    };
    update();
    window.addEventListener('resize', update);
    window.addEventListener('scroll', update, true);
    return () => {
      window.removeEventListener('resize', update);
      window.removeEventListener('scroll', update, true);
    };
  }, [open, placement, anchorRef]);
  return style;
}
