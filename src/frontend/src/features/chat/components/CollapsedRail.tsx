import React from 'react';
import { useAppStore } from '../store/appStore';

/**
 * The chat sidebar when it is CLOSED: a slim vertical icon rail instead of
 * nothing at all. Keeps the sidebar's key actions one click away — start a
 * new chat, and the dark/light toggle that otherwise lives in the sidebar
 * footer. (Expanding back is the top-bar SidebarToggle — the one fixed
 * control for both directions.)
 */
const CollapsedRail: React.FC<{ onNewChat: () => void }> = ({ onNewChat }) => {
  const isDark = useAppStore((s) => s.theme) === 'dark';
  const toggleTheme = useAppStore((s) => s.toggleTheme);

  const iconButton =
    'w-9 h-9 rounded-xl flex items-center justify-center transition-colors hover:bg-[var(--bg-rail-hover)]';

  return (
    <aside
      data-testid="collapsed-rail"
      className="w-12 flex flex-col items-center flex-shrink-0 py-3"
      style={{ backgroundColor: 'var(--bg-rail)' }}
    >
      <button
        type="button"
        onClick={onNewChat}
        className={iconButton}
        style={{ color: 'var(--text-secondary)' }}
        aria-label="New chat"
      >
        <svg className="w-[18px] h-[18px]" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M12 4.5v15m7.5-7.5h-15" />
        </svg>
      </button>

      <div className="flex-1" />

      <button
        type="button"
        onClick={toggleTheme}
        className={iconButton}
        style={{ color: 'var(--text-secondary)' }}
        aria-label={isDark ? 'Switch to light mode' : 'Switch to dark mode'}
      >
        {isDark ? (
          // Sun — currently dark, click for light
          <svg className="w-[18px] h-[18px]" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
            <circle cx="12" cy="12" r="4" />
            <path strokeLinecap="round" strokeLinejoin="round" d="M12 2v2m0 16v2M4.93 4.93l1.41 1.41m11.32 11.32l1.41 1.41M2 12h2m16 0h2M4.93 19.07l1.41-1.41m11.32-11.32l1.41-1.41" />
          </svg>
        ) : (
          // Moon — currently light, click for dark
          <svg className="w-[18px] h-[18px]" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" />
          </svg>
        )}
      </button>
    </aside>
  );
};

export default CollapsedRail;
