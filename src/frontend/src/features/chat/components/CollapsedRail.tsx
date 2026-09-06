import React from 'react';
import SettingsIcon from '@mui/icons-material/Settings';
import ThemeModeIcon from '../../../components/ThemeModeIcon';
import GroupSelector from '../../groups/components/GroupSelector';
import { useAppStore } from '../store/appStore';

/**
 * The chat sidebar when it is CLOSED: a slim vertical icon rail instead of
 * nothing at all. Keeps the sidebar's key actions one click away — start a
 * new chat, and the dark/light toggle that otherwise lives in the sidebar
 * footer. (Expanding back is the top-bar SidebarToggle — the one fixed
 * control for both directions.)
 */
const CollapsedRail: React.FC<{ onNewChat: () => void; onOpenSettings?: () => void }> = ({ onNewChat, onOpenSettings }) => {
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
      {onOpenSettings && <button type="button" aria-label="Settings" title="Settings" onClick={onOpenSettings}
        className={iconButton} style={{ color: 'var(--text-secondary)', width: 40, height: 40 }}>
        <SettingsIcon sx={{ fontSize: 20 }} />
      </button>}

      <button
        type="button"
        onClick={toggleTheme}
        className={iconButton}
        style={{ color: 'var(--text-secondary)', width: 40, height: 40 }}
        aria-label={isDark ? 'Switch to light mode' : 'Switch to dark mode'}
      >
        <ThemeModeIcon dark={isDark} />
      </button>
      <GroupSelector />
    </aside>
  );
};

export default CollapsedRail;
