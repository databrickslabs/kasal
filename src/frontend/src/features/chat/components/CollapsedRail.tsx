import React from 'react';
import SidebarAccountActions from '../../../components/SidebarAccountActions';

/**
 * The chat sidebar when it is CLOSED: a slim vertical icon rail instead of
 * nothing at all. Keeps the sidebar's key actions one click away — start a
 * new chat, and the dark/light toggle that otherwise lives in the sidebar
 * footer. (Expanding back is the top-bar SidebarToggle — the one fixed
 * control for both directions.)
 */
const CollapsedRail: React.FC<{ onNewChat: () => void; onOpenSettings?: () => void }> = ({ onNewChat, onOpenSettings }) => {
  const iconButton =
    'w-9 h-9 rounded-xl flex items-center justify-center transition-colors hover:bg-[var(--bg-rail-hover)]';

  return (
    <aside
      data-testid="collapsed-rail"
      className="w-12 flex flex-col items-center flex-shrink-0 pt-3"
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

      <SidebarAccountActions onOpenSettings={onOpenSettings} />
    </aside>
  );
};

export default CollapsedRail;
