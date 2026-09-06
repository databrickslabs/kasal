import React, { useEffect } from 'react';
import { createPortal } from 'react-dom';
import { X } from 'lucide-react';

/**
 * A near-fullscreen overlay for viewing a diagram or deck large. Rendered into
 * document.body via a portal so no `overflow:hidden`/transformed ancestor in the
 * chat can clip it. Closes on Esc or backdrop click.
 */
interface FullscreenModalProps {
  onClose: () => void;
  title?: string;
  /** Extra controls shown in the header (e.g. slide nav). */
  toolbar?: React.ReactNode;
  children: React.ReactNode;
}

const FullscreenModal: React.FC<FullscreenModalProps> = ({ onClose, title, toolbar, children }) => {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, [onClose]);

  return createPortal(
    <div
      onClick={onClose}
      style={{
        position: 'fixed',
        inset: 0,
        zIndex: 9999,
        background: 'rgba(0,0,0,0.6)',
        display: 'flex',
        flexDirection: 'column',
        padding: '3vh 3vw',
      }}
    >
      <div
        onClick={(e) => e.stopPropagation()}
        style={{
          display: 'flex',
          flexDirection: 'column',
          flex: 1,
          minHeight: 0,
          borderRadius: 12,
          overflow: 'hidden',
          background: 'var(--bg-primary, #fff)',
          boxShadow: '0 12px 48px rgba(0,0,0,0.4)',
        }}
      >
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            gap: 8,
            padding: '8px 12px',
            background: 'var(--bg-secondary, #f5f5f5)',
            color: 'var(--text-muted, rgba(0,0,0,0.6))',
            flex: '0 0 auto',
          }}
        >
          <span style={{ fontSize: 13, fontWeight: 600 }}>{title}</span>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            {toolbar}
            <button
              type="button"
              onClick={onClose}
              title="Close (Esc)"
              aria-label="Close"
              style={{
                display: 'inline-flex',
                alignItems: 'center',
                justifyContent: 'center',
                padding: 4,
                borderRadius: 6,
                cursor: 'pointer',
                background: 'transparent',
                border: 0,
                color: 'inherit',
              }}
            >
              <X size={16} />
            </button>
          </div>
        </div>
        <div style={{ flex: 1, minHeight: 0, overflow: 'hidden', background: '#ffffff' }}>{children}</div>
      </div>
    </div>,
    document.body,
  );
};

export default FullscreenModal;
