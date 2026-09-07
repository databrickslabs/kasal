import React, { useRef } from 'react';
import { Box } from '@mui/material';

interface Props {
  side: 'left' | 'right';
  ratio: number;
  leftInset: number;
  rightInset: number;
  onChange: (ratio: number) => void;
}

/** Pointer and keyboard resizing share the same bounds; both panes remain usable. */
export default function WorkspaceSplitDivider({ side, ratio, leftInset, rightInset, onChange }: Props) {
  const dragging = useRef(false);
  const updateFromPointer = (event: React.PointerEvent<HTMLElement>) => {
    const available = window.innerWidth - leftInset - rightInset;
    const fraction = (event.clientX - leftInset) / available;
    onChange(side === 'left' ? fraction : 1 - fraction);
  };
  return <Box role="separator" aria-label="Resize conversation and canvas" aria-orientation="vertical"
    aria-valuemin={30} aria-valuemax={75} aria-valuenow={Math.round(ratio * 100)}
    aria-valuetext={`Conversation ${Math.round(ratio * 100)}%, canvas ${Math.round((1 - ratio) * 100)}%`}
    tabIndex={0}
    onPointerDown={event => { event.preventDefault(); dragging.current = true; event.currentTarget.setPointerCapture(event.pointerId); }}
    onPointerMove={event => { if (dragging.current) updateFromPointer(event); }}
    onPointerUp={event => { if (dragging.current) updateFromPointer(event); dragging.current = false; event.currentTarget.releasePointerCapture(event.pointerId); }}
    onPointerCancel={() => { dragging.current = false; }}
    onLostPointerCapture={() => { dragging.current = false; }}
    onDoubleClick={() => onChange(0.6)}
    onKeyDown={event => {
      if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return;
      event.preventDefault();
      if (event.key === 'Home') onChange(0.3);
      else if (event.key === 'End') onChange(0.75);
      else onChange(ratio + (event.key === 'ArrowRight' ? 0.05 : -0.05) * (side === 'left' ? 1 : -1));
    }}
    sx={{ position: 'fixed', zIndex: 1201, top: 8, bottom: 24, width: 12,
      left: `calc(${leftInset}px + (100vw - ${leftInset + rightInset}px) * ${side === 'left' ? ratio : 1 - ratio} - 6px)`,
      cursor: 'col-resize', touchAction: 'none', display: 'flex', alignItems: 'center', justifyContent: 'center', borderRadius: 2,
      '&::after': { content: '""', width: 3, height: 36, borderRadius: 2, bgcolor: 'text.disabled', opacity: 0.3, transition: 'opacity 150ms' },
      '&:hover::after, &:focus-visible::after': { opacity: 0.8 },
      '&:focus-visible': { outline: '2px solid', outlineColor: 'text.secondary', outlineOffset: 1 },
    }} />;
}
