import { useContext } from 'react';
import RunProgress from '../../../chat/components/Chat/RunProgress';
import { useThemeStore } from '../../../../store/theme';
import { BuilderPreviewContext } from './BuilderPreviewContext';

/** Chat's activity and trace reader, pinned to the transcript's execution. */
export default function BuilderRunActivity({ jobId, running, onOpenLogs }: { jobId: string; running: boolean; onOpenLogs?: (jobId: string) => void }) {
  const preview = useContext(BuilderPreviewContext);
  const dark = useThemeStore(state => state.isDarkMode);
  return <li className="kasal-chat-root" data-theme={dark ? 'dark' : 'light'} style={{ listStyle: 'none' }}>
    <RunProgress inline jobId={jobId} running={running} generating={false}
      onSelectStep={step => preview ? preview.openStep(jobId, step) : onOpenLogs?.(jobId)} />
  </li>;
}
