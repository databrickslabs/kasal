import { render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import EnginesConfiguration from './EnginesConfiguration';
import { DecisionConfigService } from '../../../../api/config/DecisionConfigService';

vi.mock('../../../../api/config/EngineConfigService', () => ({
  EngineConfigService: {
    getOtelAppTelemetryConfig: vi.fn().mockResolvedValue({
      otel_app_telemetry_enabled: false, otel_app_telemetry_log_level: 'INFO',
    }),
    setOtelAppTelemetryConfig: vi.fn(),
  },
}));
vi.mock('../../../../api/config/DecisionConfigService', () => ({
  DecisionConfigService: { getConfig: vi.fn(), saveConfig: vi.fn(), recommend: vi.fn() },
}));
vi.mock('../../../../store/crewExecution', () => ({
  useCrewExecutionStore: (selector: (state: object) => unknown) =>
    selector({ inputMode: 'dialog', setInputMode: vi.fn() }),
}));
vi.mock('../../../../store/eventTriggers', () => {
  const state = { enabled: false, setEnabled: vi.fn(), load: vi.fn().mockResolvedValue(undefined) };
  return { useEventTriggersStore: (selector: (s: typeof state) => unknown) => selector(state) };
});
vi.mock('./HarnessSelector', () => ({ default: () => <div>Harness selector</div> }));
vi.mock('./EngineSystemSettings', () => ({ default: () => <div>Engine system settings</div> }));

describe('EnginesConfiguration', () => {
  it('no longer hosts the decision model setting (it lives under Models)', async () => {
    render(<EnginesConfiguration />);
    expect(await screen.findByText('Harness selector')).toBeInTheDocument();
    expect(screen.getByText('Engine system settings')).toBeInTheDocument();
    expect(screen.queryByText(/Decision model|Jev decisions/)).not.toBeInTheDocument();
    expect(screen.queryByRole('checkbox', { name: /decision model|Use Jev/i })).not.toBeInTheDocument();
    expect(DecisionConfigService.getConfig).not.toHaveBeenCalled();
  });
});
