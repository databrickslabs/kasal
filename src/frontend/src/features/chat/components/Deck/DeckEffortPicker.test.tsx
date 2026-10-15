import type { ComponentProps } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { act, fireEvent, render, screen } from '@testing-library/react';
import DeckEffortPicker from './DeckEffortPicker';
import { currentModelsGroup, resetModelsStoreForTests, useModelsStore } from '../../../../store/models';

vi.mock('../../../../api/config/EnabledModelsService', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../../../../api/config/EnabledModelsService')>()),
  fetchEnabledModelRows: vi.fn(async () => []),
}));

const publish = (...rows: { key: string; allowed_efforts?: string[] }[]) => act(() => useModelsStore.setState({
  models: rows.map((row, id) => ({ id, name: row.key, ...row } as never)),
  loadedForGroup: currentModelsGroup(), checkedGroup: currentModelsGroup(), checkedAt: Date.now(),
}));
const levels = () => screen.getAllByRole('option').map((o) => o.textContent);
const picker = (props: Partial<ComponentProps<typeof DeckEffortPicker>> = {}) => (
  <DeckEffortPicker value={{ tier: 'medium' }} onChange={() => undefined} model="plain" disabled={false} {...props} />
);

describe('DeckEffortPicker (chat) on the shared live list', () => {
  beforeEach(() => resetModelsStoreForTests());

  it('offers the edit model’s native levels, else the runtime levels, with the shared labels', () => {
    publish({ key: 'native', allowed_efforts: ['low', 'medium', 'high', 'xhigh', 'max'] }, { key: 'plain' });
    const { rerender } = render(picker({ model: 'native' }));
    expect(levels()).toEqual(['Low', 'Medium', 'High', 'Extra high', 'Max']);
    rerender(picker({ model: 'plain' }));
    expect(levels()).toEqual(['Low', 'Medium', 'High']);
    // The workspace default: its scale is not known here, as in the composer.
    rerender(picker({ model: '' }));
    expect(levels()).toEqual(['Low', 'Medium', 'High']);
  });

  it('keeps a level the model lacks selectable instead of silently changing it', () => {
    publish({ key: 'plain' });
    render(picker({ value: { tier: 'max' } }));
    expect(screen.getByLabelText('Slide edit effort')).toHaveValue('max');
    expect(levels()).toEqual(['Low', 'Medium', 'High', 'Max']);
  });

  it('reports the picked level and keeps its keys from reaching the studio', () => {
    publish({ key: 'plain' });
    const change = vi.fn();
    const studioKeys = vi.fn();
    render(<div onKeyDown={studioKeys}>{picker({ onChange: change })}</div>);
    const select = screen.getByRole('combobox', { name: 'Slide edit effort' });
    fireEvent.keyDown(select, { key: 'Escape' });
    fireEvent.keyDown(select, { key: 'ArrowDown' });
    expect(studioKeys).not.toHaveBeenCalled();
    fireEvent.change(select, { target: { value: 'high' } });
    expect(change).toHaveBeenCalledWith({ tier: 'high' });
  });
});
