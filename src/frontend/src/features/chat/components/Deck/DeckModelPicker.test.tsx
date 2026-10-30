import { beforeEach, describe, expect, it, vi } from 'vitest';
import { act, render, screen } from '@testing-library/react';
import DeckModelPicker from './DeckModelPicker';
import { currentModelsGroup, resetModelsStoreForTests, useModelsStore } from '../../../../store/models';

vi.mock('../../../../api/config/EnabledModelsService', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../../../../api/config/EnabledModelsService')>()),
  fetchEnabledModelRows: vi.fn(async () => []),
}));

const publish = (...keys: string[]) => act(() => useModelsStore.setState({
  models: keys.map((key, id) => ({ id, key, name: key } as never)),
  loadedForGroup: currentModelsGroup(), checkedGroup: currentModelsGroup(), checkedAt: Date.now(),
}));

describe('DeckModelPicker (chat) on the shared live list', () => {
  beforeEach(() => resetModelsStoreForTests());

  it('re-renders with the new model list without a remount', () => {
    publish('one', 'two');
    render(<DeckModelPicker value="" onChange={() => undefined} disabled={false} />);
    const options = () => screen.getAllByRole('option').map((o) => o.getAttribute('value'));
    expect(options()).toEqual(['', 'one', 'two']);
    publish('one', 'three');
    expect(options()).toEqual(['', 'one', 'three']);
  });

  it('says so when the last refresh failed, keeping the list', () => {
    publish('one');
    render(<DeckModelPicker value="" onChange={() => undefined} disabled={false} />);
    act(() => useModelsStore.setState({ error: 'offline' }));
    expect(screen.getByRole('status')).toHaveTextContent('Could not refresh models.');
    expect(screen.getAllByRole('option')).toHaveLength(2);
  });
});
