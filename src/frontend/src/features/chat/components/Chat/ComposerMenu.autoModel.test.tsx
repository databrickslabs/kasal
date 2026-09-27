import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, fireEvent, render, screen } from '@testing-library/react';
import ComposerMenu from './ComposerMenu';
import { currentModelsGroup, useModelsStore } from '../../../../store/models';

const models = [
  { key: 'k1', name: 'Model One' },
  { key: 'k2', name: 'Model Two' },
] as never[];

function open(selectedModel: string, onModelChange = vi.fn()) {
  render(
    <ComposerMenu
      menuPlacement="up"
      menuAnimClass=""
      onPicked={() => undefined}
      models={models}
      selectedModel={selectedModel}
      onModelChange={onModelChange}
      memoryEnabled
      onToggleMemory={() => undefined}
      attachmentCount={0}
      onAttachFiles={() => undefined}
    />,
  );
  fireEvent.click(screen.getByLabelText('Composer settings and tools'));
  return onModelChange;
}

describe('ComposerMenu model selector with Auto', () => {
  // A fresh, already-checked list: the menu must not fetch in these tests.
  beforeEach(() => useModelsStore.setState({
    autoModelAvailable: false, checkedGroup: currentModelsGroup(), checkedAt: Date.now(),
  }));

  it('shows Auto first, checked, when it is available and selected', () => {
    useModelsStore.setState({ autoModelAvailable: true });
    open('auto');
    // The collapsed row names the choice.
    expect(screen.getByRole('button', { name: 'Model' })).toHaveTextContent('Auto');
    fireEvent.click(screen.getByRole('button', { name: 'Model' }));
    const options = screen.getAllByRole('menuitemradio');
    expect(options[0]).toHaveTextContent('Auto');
    expect(options[0]).toHaveAttribute('aria-checked', 'true');
    expect(options[1]).toHaveAttribute('aria-checked', 'false');
  });

  it('hides Auto when the decision model is unavailable', () => {
    open('k1');
    fireEvent.click(screen.getByRole('button', { name: 'Model' }));
    const options = screen.getAllByRole('menuitemradio');
    expect(options).toHaveLength(2);
    expect(options.map((o) => o.textContent)).not.toContain('Auto');
  });

  it('an explicit model pick replaces Auto, and Auto can be picked back', () => {
    useModelsStore.setState({ autoModelAvailable: true });
    const onModelChange = open('auto');
    fireEvent.click(screen.getByRole('button', { name: 'Model' }));
    fireEvent.click(screen.getByRole('menuitemradio', { name: /Model Two/ }));
    expect(onModelChange).toHaveBeenLastCalledWith('k2');
    fireEvent.click(screen.getByRole('button', { name: 'Model' }));
    fireEvent.click(screen.getByRole('menuitemradio', { name: /Auto/ }));
    expect(onModelChange).toHaveBeenLastCalledWith('auto');
  });

  it('shows Auto as soon as the decision model becomes available, without a remount', () => {
    open('k1');
    fireEvent.click(screen.getByRole('button', { name: 'Model' }));
    expect(screen.getAllByRole('menuitemradio')).toHaveLength(2);
    act(() => useModelsStore.setState({ autoModelAvailable: true }));
    expect(screen.getAllByRole('menuitemradio')[0]).toHaveTextContent('Auto');
  });

  it('asks the models store for a (throttled) refresh when the menu opens', () => {
    const ensureFresh = vi.fn(async () => undefined);
    const original = useModelsStore.getState().ensureFresh;
    useModelsStore.setState({ ensureFresh });
    try {
      open('k1');
      expect(ensureFresh).toHaveBeenCalled();
    } finally {
      useModelsStore.setState({ ensureFresh: original });
    }
  });
});
