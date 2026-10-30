import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import A2UIRuntimeOverrides from './A2UIRuntimeOverrides';
import { parseOverrides } from './a2uiOverrides';

const defaults = { a2ui_streaming: true, a2ui_compose_retries: 2 };

function open() {
  fireEvent.click(screen.getByText('Advanced: rich answer behaviour'));
}

describe('A2UIRuntimeOverrides', () => {
  it('follows the system default until the workspace overrides it', () => {
    const onChange = vi.fn();
    render(<A2UIRuntimeOverrides overrides={{}} defaults={defaults} onChange={onChange} />);
    open();
    expect(screen.getByLabelText('Compose attempts')).toBeDisabled();

    fireEvent.click(screen.getByRole('checkbox', { name: 'Use system default (2)' }));
    expect(onChange).toHaveBeenLastCalledWith({ a2ui_compose_retries: 2 });
  });

  it('edits an override and can go back to the default', () => {
    const onChange = vi.fn();
    render(
      <A2UIRuntimeOverrides overrides={{ a2ui_compose_retries: 4 }} defaults={defaults} onChange={onChange} />,
    );
    open();
    fireEvent.change(screen.getByLabelText('Compose attempts'), { target: { value: '6' } });
    expect(onChange).toHaveBeenLastCalledWith({ a2ui_compose_retries: 6 });

    fireEvent.click(screen.getByRole('checkbox', { name: 'Use system default (2)' }));
    expect(onChange).toHaveBeenLastCalledWith({});
  });

  it('shows nothing when the server sends no defaults', () => {
    const { container } = render(<A2UIRuntimeOverrides overrides={{}} defaults={{}} onChange={vi.fn()} />);
    expect(container).toBeEmptyDOMElement();
  });
});

describe('parseOverrides', () => {
  it('keeps booleans and numbers, drops the rest', () => {
    expect(parseOverrides('{"a2ui_streaming": false, "x": "y"}')).toEqual({ a2ui_streaming: false });
    expect(parseOverrides('nope')).toEqual({});
    expect(parseOverrides(null)).toEqual({});
  });
});
