import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import LocalMlflowServerField from './LocalMlflowServerField';

describe('LocalMlflowServerField', () => {
  it('saves a trimmed http(s) URL and refuses anything else', () => {
    const onSave = vi.fn();
    render(<LocalMlflowServerField value={null} saving={false} onSave={onSave} />);
    const field = screen.getByLabelText('Local MLflow server');

    fireEvent.change(field, { target: { value: 'localhost:5555' } });
    expect(screen.getByText('Use an http:// or https:// URL.')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Save' })).toBeDisabled();

    fireEvent.change(field, { target: { value: ' http://127.0.0.1:5555/ ' } });
    fireEvent.click(screen.getByRole('button', { name: 'Save' }));
    expect(onSave).toHaveBeenCalledWith('http://127.0.0.1:5555');
  });

  it('clears a saved server', () => {
    const onSave = vi.fn();
    render(<LocalMlflowServerField value="http://127.0.0.1:5555" saving={false} onSave={onSave} />);
    fireEvent.click(screen.getByRole('button', { name: 'Clear' }));
    expect(onSave).toHaveBeenCalledWith('');
  });
});
