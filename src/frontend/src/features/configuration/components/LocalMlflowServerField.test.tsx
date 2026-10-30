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

  it('allows plain http only for this machine, and no credentials', () => {
    const onSave = vi.fn();
    render(<LocalMlflowServerField value={null} saving={false} onSave={onSave} />);
    const field = screen.getByLabelText('Local MLflow server');
    const save = () => screen.getByRole('button', { name: 'Save' });

    fireEvent.change(field, { target: { value: 'http://mlflow.example.com' } });
    expect(
      screen.getByText('Plain http is only allowed for localhost; use https for other hosts.'),
    ).toBeInTheDocument();
    expect(save()).toBeDisabled();

    fireEvent.change(field, { target: { value: 'https://user:pw@mlflow.example.com' } });
    expect(screen.getByText('Remove the credentials from the URL.')).toBeInTheDocument();
    expect(save()).toBeDisabled();

    for (const ok of ['https://mlflow.example.com', 'http://localhost:5000', 'http://[::1]:5555']) {
      fireEvent.change(field, { target: { value: ok } });
      expect(save()).not.toBeDisabled();
    }
  });

  it('clears a saved server', () => {
    const onSave = vi.fn();
    render(<LocalMlflowServerField value="http://127.0.0.1:5555" saving={false} onSave={onSave} />);
    fireEvent.click(screen.getByRole('button', { name: 'Clear' }));
    expect(onSave).toHaveBeenCalledWith('');
  });
});
