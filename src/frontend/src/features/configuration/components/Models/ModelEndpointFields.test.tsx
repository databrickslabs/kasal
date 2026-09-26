import { describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen, within } from '@testing-library/react';
import ModelEndpointFields from './ModelEndpointFields';
import { endpointError } from './modelEndpoint';

function renderFields(provider: string, params: Record<string, unknown> | null = null) {
  const onChange = vi.fn();
  render(<ModelEndpointFields provider={provider} params={params} onChange={onChange} />);
  return onChange;
}

describe('ModelEndpointFields', () => {
  it.each(['vllm', 'ollama', 'custom'])('asks for the endpoint of a self-hosted %s model', (provider) => {
    const onChange = renderFields(provider);
    const field = screen.getByLabelText('Endpoint URL');
    fireEvent.change(field, { target: { value: 'https://llm.example.com/v1' } });
    expect(onChange).toHaveBeenCalledWith({ api_base: 'https://llm.example.com/v1' });
  });

  it('clearing the endpoint removes it (use the default)', () => {
    const onChange = renderFields('vllm', { api_base: 'https://llm.example.com/v1', tool_choice: 'required' });
    fireEvent.change(screen.getByLabelText('Endpoint URL'), { target: { value: '' } });
    expect(onChange).toHaveBeenCalledWith({ tool_choice: 'required' });
  });

  it('offers an optional override for hosted providers', () => {
    renderFields('anthropic');
    expect(screen.getByLabelText('Endpoint override')).toBeInTheDocument();
  });

  it('shows nothing for Databricks models', () => {
    const { container } = render(
      <ModelEndpointFields provider="databricks" params={null} onChange={vi.fn()} />,
    );
    expect(container).toBeEmptyDOMElement();
  });

  it('flags an invalid URL', () => {
    renderFields('custom', { api_base: 'not a url' });
    expect(screen.getByText(/Enter a full URL/)).toBeInTheDocument();
  });

  it('vLLM tool options map to supports_tools and tool_choice', () => {
    const onChange = renderFields('vllm');
    fireEvent.click(screen.getByRole('checkbox'));
    expect(onChange).toHaveBeenLastCalledWith({ supports_tools: false });

    fireEvent.mouseDown(screen.getByRole('combobox'));
    fireEvent.click(within(screen.getByRole('listbox')).getByText(/required/));
    expect(onChange).toHaveBeenLastCalledWith({ tool_choice: 'required' });
  });
});

describe('endpointError', () => {
  it('accepts empty and http(s) URLs, rejects the rest', () => {
    expect(endpointError('')).toBeNull();
    expect(endpointError('http://localhost:8081/v1')).toBeNull();
    expect(endpointError('https://kat.example.com/v1')).toBeNull();
    expect(endpointError('ftp://x.example.com')).toMatch(/http/);
    expect(endpointError('kat.example.com')).toMatch(/full URL/);
  });
});
