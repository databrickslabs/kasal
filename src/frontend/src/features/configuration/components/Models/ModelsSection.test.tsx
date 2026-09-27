import { render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import ModelsSection from '.';

vi.mock('./ModelConfiguration', () => ({
  default: ({ mode }: { mode: string }) => <div>Model list: {mode}</div>,
}));
vi.mock('./DecisionModelConfiguration', () => ({
  default: () => <div>Workspace decision model card</div>,
}));
vi.mock('./DecisionModelSystemSettings', () => ({
  default: () => <div>System decision model card</div>,
}));

describe('ModelsSection', () => {
  it('shows the workspace decision model opt-in in Workspace settings', () => {
    render(<ModelsSection mode="workspace" />);
    expect(screen.getByText('Model list: workspace')).toBeInTheDocument();
    expect(screen.getByText('Workspace decision model card')).toBeInTheDocument();
    expect(screen.queryByText('System decision model card')).not.toBeInTheDocument();
  });

  it('shows the provider endpoint card in System administration', () => {
    render(<ModelsSection mode="system" />);
    expect(screen.getByText('Model list: system')).toBeInTheDocument();
    expect(screen.getByText('System decision model card')).toBeInTheDocument();
    expect(screen.queryByText('Workspace decision model card')).not.toBeInTheDocument();
  });
});
