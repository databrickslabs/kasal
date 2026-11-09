import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { UCMVDriftMonitorConfigSelector, invalidViewLines } from './UCMVDriftMonitorConfigSelector';

vi.mock('../../../../api/databricks/DatabricksService', () => ({
  DatabricksService: { listWarehouses: vi.fn().mockResolvedValue([]) },
}));

describe('invalidViewLines', () => {
  it('accepts three-part names and Catalog Explorer URLs', () => {
    expect(invalidViewLines('a.b.c\nhttps://example.com/explore/data/a/b/c')).toEqual([]);
  });

  it('flags anything else', () => {
    expect(invalidViewLines('a.b.c\njust_view, x.y')).toEqual(['just_view', 'x.y']);
  });
});

describe('UCMVDriftMonitorConfigSelector', () => {
  it('writes the monitored views', () => {
    const onChange = vi.fn();
    render(<UCMVDriftMonitorConfigSelector value={{}} onChange={onChange} />);
    fireEvent.change(screen.getByLabelText(/Deployed metric views/), { target: { value: 'c.s.mv' } });
    expect(onChange).toHaveBeenCalledWith({ ucmv_names: 'c.s.mv' });
  });

  it('shows invalid view names inline', () => {
    render(<UCMVDriftMonitorConfigSelector value={{ ucmv_names: 'bad' }} onChange={vi.fn()} />);
    expect(screen.getByText(/Not a catalog.schema.view name: bad/)).toBeInTheDocument();
  });

  it('renders both Power BI credential sets from the shared fields', () => {
    render(<UCMVDriftMonitorConfigSelector value={{}} onChange={vi.fn()} />);
    expect(screen.getByLabelText(/^Tenant ID/)).toBeInTheDocument();
    expect(screen.getByLabelText(/^Admin Client ID/)).toBeInTheDocument();
  });

  it('defaults the legacy LLM check on and suspected-change patching off', () => {
    const onChange = vi.fn();
    render(<UCMVDriftMonitorConfigSelector value={{}} onChange={onChange} />);
    const [legacy, suspected] = screen.getAllByRole('checkbox');
    expect(legacy).toBeChecked();
    expect(suspected).not.toBeChecked();
    fireEvent.click(suspected);
    expect(onChange).toHaveBeenCalledWith({ apply_suspected_changes: true });
  });
});
