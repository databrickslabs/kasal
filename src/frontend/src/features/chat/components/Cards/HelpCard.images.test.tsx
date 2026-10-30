/**
 * HelpCard with the real markdown renderer (HelpCard.test.tsx stubs it out):
 * remote images wait for a click, same-origin ones and links render as before.
 */
import { render, screen } from '@testing-library/react';
import { describe, it, expect } from 'vitest';
import HelpCard from './HelpCard';

const remote = 'https://images.example.org/c.png?d=1';
const md = `# Help\n\n![chart](${remote}) ![logo](/logo192.png)\n\n[the docs](https://example.com/docs)`;

describe('HelpCard images', () => {
  it('shows a remote image as a placeholder and makes no request until clicked', () => {
    const { container } = render(<HelpCard content={md} />);
    expect(container.querySelector(`img[src="${remote}"]`)).toBeNull();
    expect(screen.getByRole('link', { name: 'Load image' })).toHaveAttribute('href', remote);
  });

  it('renders same-origin images directly and leaves links unchanged', () => {
    const { container } = render(<HelpCard content={md} />);
    expect(container.querySelector('img[src="/logo192.png"]')).not.toBeNull();
    const link = screen.getByRole('link', { name: 'the docs' });
    expect(link).toHaveAttribute('href', 'https://example.com/docs');
    expect(link).not.toHaveAttribute('target');
  });
});
