import React from 'react';
import { describe, expect, it } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import BuilderRichText from './BuilderRichText';

describe('builder HTML results', () => {
  it.each([
    '    <!-- Start Screen -->\n    <div class="start-screen"><h2>Ready to Test Your LLM Knowledge?</h2></div>',
    'Here is the quiz:\n```\n<!-- Start Screen -->\n<div class="start-screen"><h2>Ready to Test Your LLM Knowledge?</h2></div>\n```',
  ])('previews comment-led and unlabelled HTML fragments in the sandbox', content => {
    render(<BuilderRichText content={content} />);
    const frame = screen.getByTitle('Rendered diagram');
    expect(frame).toHaveAttribute('srcdoc', expect.stringContaining('start-screen'));
    expect(frame.getAttribute('sandbox')).not.toContain('allow-same-origin');
    expect(screen.queryByText(/<div class=/)).toBeNull();
  });
  it('renders raw HTML in the same sandboxed frame as Chat', () => {
    render(<BuilderRichText content="<!doctype html><html><body><h1>Report</h1></body></html>" />);
    const frame = screen.getByTitle('Rendered diagram');
    expect(frame).toHaveAttribute('sandbox');
    expect(frame.getAttribute('sandbox')).not.toContain('allow-same-origin');
  });
  it('provides slide navigation and export without writing to an unrelated Chat session', () => {
    const html = '```html\n<section class="slide"><h1>One</h1></section><section class="slide"><h1>Two</h1></section>\n```';
    render(<BuilderRichText content={html} />);
    expect(screen.getByText('Slide 1 / 2')).toBeInTheDocument();
    fireEvent.click(screen.getByTitle('Next slide'));
    expect(screen.getByText('Slide 2 / 2')).toBeInTheDocument();
    expect(screen.queryByTitle('Edit deck')).toBeNull();
  });
  it('shows an unfinished HTML block as building only while the response is streaming', () => {
    const view = render(<BuilderRichText content={'```html\n<div>Building'} streaming />);
    expect(screen.getByText('Building diagram…')).toBeInTheDocument();
    view.rerender(<BuilderRichText content={'```html\n<div>Building'} />);
    expect(screen.queryByText('Building diagram…')).toBeNull();
  });
});
