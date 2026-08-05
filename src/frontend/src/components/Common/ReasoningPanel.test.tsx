/**
 * The reasoning panel: collapsed by default, uncapped when open.
 *
 * Both properties are requirements, not styling preferences. Collapsed because
 * reasoning is supporting detail, not the answer. Uncapped because expanding it
 * is an explicit request to READ it — an inner max-height turns a 2,000-character
 * train of thought into a nested scrollbar fighting the pane behind it.
 */
import React from 'react';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it } from 'vitest';

import ReasoningPanel, {
  REASONING_VISIBLE_MODELS,
  REDACTED_REASONING,
  reasoningText,
} from './ReasoningPanel';

const THINKING = '**My Thought Process for Calculating 17 x 23**\nAlright, so I need to…';

describe('reasoningText', () => {
  it('returns a plain string as-is', () => {
    expect(reasoningText('thinking')).toBe('thinking');
  });

  it('is empty for absent / non-text values', () => {
    expect(reasoningText(undefined)).toBe('');
    expect(reasoningText(null)).toBe('');
    expect(reasoningText('   ')).toBe('');
    expect(reasoningText(42)).toBe('');
  });

  it('joins a list of summary parts', () => {
    // Defensive: a provider could hand back the summary array un-flattened.
    expect(reasoningText([{ text: 'step1 ' }, { text: 'step2' }])).toBe('step1 step2');
    expect(reasoningText(['a', 'b'])).toBe('ab');
  });
});

describe('ReasoningPanel', () => {
  it('renders nothing when there is no reasoning', () => {
    // The common case by far: most models never expose thinking, and Claude on
    // Databricks sends a reasoning block whose text is redacted. Neither should
    // leave an empty affordance behind.
    const { container } = render(<ReasoningPanel reasoning={undefined} />);
    expect(container).toBeEmptyDOMElement();
  });

  it('renders nothing for whitespace-only reasoning', () => {
    const { container } = render(<ReasoningPanel reasoning={'  \n '} />);
    expect(container).toBeEmptyDOMElement();
  });

  it('starts collapsed and reports the size', () => {
    render(<ReasoningPanel reasoning={THINKING} />);

    const toggle = screen.getByRole('button', { name: /show model reasoning/i });
    expect(toggle).toHaveAttribute('aria-expanded', 'false');
    expect(screen.getByText(`Reasoning (${THINKING.length} chars)`)).toBeInTheDocument();
    expect(screen.queryByText(THINKING)).not.toBeInTheDocument();
  });

  it('reveals the FULL text on click, not a truncated preview', async () => {
    const user = userEvent.setup();
    const long = 'x'.repeat(5000);
    render(<ReasoningPanel reasoning={long} />);

    await user.click(screen.getByRole('button', { name: /show model reasoning/i }));

    const body = screen.getByText(long);
    expect(body).toBeInTheDocument();
    expect(body.textContent).toHaveLength(5000);
  });

  // MUI compiles `sx` to a CSS class rather than inline styles, so these read
  // the computed value instead of element.style.
  const bodyOf = (text: string): HTMLElement =>
    screen.getByText(text, { collapseWhitespace: false }).parentElement as HTMLElement;

  it('does not cap its height by default', async () => {
    // The bug this guards: a fixed maxHeight scrolls internally, so "expand"
    // showed a 288px window onto the reasoning instead of the reasoning.
    const user = userEvent.setup();
    render(<ReasoningPanel reasoning={THINKING} />);

    await user.click(screen.getByRole('button', { name: /show model reasoning/i }));

    const computed = getComputedStyle(bodyOf(THINKING));
    expect(computed.maxHeight).toBe('');
    expect(computed.overflow).toBe('');
  });

  it('honours an explicit cap when a surface asks for one', async () => {
    const user = userEvent.setup();
    render(<ReasoningPanel reasoning={THINKING} maxHeight={200} />);

    await user.click(screen.getByRole('button', { name: /show model reasoning/i }));

    const computed = getComputedStyle(bodyOf(THINKING));
    expect(computed.maxHeight).toBe('200px');
    expect(computed.overflow).toBe('auto');
  });

  it('can start expanded', () => {
    render(<ReasoningPanel reasoning={THINKING} defaultExpanded />);

    expect(screen.getByRole('button', { name: /hide model reasoning/i })).toHaveAttribute(
      'aria-expanded',
      'true',
    );
    expect(screen.getByText(THINKING, { collapseWhitespace: false })).toBeInTheDocument();
  });

  describe('when the provider redacted the reasoning', () => {
    // Anthropic Claude on Databricks. Showing an empty panel implied these
    // models don't think; showing the sentinel would leak an internal token.
    it('says so in the header instead of counting characters', () => {
      render(<ReasoningPanel reasoning={REDACTED_REASONING} />);

      expect(screen.getByText('Reasoning (hidden by provider)')).toBeInTheDocument();
      expect(screen.queryByText(/chars/)).not.toBeInTheDocument();
    });

    it('never renders the raw sentinel', async () => {
      const user = userEvent.setup();
      render(<ReasoningPanel reasoning={REDACTED_REASONING} />);

      await user.click(screen.getByRole('button', { name: /show model reasoning/i }));

      expect(screen.queryByText(REDACTED_REASONING)).not.toBeInTheDocument();
    });

    it('handles the sentinel REPEATED, which is what shipped', () => {
      // The sentinel is a per-call flag, but the streaming path reported it once
      // per delta and the backend appended each one, so the stored reasoning read
      // `__kasal_reasoning_redacted__` six times over. Every consumer tested it
      // for EQUALITY, so a repeat matched nothing and rendered to the user as
      // literal text — exactly how this was found. The backend no longer
      // accumulates it; traces already written still hold the repeat.
      const repeated = REDACTED_REASONING.repeat(6);
      render(<ReasoningPanel reasoning={repeated} />);

      expect(screen.getByText('Reasoning (hidden by provider)')).toBeInTheDocument();
      expect(screen.queryByText(/chars/)).not.toBeInTheDocument();
      expect(screen.queryByText(repeated)).not.toBeInTheDocument();
    });

    it('tells the user the actionable fix, not just that it is hidden', async () => {
      // This message has been wrong twice. First it claimed the text could not be
      // had from Kasal at all — false: `display` defaults to "omitted" on Claude
      // 5/Fable/4.7/4.8 and the transport can opt in. Then it said to enable
      // Extended Thinking in Settings — also stale, because the transport now
      // asks for the summary on every adaptive Claude automatically, so the only
      // way to still see this is an old run (re-run it) or a manual-mode model
      // that needs a budget. Saying "flip a toggle" when there is no toggle left
      // sends the reader somewhere that cannot help.
      const user = userEvent.setup();
      render(<ReasoningPanel reasoning={REDACTED_REASONING} />);

      await user.click(screen.getByRole('button', { name: /show model reasoning/i }));

      expect(screen.getByText(/re-run it/)).toBeInTheDocument();
      expect(screen.getByText(/thinking budget/)).toBeInTheDocument();
      expect(screen.getByText(/GPT-5 family/)).toBeInTheDocument();
      for (const model of REASONING_VISIBLE_MODELS) {
        expect(screen.getByText(model)).toBeInTheDocument();
      }
    });

    it('cites the authoritative source for each provider claim', async () => {
      // These are factual claims about someone else's API, and both were got
      // wrong here first. A reader must be able to check them.
      const user = userEvent.setup();
      render(<ReasoningPanel reasoning={REDACTED_REASONING} />);

      await user.click(screen.getByRole('button', { name: /show model reasoning/i }));

      const anthropic = screen.getByRole('link', { name: /Anthropic/i });
      expect(anthropic).toHaveAttribute(
        'href',
        expect.stringContaining('platform.claude.com'),
      );
      const openai = screen.getByRole('link', { name: /OpenAI/i });
      expect(openai).toHaveAttribute(
        'href',
        expect.stringContaining('developers.openai.com'),
      );
      // Opened safely — these leave the app.
      for (const link of [anthropic, openai]) {
        expect(link).toHaveAttribute('target', '_blank');
        expect(link).toHaveAttribute('rel', expect.stringContaining('noopener'));
      }
    });
  });

  it('collapses again on a second click', async () => {
    const user = userEvent.setup();
    render(<ReasoningPanel reasoning={THINKING} defaultExpanded />);

    await user.click(screen.getByRole('button', { name: /hide model reasoning/i }));

    expect(screen.getByRole('button', { name: /show model reasoning/i })).toHaveAttribute(
      'aria-expanded',
      'false',
    );
  });
});
