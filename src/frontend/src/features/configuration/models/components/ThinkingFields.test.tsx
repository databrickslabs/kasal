/**
 * Only render a control the model actually accepts.
 *
 * This is not cosmetic gating. Offering a parameter the endpoint refuses does not
 * degrade — the run fails with a 400 — and the shapes are mutually exclusive:
 * Claude 4.1–4.6 take a token budget, Claude 4.7+/5/Fable reject that and take an
 * effort level, and the effort SCALES differ per model (five distinct ones across
 * the catalogue). So every decision here comes from server-derived capability, and
 * these tests exist to keep it that way.
 */
import React from 'react';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';

import ThinkingFields from './ThinkingFields';

const noop = () => undefined;

describe('ThinkingFields', () => {
  describe('when the model has no thinking surface', () => {
    it('renders nothing at all by default', () => {
      const { container } = render(
        <ThinkingFields
          thinkingMode={null}
          onBudgetTokensChange={noop}
          onEffortChange={noop}
        />,
      );
      expect(container).toBeEmptyDOMElement();
    });

    it('explains itself when given a hint', () => {
      // Silence reads as "the feature is missing"; the truth is "this model has
      // no knob to turn".
      render(
        <ThinkingFields
          thinkingMode={null}
          unsupportedHint="This model exposes no thinking controls."
          onBudgetTokensChange={noop}
          onEffortChange={noop}
        />,
      );
      expect(screen.getByText(/no thinking controls/i)).toBeInTheDocument();
    });
  });

  describe('manual models (a token budget)', () => {
    it('shows the budget field and no effort selector', () => {
      render(
        <ThinkingFields
          thinkingMode="manual"
          enabled
          onEnabledChange={noop}
          budgetTokens={10240}
          onBudgetTokensChange={noop}
          onEffortChange={noop}
        />,
      );
      expect(screen.getByLabelText(/Thinking Budget \(tokens\)/i)).toBeInTheDocument();
      expect(screen.queryByRole('combobox')).not.toBeInTheDocument();
    });

    it('enforces the endpoint minimum of 1024', () => {
      render(
        <ThinkingFields
          thinkingMode="manual"
          enabled
          onEnabledChange={noop}
          onBudgetTokensChange={noop}
          onEffortChange={noop}
        />,
      );
      expect(screen.getByLabelText(/Thinking Budget/i)).toHaveAttribute('min', '1024');
    });

    it('disables the budget while thinking is off', () => {
      render(
        <ThinkingFields
          thinkingMode="manual"
          enabled={false}
          onEnabledChange={noop}
          onBudgetTokensChange={noop}
          onEffortChange={noop}
        />,
      );
      expect(screen.getByLabelText(/Thinking Budget/i)).toBeDisabled();
    });

    it('reports a cleared field as null, not 0', () => {
      // 0 would be a real budget the endpoint rejects; null means "inherit".
      const onBudgetTokensChange = vi.fn();
      render(
        <ThinkingFields
          thinkingMode="manual"
          enabled
          onEnabledChange={noop}
          budgetTokens={2048}
          onBudgetTokensChange={onBudgetTokensChange}
          onEffortChange={noop}
        />,
      );
      // Clearing must report null ("inherit"), never 0 — which would be a real
      // budget below the endpoint's 1024 minimum.
      return userEvent.clear(screen.getByLabelText(/Thinking Budget/i)).then(() => {
        expect(onBudgetTokensChange).toHaveBeenCalledWith(null);
      });
    });
  });

  describe('adaptive models (an effort level)', () => {
    const ANTHROPIC_SCALE = ['low', 'medium', 'high', 'xhigh', 'max'];

    it('shows the effort selector and no budget field', () => {
      render(
        <ThinkingFields
          thinkingMode="adaptive"
          allowedEfforts={ANTHROPIC_SCALE}
          enabled
          onEnabledChange={noop}
          onBudgetTokensChange={noop}
          onEffortChange={noop}
        />,
      );
      expect(screen.getByRole('combobox')).toBeInTheDocument();
      expect(screen.queryByLabelText(/Thinking Budget/i)).not.toBeInTheDocument();
    });

    it('offers exactly the options the SERVER supplied', async () => {
      // The whole point: hardcoding a list would offer values this model
      // rejects. Anthropic adaptive has five levels, not the three that were
      // assumed before it was measured.
      const user = userEvent.setup();
      render(
        <ThinkingFields
          thinkingMode="adaptive"
          allowedEfforts={ANTHROPIC_SCALE}
          enabled
          onEnabledChange={noop}
          onBudgetTokensChange={noop}
          onEffortChange={noop}
        />,
      );
      await user.click(screen.getByRole('combobox'));
      for (const level of ANTHROPIC_SCALE) {
        expect(screen.getByRole('option', { name: level })).toBeInTheDocument();
      }
      // Not offered by this model, and a 400 if sent.
      expect(screen.queryByRole('option', { name: 'minimal' })).not.toBeInTheDocument();
    });

    it('offers a DIFFERENT set for a model with a different scale', async () => {
      // gpt-5 accepts "minimal" and rejects "none"; gpt-5-1 is the reverse. Same
      // component, different options, because the server said so.
      const user = userEvent.setup();
      render(
        <ThinkingFields
          thinkingMode={null}
          allowedEfforts={['minimal', 'low', 'medium', 'high']}
          onBudgetTokensChange={noop}
          onEffortChange={noop}
        />,
      );
      await user.click(screen.getByRole('combobox'));
      expect(screen.getByRole('option', { name: 'minimal' })).toBeInTheDocument();
      expect(screen.queryByRole('option', { name: 'xhigh' })).not.toBeInTheDocument();
    });
  });

  describe('models with only a reasoning_effort scale', () => {
    it('shows the effort control with no Extended Thinking toggle', () => {
      // GPT-5 and Gemini are not Anthropic: no `thinking` block to switch on, but
      // they do take a depth. A toggle here would imply a setting that does not
      // exist.
      render(
        <ThinkingFields
          thinkingMode={null}
          allowedEfforts={['low', 'medium', 'high']}
          onBudgetTokensChange={noop}
          onEffortChange={noop}
        />,
      );
      expect(screen.getByRole('combobox')).toBeInTheDocument();
      expect(screen.queryByRole('checkbox')).not.toBeInTheDocument();
    });

    it('says so when the model reasons but returns no text', () => {
      // gpt-5* bills reasoning_tokens and never returns the trace. Effort still
      // changes cost and depth, so the control is real — the helper text stops it
      // reading as broken.
      render(
        <ThinkingFields
          thinkingMode={null}
          allowedEfforts={['minimal', 'low', 'medium', 'high']}
          returnsThinkingText={false}
          onBudgetTokensChange={noop}
          onEffortChange={noop}
        />,
      );
      expect(screen.getByText(/never returns the text/i)).toBeInTheDocument();
    });
  });

  describe('agent-override mode', () => {
    it('drops the toggle and offers an inherit option', () => {
      render(
        <ThinkingFields
          overrideMode
          thinkingMode="adaptive"
          allowedEfforts={['low', 'high']}
          onBudgetTokensChange={noop}
          onEffortChange={noop}
        />,
      );
      expect(screen.queryByRole('checkbox')).not.toBeInTheDocument();
      expect(screen.getByRole('combobox')).toBeInTheDocument();
      // MUI renders the label twice (the <label> and the fieldset legend).
      expect(screen.getAllByText(/Reasoning Effort Override/i).length).toBeGreaterThan(0);
    });

    it('labels a blank budget as inheriting the model', () => {
      render(
        <ThinkingFields
          overrideMode
          thinkingMode="manual"
          onBudgetTokensChange={noop}
          onEffortChange={noop}
        />,
      );
      expect(screen.getByText(/inherit the model default/i)).toBeInTheDocument();
    });

    it('leaves the field editable with no toggle to gate it', () => {
      render(
        <ThinkingFields
          overrideMode
          thinkingMode="manual"
          onBudgetTokensChange={noop}
          onEffortChange={noop}
        />,
      );
      expect(screen.getByLabelText(/Thinking Budget Override/i)).not.toBeDisabled();
    });
  });
});
