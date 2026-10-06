import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import { UCMVDomainContextHint } from './UCMVDomainContextHint';
import { OPEN_SETTINGS_EVENT, readSettingsIntent } from '../../../configuration/lib/settingsIntent';
import {
  DOMAIN_CONTEXT_EXAMPLE,
  domainContextSkillName,
} from '../../../configuration/components/Skills/skillTemplates';

const list = vi.fn();
const create = vi.fn();
vi.mock('../../../../api/tools/SkillService', () => ({
  SkillService: {
    list: (...a: unknown[]) => list(...a),
    create: (...a: unknown[]) => create(...a),
    update: vi.fn(),
  },
}));
const attachSkillToAgent = vi.fn();
vi.mock('../../../configuration/components/Skills/attachSkill', () => ({
  attachSkillToAgent: (...a: unknown[]) => attachSkillToAgent(...a),
}));
// The learning panel's service is not under test here.
vi.mock('../../../../api/tools/UcmvLearningService', () => ({ UcmvLearningService: { learn: vi.fn() } }));

const skill = (name: string, tagged: boolean, global_enabled = false) => ({
  id: name.length,
  name,
  description: 'd',
  body: 'b',
  metadata: tagged ? { 'kasal-tool-param': 'domain_context' } : {},
  enabled: true,
  global_enabled,
  source: 'authored',
  files: [],
});

// The test environment's Web Storage is unreliable; give the intent a real one.
beforeEach(() => {
  const store = new Map<string, string>();
  vi.stubGlobal('sessionStorage', {
    getItem: (k: string) => store.get(k) ?? null,
    setItem: (k: string, v: string) => void store.set(k, v),
    removeItem: (k: string) => void store.delete(k),
  });
  list.mockReset();
  create.mockReset();
  attachSkillToAgent.mockReset();
});

describe('UCMVDomainContextHint', () => {
  it('shows the domain-context skills the agent will feed to the tool', async () => {
    list.mockResolvedValue([skill('cchbc-context', true), skill('pricing', false), skill('glossary', true, true)]);
    render(<UCMVDomainContextHint agentId="a1" agentSkills={['cchbc-context', 'pricing']} agentName="Generator" onClearInline={() => {}} />);
    expect(await screen.findByText('cchbc-context')).toBeInTheDocument();
    expect(screen.getByText('glossary')).toBeInTheDocument(); // given to every agent
    expect(screen.queryByText('pricing')).not.toBeInTheDocument(); // not tagged
  });

  it('attaches an existing domain-context skill to the agent in one click', async () => {
    list.mockResolvedValue([skill('cchbc-context', true)]);
    attachSkillToAgent.mockResolvedValue({ id: 'a1' });
    render(<UCMVDomainContextHint agentId="a1" agentSkills={[]} agentName="Generator" onClearInline={() => {}} />);
    expect(await screen.findByText(/has no domain-context skill attached/)).toBeInTheDocument();
    fireEvent.click(screen.getByText('cchbc-context'));
    await waitFor(() => expect(attachSkillToAgent).toHaveBeenCalledWith('a1', 'cchbc-context'));
    expect(await screen.findByText('Used for this task:')).toBeInTheDocument();
  });

  it('opens the domain-context form in place: greyed example + YAML learning', async () => {
    list.mockResolvedValue([]);
    render(<UCMVDomainContextHint agentId="a1" agentName="Generator" agentSkills={[]} onClearInline={() => {}} />);
    fireEvent.click(screen.getByText('Create domain-context skill'));
    const field = await screen.findByLabelText('Domain context');
    expect(field).toHaveValue('');
    expect(field).toHaveAttribute('placeholder', DOMAIN_CONTEXT_EXAMPLE);
    expect(screen.getByText('Upload corrected UCMV YAMLs')).toBeInTheDocument();
  });

  it('creates the tagged skill and attaches it to the task agent', async () => {
    list.mockResolvedValue([]);
    create.mockImplementation(async (input: { name: string }) => ({ ...skill(input.name, true), ...input }));
    attachSkillToAgent.mockResolvedValue({ id: 'a1' });
    render(<UCMVDomainContextHint agentId="a1" agentName="Generator" agentSkills={[]} onClearInline={() => {}} />);
    fireEvent.click(screen.getByText('Create domain-context skill'));
    fireEvent.change(await screen.findByLabelText('Model / report name'), { target: { value: 'Total Supply Chain' } });
    fireEvent.change(screen.getByLabelText('Domain context'), { target: { value: '# TSC\nYTD = P001..latest' } });
    fireEvent.click(screen.getByText('Create & attach'));
    await waitFor(() => expect(create).toHaveBeenCalled());
    expect(create.mock.calls[0][0]).toMatchObject({
      name: 'total-supply-chain-domain-context',
      body: '# TSC\nYTD = P001..latest',
      metadata: { 'kasal-tool-param': 'domain_context' },
    });
    await waitFor(() => expect(attachSkillToAgent).toHaveBeenCalledWith('a1', 'total-supply-chain-domain-context'));
  });

  it('moves inline text from an older task into the form and clears it once saved', async () => {
    list.mockResolvedValue([]);
    create.mockImplementation(async (input: { name: string }) => ({ ...skill(input.name, true), ...input }));
    const clear = vi.fn();
    render(<UCMVDomainContextHint inlineValue="# Legacy notes" onClearInline={clear} />);
    fireEvent.click(screen.getByText('Move to a skill'));
    expect(await screen.findByLabelText('Domain context')).toHaveValue('# Legacy notes');
    fireEvent.click(screen.getByText('Create skill'));
    await waitFor(() => expect(clear).toHaveBeenCalled());
  });

  it('links to Settings → Skills', () => {
    list.mockResolvedValue([]);
    const opened = vi.fn();
    window.addEventListener(OPEN_SETTINGS_EVENT, opened);
    render(<UCMVDomainContextHint onClearInline={() => {}} />);
    fireEvent.click(screen.getByText('Manage skills'));
    expect(opened).toHaveBeenCalled();
    expect(readSettingsIntent()).toEqual({ section: 'skills' });
    window.removeEventListener(OPEN_SETTINGS_EVENT, opened);
  });
});

describe('domainContextSkillName', () => {
  it('derives a valid skill name from the model name', () => {
    expect(domainContextSkillName('Total Supply Chain (CCH)')).toBe('total-supply-chain-cch-domain-context');
    expect(domainContextSkillName('')).toBe('ucmv-domain-context');
  });
});
