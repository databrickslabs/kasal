import { vi, describe, it, expect, beforeEach } from 'vitest';
import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import SlashCommandMenu from './SlashCommandMenu';
import { SlashCommand } from '../utils/chatHelpers';

// scrollIntoView is not implemented in jsdom
Element.prototype.scrollIntoView = vi.fn();

const mockCommands: SlashCommand[] = [
  { command: '/list crews', description: 'List all saved crews', category: 'crew' },
  { command: '/list flows', description: 'List all saved flows', category: 'flow' },
  { command: '/load crew', description: 'Load a saved crew onto the canvas', category: 'crew' },
];

describe('SlashCommandMenu', () => {
  const onSelect = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('renders nothing when commands array is empty', () => {
    const { container } = render(
      <SlashCommandMenu commands={[]} selectedIndex={0} onSelect={onSelect} />
    );
    expect(container.firstChild).toBeNull();
  });

  it('renders all commands', () => {
    render(
      <SlashCommandMenu commands={mockCommands} selectedIndex={0} onSelect={onSelect} />
    );

    expect(screen.getByText('/list crews')).toBeInTheDocument();
    expect(screen.getByText('/list flows')).toBeInTheDocument();
    expect(screen.getByText('/load crew')).toBeInTheDocument();
  });

  it('renders command descriptions', () => {
    render(
      <SlashCommandMenu commands={mockCommands} selectedIndex={0} onSelect={onSelect} />
    );

    expect(screen.getByText('List all saved crews')).toBeInTheDocument();
    expect(screen.getByText('List all saved flows')).toBeInTheDocument();
    expect(screen.getByText('Load a saved crew onto the canvas')).toBeInTheDocument();
  });

  it('highlights the selected item', () => {
    render(
      <SlashCommandMenu commands={mockCommands} selectedIndex={1} onSelect={onSelect} />
    );

    const items = screen.getAllByRole('button');
    expect(items[1]).toHaveClass('Mui-selected');
    expect(items[0]).not.toHaveClass('Mui-selected');
  });

  it('calls onSelect with the clicked command on mouseDown', () => {
    render(
      <SlashCommandMenu commands={mockCommands} selectedIndex={0} onSelect={onSelect} />
    );

    const secondItem = screen.getByText('/list flows').closest('[role="button"]')!;
    fireEvent.mouseDown(secondItem);

    expect(onSelect).toHaveBeenCalledTimes(1);
    expect(onSelect).toHaveBeenCalledWith(mockCommands[1]);
  });

  it('prevents default on mouseDown to avoid input blur', () => {
    render(
      <SlashCommandMenu commands={mockCommands} selectedIndex={0} onSelect={onSelect} />
    );

    const item = screen.getByText('/list crews').closest('[role="button"]')!;
    const event = new MouseEvent('mousedown', { bubbles: true, cancelable: true });
    const preventDefaultSpy = vi.spyOn(event, 'preventDefault');

    item.dispatchEvent(event);

    expect(preventDefaultSpy).toHaveBeenCalled();
  });

  it('scrolls selected item into view when selectedIndex changes', () => {
    const { rerender } = render(
      <SlashCommandMenu commands={mockCommands} selectedIndex={0} onSelect={onSelect} />
    );

    rerender(
      <SlashCommandMenu commands={mockCommands} selectedIndex={2} onSelect={onSelect} />
    );

    expect(Element.prototype.scrollIntoView).toHaveBeenCalled();
  });

  it('renders with monospace font for command text', () => {
    render(
      <SlashCommandMenu commands={mockCommands} selectedIndex={0} onSelect={onSelect} />
    );

    const commandText = screen.getByText('/list crews');
    expect(commandText).toHaveStyle({ fontFamily: 'monospace' });
  });
});
