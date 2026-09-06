import { describe, expect, it } from 'vitest';
import { cssColorToPptx, gradientFirstColor, isTextBlock, pxToIn, pxToPt } from './deckExport';

describe('editable PPTX unit conversions', () => {
  it('maps the 1280px stage onto the 13.333in wide layout exactly', () => {
    expect(pxToIn(1280)).toBeCloseTo(13.333, 2);
    expect(pxToIn(96)).toBe(1);
    expect(pxToIn(720)).toBe(7.5);
  });

  it('converts CSS px font sizes to PowerPoint points', () => {
    expect(pxToPt(16)).toBe(12);
    expect(pxToPt(56)).toBe(42);
  });
});

describe('cssColorToPptx', () => {
  it('handles hex, short hex, rgb and rgba', () => {
    expect(cssColorToPptx('#FF5F46')).toBe('FF5F46');
    expect(cssColorToPptx('#0bc')).toBe('00BBCC');
    expect(cssColorToPptx('rgb(11, 32, 38)')).toBe('0B2026');
    expect(cssColorToPptx('rgba(255, 255, 255, 0.9)')).toBe('FFFFFF');
  });

  it('treats transparent as no color', () => {
    expect(cssColorToPptx('transparent')).toBeNull();
    expect(cssColorToPptx('rgba(0, 0, 0, 0)')).toBeNull();
    expect(cssColorToPptx('')).toBeNull();
  });
});

describe('gradientFirstColor', () => {
  it('flattens a gradient to its first stop', () => {
    expect(gradientFirstColor('linear-gradient(135deg, #0b2026, #1B5162)')).toBe('0B2026');
    expect(gradientFirstColor('linear-gradient(rgb(27, 81, 98), #fff)')).toBe('1B5162');
    expect(gradientFirstColor('none')).toBeNull();
  });
});

describe('isTextBlock', () => {
  it('accepts a heading with inline children and rejects block containers', () => {
    const h = document.createElement('h1');
    h.innerHTML = 'Deck <b>Title</b>';
    Object.defineProperty(h, 'innerText', { value: 'Deck Title' });
    document.body.appendChild(h);
    expect(isTextBlock(h)).toBe(true);

    const card = document.createElement('div');
    card.innerHTML = '<h3>Card</h3><p>Body</p>'; // block children → not one text run
    Object.defineProperty(card, 'innerText', { value: 'Card\nBody' });
    document.body.appendChild(card);
    expect(isTextBlock(card)).toBe(false);
    h.remove();
    card.remove();
  });
});
