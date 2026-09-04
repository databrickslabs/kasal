import { describe, expect, it } from 'vitest';
import { LOADING_PIXEL, findAssetIds, substituteAssetRefs } from './assetRefs';

describe('assetRefs', () => {
  const html =
    '<section class="slide"><img src="asset:abc123def" alt="logo">' +
    '<div style="background:url(asset:abc123def)"></div><img src="asset:zz-9999_aa"></section>';

  it('finds each referenced asset once, in order', () => {
    expect(findAssetIds(html)).toEqual(['abc123def', 'zz-9999_aa']);
    expect(findAssetIds('<p>no images</p>')).toEqual([]);
    expect(findAssetIds('asset:short')).toEqual([]); // too short to be an id
  });

  it('substitutes known urls and stands in a loading pixel for the rest', () => {
    const out = substituteAssetRefs(html, { abc123def: 'data:image/png;base64,AAA' });
    expect(out).toContain('src="data:image/png;base64,AAA"');
    expect(out).toContain('url(data:image/png;base64,AAA)');
    expect(out).toContain(`src="${LOADING_PIXEL}"`);
    expect(out).not.toContain('asset:');
    expect(substituteAssetRefs('<p>plain</p>', {})).toBe('<p>plain</p>');
  });
});
