// Inert stand-in for image-size (see package.json for why). pptxgenjs only
// reaches image-size in its Node build; Kasal runs pptxgenjs in the browser,
// where its `browser` field maps this module to `false`. If a Node code path
// ever does land here, failing loudly beats silently mis-sizing images.
module.exports = function imageSizeStub() {
  throw new Error(
    'image-size is stubbed out in Kasal (browser-only pptxgenjs usage; upstream package has unfixed advisories).',
  );
};
