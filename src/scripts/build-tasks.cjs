#!/usr/bin/env node
// Cross-platform replacement for the mkdir -p / cp / rm -rf shell chains that
// broke `npm run build` under Windows cmd.exe (see fix(deploy): use npm.cmd
// on Windows — same class of bug, one layer up in the npm lifecycle).
const fs = require('fs');
const path = require('path');

const root = path.join(__dirname, '..');
const docSuffixes = new Set(['.md', '.png', '.jpg', '.jpeg', '.gif', '.svg', '.json', '.css']);

function copyDocs(source, destination) {
  // Only generated destinations belong here. Recreate them so removed source
  // pages cannot survive in a later build.
  if (!fs.statSync(source).isDirectory()) throw new Error(`Missing docs directory: ${source}`);
  fs.rmSync(destination, { recursive: true, force: true });
  fs.cpSync(source, destination, {
    recursive: true,
    filter: (file) => fs.statSync(file).isDirectory() || docSuffixes.has(path.extname(file).toLowerCase()),
  });
}

function preparePublic(sourceRoot = root) {
  const docsDir = path.join(sourceRoot, 'docs');
  // Validate the canonical source before replacing the previous generated tree.
  if (!fs.statSync(docsDir).isDirectory()) throw new Error(`Missing docs directory: ${docsDir}`);
  const publicDir = path.join(sourceRoot, 'frontend', '.generated', 'public');
  fs.rmSync(publicDir, { recursive: true, force: true });
  fs.cpSync(path.join(sourceRoot, 'frontend', 'public'), publicDir, { recursive: true });
  copyDocs(docsDir, path.join(publicDir, 'docs'));
  return publicDir;
}

function publishFrontend(sourceRoot = root) {
  const distDir = path.join(sourceRoot, 'frontend', 'dist');
  if (!fs.existsSync(path.join(distDir, 'index.html'))) {
    throw new Error(`Frontend build missing: ${distDir}`);
  }
  const staticDir = path.join(sourceRoot, 'frontend_static');
  fs.rmSync(staticDir, { recursive: true, force: true });
  fs.cpSync(distDir, staticDir, { recursive: true });
}

module.exports = { copyDocs, preparePublic, publishFrontend };

if (require.main === module) {
  const task = process.argv[2];
  if (task === 'prebuild') preparePublic();
  else if (task === 'postbuild') publishFrontend();
  else {
    console.error(`Unknown build task: ${task}`);
    process.exitCode = 1;
  }
}
