#!/usr/bin/env node
// Cross-platform replacement for the mkdir -p / cp / rm -rf shell chains that
// broke `npm run build` under Windows cmd.exe (see fix(deploy): use npm.cmd
// on Windows — same class of bug, one layer up in the npm lifecycle).
const fs = require('fs');
const path = require('path');

const root = path.join(__dirname, '..');
const docsDir = path.join(root, 'docs');

function copyMarkdown(destDir) {
  fs.mkdirSync(destDir, { recursive: true });
  if (!fs.existsSync(docsDir)) return;
  for (const file of fs.readdirSync(docsDir)) {
    if (file.endsWith('.md')) {
      fs.copyFileSync(path.join(docsDir, file), path.join(destDir, file));
    }
  }
}

const task = process.argv[2];

if (task === 'prebuild') {
  copyMarkdown(path.join(root, 'frontend', 'public', 'docs'));
} else if (task === 'postbuild') {
  const staticDir = path.join(root, 'frontend_static');
  fs.rmSync(staticDir, { recursive: true, force: true });
  fs.cpSync(path.join(root, 'frontend', 'dist'), staticDir, { recursive: true });
  copyMarkdown(path.join(staticDir, 'docs'));
} else {
  console.error(`Unknown build task: ${task}`);
  process.exit(1);
}
