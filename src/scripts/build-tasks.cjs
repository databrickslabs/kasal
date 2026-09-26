#!/usr/bin/env node
// Cross-platform replacement for the mkdir -p / cp / rm -rf shell chains that
// broke `npm run build` under Windows cmd.exe (see fix(deploy): use npm.cmd
// on Windows — same class of bug, one layer up in the npm lifecycle).
const fs = require('fs');
const path = require('path');

const root = path.join(__dirname, '..');
const docSuffixes = new Set(['.md', '.png', '.jpg', '.jpeg', '.gif', '.svg', '.json', '.css']);

// Docs that stay in the repo but never ship in the app bundle (and so never in
// the pip wheel, which force-includes frontend_static). This is an exclude
// list, not an allowlist: shipped docs outnumber internal ones by about three
// to one and link to each other freely, so an allowlist would have to name
// every page, image and example. New internal pages are caught without an
// edit here by the status banner below.
//
// Paths are relative to src/docs, POSIX separators.
const EXCLUDED_DOC_DIRS = [
  'archive', // superseded pages, no longer maintained
  'reviews', // review notes
];
const EXCLUDED_DOC_FILES = [
  'crewai-engine-refactor-proposal.md',
  'conversational-flow-state-proposal.md',
  'dual-harness-backlog.md',
  'internal-dbu-tagging-plan.md',
  'kasal-platform-feedback-for-product-teams.md',
  'powerbi/ucmv-coverage-evaluation-and-roadmap.md', // names a customer
  'deployment/lakebase-persistence-across-redeploys.md', // proposal, not decided
];
// A Markdown page whose opening carries a "**Status: internal" banner (the
// convention in DOCUMENTATION_STYLE_GUIDE.md) is excluded too.
const INTERNAL_BANNER = /^(?:>\s*)?\*\*Status:\s*internal\b/im;
const BANNER_SCAN_BYTES = 2048;

function toDocPath(relative) {
  return relative.split(path.sep).join('/');
}

function hasInternalBanner(file) {
  if (path.extname(file).toLowerCase() !== '.md') return false;
  const fd = fs.openSync(file, 'r');
  try {
    const buffer = Buffer.alloc(BANNER_SCAN_BYTES);
    const read = fs.readSync(fd, buffer, 0, BANNER_SCAN_BYTES, 0);
    return INTERNAL_BANNER.test(buffer.toString('utf8', 0, read));
  } finally {
    fs.closeSync(fd);
  }
}

/** Whether a doc (path relative to the docs root) must stay out of the bundle. */
function isExcludedDocPath(docPath) {
  const segments = docPath.split('/');
  if (segments.some((segment) => EXCLUDED_DOC_DIRS.includes(segment))) return true;
  return EXCLUDED_DOC_FILES.includes(docPath);
}

function isExcludedDoc(file, docsRoot) {
  return isExcludedDocPath(toDocPath(path.relative(docsRoot, file))) || hasInternalBanner(file);
}

function walkFiles(dir) {
  const files = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) files.push(...walkFiles(full));
    else if (entry.isFile()) files.push(full);
  }
  return files;
}

const MD_LINK = /\]\(\s*<?([^)\s>]+)>?(?:\s+"[^"]*")?\s*\)/g;

/**
 * Relative Markdown links from shipped pages to excluded ones. Such a link
 * would render as "Document Not Found" in the app, and it is how an internal
 * page gets found, so the build refuses it.
 */
function findLinksToExcludedDocs(docsRoot) {
  const problems = [];
  for (const file of walkFiles(docsRoot)) {
    if (path.extname(file).toLowerCase() !== '.md' || isExcludedDoc(file, docsRoot)) continue;
    const text = fs.readFileSync(file, 'utf8');
    for (const match of text.matchAll(MD_LINK)) {
      const target = decodeURI(match[1].split('#')[0]);
      if (!target || /^[a-z][a-z0-9+.-]*:/i.test(target) || target.startsWith('/')) continue;
      const resolved = path.resolve(path.dirname(file), target);
      const relative = path.relative(docsRoot, resolved);
      if (relative.startsWith('..') || path.isAbsolute(relative)) continue;
      const excluded = isExcludedDocPath(toDocPath(relative))
        || (fs.existsSync(resolved) && fs.statSync(resolved).isFile() && hasInternalBanner(resolved));
      if (excluded) problems.push(`${toDocPath(path.relative(docsRoot, file))} -> ${match[1]}`);
    }
  }
  return problems;
}

/** Throw if any excluded doc is present under a built docs directory. */
function assertNoExcludedDocs(builtDocsDir) {
  if (!fs.existsSync(builtDocsDir)) return;
  const leaked = walkFiles(builtDocsDir)
    .filter((file) => isExcludedDoc(file, builtDocsDir))
    .map((file) => toDocPath(path.relative(builtDocsDir, file)));
  if (leaked.length) {
    throw new Error(`Internal docs must not ship, found in ${builtDocsDir}:\n  ${leaked.join('\n  ')}`);
  }
}

function validateDocsSource(source) {
  if (!fs.statSync(source).isDirectory()) throw new Error(`Missing docs directory: ${source}`);
  const badLinks = findLinksToExcludedDocs(source);
  if (badLinks.length) {
    throw new Error(`Shipped docs link to docs that do not ship:\n  ${badLinks.join('\n  ')}`);
  }
}

function copyDocs(source, destination) {
  // Only generated destinations belong here. Recreate them so removed source
  // pages cannot survive in a later build.
  validateDocsSource(source);
  fs.rmSync(destination, { recursive: true, force: true });
  fs.cpSync(source, destination, {
    recursive: true,
    filter: (file) => {
      if (fs.statSync(file).isDirectory()) {
        const relative = toDocPath(path.relative(source, file));
        return !relative || !isExcludedDocPath(relative);
      }
      return docSuffixes.has(path.extname(file).toLowerCase()) && !isExcludedDoc(file, source);
    },
  });
  assertNoExcludedDocs(destination);
}

function preparePublic(sourceRoot = root) {
  const docsDir = path.join(sourceRoot, 'docs');
  // Validate the canonical source before replacing the previous generated tree.
  validateDocsSource(docsDir);
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
  // A stale or hand-assembled dist must not smuggle internal docs into the wheel.
  assertNoExcludedDocs(path.join(distDir, 'docs'));
  const staticDir = path.join(sourceRoot, 'frontend_static');
  fs.rmSync(staticDir, { recursive: true, force: true });
  fs.cpSync(distDir, staticDir, { recursive: true });
}

module.exports = {
  EXCLUDED_DOC_DIRS,
  EXCLUDED_DOC_FILES,
  assertNoExcludedDocs,
  copyDocs,
  findLinksToExcludedDocs,
  isExcludedDocPath,
  preparePublic,
  publishFrontend,
};

if (require.main === module) {
  const task = process.argv[2];
  if (task === 'prebuild') preparePublic();
  else if (task === 'postbuild') publishFrontend();
  else {
    console.error(`Unknown build task: ${task}`);
    process.exitCode = 1;
  }
}
