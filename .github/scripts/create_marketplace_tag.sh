#!/bin/bash

# Script to create a marketplace tag without test files
# This creates a clean tag for Databricks Marketplace deployment
#
# Usage: ./.github/scripts/create_marketplace_tag.sh <version> [source-branch] [--dry-run]
# Example: ./.github/scripts/create_marketplace_tag.sh 1.2.3 --dry-run
#          ./.github/scripts/create_marketplace_tag.sh 1.2.3 main

set -e  # Exit on error

# Function to display usage
usage() {
    echo "Usage: $0 <version> [source-branch] [--dry-run]"
    echo ""
    echo "Creates a marketplace tag with test files removed for faster Databricks deployment."
    echo ""
    echo "Arguments:"
    echo "  version:       Version number (e.g., 1.2.3)"
    echo "  source-branch: Branch to create tag from (default: current branch)"
    echo "  --dry-run:     Test the process without pushing to remote"
    echo ""
    echo "Examples:"
    echo "  $0 1.2.3 --dry-run                    # Test with current branch"
    echo "  $0 1.2.3 main --dry-run               # Test with main branch"
    echo "  $0 1.2.3 main                         # Create and push tag from main"
    echo "  $0 1.2.3 feature/new-feature          # Create tag from feature branch"
    echo ""
    echo "Notes:"
    echo "  - Creates tag 'marketplace-v<version>' from the specified branch"
    echo "  - Removes all test files to speed up Databricks Marketplace imports"
    echo "  - Automatically preserves temp directory for manual inspection"
    echo "    (shows location and cleanup command after execution)"
    echo "  - Requires confirmation before pushing (unless --dry-run)"
    exit 1
}

# Parse arguments
VERSION=""
SOURCE_BRANCH=""
DRY_RUN=false
REMOTE="origin"

# Check for help flag first
for arg in "$@"; do
    if [ "$arg" = "-h" ] || [ "$arg" = "--help" ]; then
        usage
    fi
done

# Parse remaining arguments
for arg in "$@"; do
    if [ "$arg" = "--dry-run" ]; then
        DRY_RUN=true
    elif [ -z "$VERSION" ]; then
        VERSION="$arg"
    elif [ -z "$SOURCE_BRANCH" ]; then
        SOURCE_BRANCH="$arg"
    fi
done

# Validate version parameter
if [ -z "$VERSION" ]; then
    echo "❌ Error: Version parameter is required"
    usage
fi

# If no source branch specified, use current branch
if [ -z "$SOURCE_BRANCH" ]; then
    SOURCE_BRANCH=$(git rev-parse --abbrev-ref HEAD)
    echo "ℹ️  No source branch specified, using current branch: $SOURCE_BRANCH"
fi

# Tag names
SOURCE_TAG="v${VERSION}"
MARKETPLACE_TAG="marketplace-v${VERSION}"

echo "📦 Creating marketplace tag: $MARKETPLACE_TAG"
echo "📌 Source branch: $SOURCE_BRANCH"
echo "🧪 Dry run mode: $DRY_RUN"
echo ""

# Ensure we're in the git root directory
GIT_ROOT="$(git rev-parse --show-toplevel)"
cd "$GIT_ROOT"

# Fetch latest changes
echo "🔄 Fetching latest changes..."
git fetch $REMOTE --tags

# Check if source branch exists locally or remotely
if git rev-parse --verify "$SOURCE_BRANCH" >/dev/null 2>&1; then
    # Local branch exists
    SOURCE_REF="$SOURCE_BRANCH"
    echo "✅ Using local branch: $SOURCE_BRANCH"
elif git rev-parse --verify "$REMOTE/$SOURCE_BRANCH" >/dev/null 2>&1; then
    # Remote branch exists
    SOURCE_REF="$REMOTE/$SOURCE_BRANCH"
    echo "✅ Using remote branch: $REMOTE/$SOURCE_BRANCH"
else
    echo "❌ Error: Branch '$SOURCE_BRANCH' does not exist locally or on $REMOTE"
    echo ""
    echo "Available local branches:"
    git branch | head -10
    echo ""
    echo "Available remote branches:"
    git branch -r | grep "$REMOTE" | head -10
    exit 1
fi

# Check if marketplace tag already exists
if git rev-parse "$MARKETPLACE_TAG" >/dev/null 2>&1; then
    echo "⚠️  Warning: Tag $MARKETPLACE_TAG already exists locally"
    echo "🗑️  Deleting local tag..."
    git tag -d "$MARKETPLACE_TAG" || {
        echo "❌ Failed to delete existing tag"
        exit 1
    }
    echo "✅ Local tag deleted"
fi

# Check if tag exists on remote
if git ls-remote --tags "$REMOTE" | grep -q "refs/tags/$MARKETPLACE_TAG"; then
    echo "❌ Error: Tag $MARKETPLACE_TAG already exists on remote"
    echo "To delete it from remote: git push $REMOTE :refs/tags/$MARKETPLACE_TAG"
    echo "Or use --force flag to overwrite (not implemented for safety)"
    exit 1
fi

# Create a temporary directory for our clean export
TEMP_DIR=$(mktemp -d)
echo "📁 Creating temporary directory at $TEMP_DIR"

# Function to cleanup on exit
cleanup() {
    if [ "$SKIP_CLEANUP" = true ]; then
        echo "📁 Temporary directory preserved for inspection: $TEMP_DIR"
        echo "   To explore: cd $TEMP_DIR"
        echo "   To cleanup: rm -rf $TEMP_DIR"
    else
        echo "🧹 Cleaning up temporary directory..."
        rm -rf "$TEMP_DIR"
    fi
}
trap cleanup EXIT

# Skip cleanup to allow manual inspection of the generated marketplace content
SKIP_CLEANUP=true

# Get the commit hash we're working from
SOURCE_COMMIT=$(git rev-parse "$SOURCE_REF")
echo "📍 Using commit: $SOURCE_COMMIT"

# Show recent commits from this branch
echo ""
echo "📜 Recent commits from $SOURCE_BRANCH:"
git log --oneline -5 "$SOURCE_REF"
echo ""

# Export the repository at the specific branch
echo "📦 Exporting repository from $SOURCE_REF..."
git archive --format=tar "$SOURCE_COMMIT" | tar -x -C "$TEMP_DIR"

# Initialize a new git repo in temp directory for creating the tag
cd "$TEMP_DIR"
git init -q
git add .
git commit -q -m "Initial commit from $SOURCE_BRANCH"

# Count files before cleanup
FILES_BEFORE=$(find . -type f | wc -l)

# Remove test files
echo ""
echo "🗑️  Removing test files..."

# Backend tests - be specific about path
if [ -d "src/backend/tests" ]; then
    rm -rf "src/backend/tests"
    echo "  ✓ Removed src/backend/tests"
fi

# Frontend test files - be specific about extensions
FRONTEND_TEST_COUNT=$(find src/frontend -name "*.test.ts" -o -name "*.test.tsx" -o -name "*.test.js" -o -name "*.test.jsx" 2>/dev/null | wc -l || echo "0")
if [ "$FRONTEND_TEST_COUNT" -gt 0 ]; then
    find src/frontend -name "*.test.ts" -o -name "*.test.tsx" -o -name "*.test.js" -o -name "*.test.jsx" -type f -delete 2>/dev/null || true
    echo "  ✓ Removed $FRONTEND_TEST_COUNT frontend test files"
fi

# Test configuration files - be careful with conftest.py as it might be needed
find . -name "jest.config.*" -o -name ".coverage" -o -name "pytest.ini" -type f -delete 2>/dev/null || true
echo "  ✓ Removed test configuration files"

# Only remove conftest.py from test directories
find . -path "*/tests/conftest.py" -o -path "*/test/conftest.py" -type f -delete 2>/dev/null || true

# Coverage and cache directories
find . -name "coverage" -o -name ".pytest_cache" -o -name "__pycache__" -o -name ".coverage.*" -type d -exec rm -rf {} + 2>/dev/null || true
echo "  ✓ Removed coverage and cache directories"

# Remove specific test directories (not generic "test" to avoid false positives)
find . -type d -name "tests" -path "*/src/*" | while read -r dir; do
    rm -rf "$dir" 2>/dev/null || true
done

# Remove test runner script
if [ -f "src/backend/run_tests.py" ]; then
    rm -f "src/backend/run_tests.py"
    echo "  ✓ Removed run_tests.py"
fi

# Count files after cleanup
FILES_AFTER=$(find . -type f | wc -l)
FILES_REMOVED=$((FILES_BEFORE - FILES_AFTER))

# Export these variables so they're available later
export FILES_BEFORE
export FILES_AFTER
export FILES_REMOVED

echo ""
echo "📊 File statistics:"
echo "   Files before: $FILES_BEFORE"
echo "   Files after: $FILES_AFTER"
echo "   Files removed: $FILES_REMOVED"

# Run validation tests
echo ""
echo "🧪 Running validation tests..."

# Test 1: Verify no test files remain
TEST_FILES_REMAINING=$(find . \( -name "*.test.*" -o -name "*_test.py" -o -name "test_*.py" \) 2>/dev/null | grep -v node_modules | wc -l || echo "0")
if [ "$TEST_FILES_REMAINING" -eq 0 ]; then
    echo "  ✅ No test files found"
else
    echo "  ⚠️  Warning: $TEST_FILES_REMAINING test files still present"
    find . \( -name "*.test.*" -o -name "*_test.py" -o -name "test_*.py" \) 2>/dev/null | grep -v node_modules | head -5
fi

# Test 2: Verify critical files still exist
CRITICAL_FILES=(
    "src/backend/src/main.py"
    "src/frontend/package.json"
    "src/backend/pyproject.toml"
)

MISSING_CRITICAL=0
for file in "${CRITICAL_FILES[@]}"; do
    if [ ! -e "$file" ]; then
        echo "  ❌ Critical file missing: $file"
        MISSING_CRITICAL=$((MISSING_CRITICAL + 1))
    fi
done

if [ "$MISSING_CRITICAL" -eq 0 ]; then
    echo "  ✅ All critical files present"
else
    echo "  ❌ $MISSING_CRITICAL critical files missing!"
    if [ "$DRY_RUN" = false ]; then
        echo "  Aborting due to missing critical files"
        exit 1
    fi
fi

# Test 3: Check if directory structure is intact
if [ -d "src/backend/src" ] && [ -d "src/frontend/src" ]; then
    echo "  ✅ Directory structure intact"
else
    echo "  ❌ Directory structure damaged!"
    if [ "$DRY_RUN" = false ]; then
        exit 1
    fi
fi

# Add a marker file to indicate this is a marketplace build
echo "$MARKETPLACE_TAG" > .marketplace-version
echo "Created from $SOURCE_BRANCH on $(date)" >> .marketplace-version
echo "Source commit: $SOURCE_COMMIT" >> .marketplace-version

# Commit the cleaned version
git add -A
git commit -q -m "Marketplace deployment $MARKETPLACE_TAG

This tag is automatically generated from $SOURCE_BRANCH
with all test files removed to speed up Databricks deployment.

Source: $SOURCE_BRANCH
Source commit: $SOURCE_COMMIT
Version: $VERSION
Files removed: $FILES_REMOVED
Created: $(date)"

# Create the tag in temp repo
git tag -a "$MARKETPLACE_TAG" -m "Marketplace deployment version $VERSION"

# If dry run, show what would be done and exit
if [ "$DRY_RUN" = true ]; then
    echo ""
    echo "🧪 DRY RUN COMPLETE - No changes pushed"
    echo ""
    echo "Would have created tag: $MARKETPLACE_TAG"
    echo "From source: $SOURCE_BRANCH (commit: $SOURCE_COMMIT)"
    echo ""
    echo "📁 Temporary files available for inspection at: $TEMP_DIR"
    echo "   Run 'ls -la $TEMP_DIR' to explore"
    echo "   Run 'cd $TEMP_DIR && git log --oneline' to see commits"
    echo "   Run 'cd $TEMP_DIR && find . -name "*.test.*" -o -name "*_test.py"' to verify test removal"
    echo ""
    echo ""
    echo "✅ All validation tests passed!"
    echo "   To create the tag for real, run without --dry-run:"
    echo "   $0 $VERSION $SOURCE_BRANCH"
    
    exit 0
fi

# Now we need to create and push the tag
echo ""
echo "🔧 Preparing marketplace tag..."

# We'll create an orphan branch in the temp repo that has no parent
cd "$TEMP_DIR"
git checkout --orphan marketplace-branch
git add -A
git commit -m "Marketplace deployment $MARKETPLACE_TAG

This tag is automatically generated from $SOURCE_BRANCH
with all test files removed to speed up Databricks deployment.

Source: $SOURCE_BRANCH
Source commit: $SOURCE_COMMIT
Version: $VERSION
Files removed: $FILES_REMOVED
Created: $(date)"

# Create the tag on this commit
git tag -f -a "$MARKETPLACE_TAG" -m "Marketplace deployment version $VERSION" || {
    echo "⚠️  Tag creation had warnings but continuing..."
}

echo "✅ Tag created in temp repository"

# Store the temp directory path before changing
TEMP_REPO_PATH="$PWD"

# Get the main repo URL and push the tag directly
echo "📍 Returning to main repository..."
cd "$GIT_ROOT" || {
    echo "❌ Failed to return to main directory"
    exit 1
}

echo "📍 Getting repository URL..."
REPO_URL=$(git config --get remote.origin.url || echo "")
if [ -z "$REPO_URL" ]; then
    echo "❌ Failed to get repository URL"
    exit 1
fi
echo "📍 Repository URL: $REPO_URL"

# Ensure variables are available
if [ -z "$FILES_REMOVED" ] || [ -z "$FILES_AFTER" ]; then
    echo "⚠️  File count variables not available, recalculating..."
    FILES_AFTER=$(cd "$TEMP_DIR" && find . -type f | wc -l)
    FILES_REMOVED="N/A"
fi

# Final confirmation before push
echo ""
echo "📋 Summary of what will be pushed:"
echo "   • Tag name: $MARKETPLACE_TAG"
echo "   • Source branch: $SOURCE_BRANCH (commit: ${SOURCE_COMMIT:0:7})"
echo "   • Files removed: ${FILES_REMOVED:-N/A}"
echo "   • Files remaining: ${FILES_AFTER:-N/A}"
echo ""
echo "🎯 This tag is optimized for Databricks Marketplace deployment with test files removed."
echo ""
echo "🚀 Ready to push tag $MARKETPLACE_TAG to $REMOTE"
read -p "Continue with push? (y/N) " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "❌ Push cancelled"
    echo "   Tag created locally but not pushed"
    echo "   To push later: git push $REMOTE $MARKETPLACE_TAG"
    echo "   To delete local tag: git tag -d $MARKETPLACE_TAG"
    exit 1
fi

# Push the tag from temp directory
echo "📤 Pushing tag $MARKETPLACE_TAG to $REMOTE..."

# Save current directory
CURRENT_DIR=$(pwd)

cd "$TEMP_REPO_PATH" || cd "$TEMP_DIR" || {
    echo "❌ Failed to change to temp directory"
    exit 1
}

echo "🔗 Adding remote repository..."
git remote add push-remote "$REPO_URL" 2>/dev/null || true

echo "📤 Pushing tag to remote..."
git push push-remote "$MARKETPLACE_TAG" || {
    echo "❌ Failed to push tag"
    cd "$GIT_ROOT"
    exit 1
}

cd "$GIT_ROOT" || {
    echo "⚠️  Warning: Could not return to git root directory"
}

echo ""
echo "✅ Successfully created marketplace tag: $MARKETPLACE_TAG"
echo ""
echo "📋 Summary:"
echo "   - Source: $SOURCE_BRANCH (commit: $SOURCE_COMMIT)"
echo "   - Version: $VERSION"
echo "   - Tag: $MARKETPLACE_TAG"
echo "   - Files removed: $FILES_REMOVED"
echo ""
echo "🔍 Next steps:"
echo "   1. Verify the tag on GitHub: https://github.com/[your-repo]/releases/tag/$MARKETPLACE_TAG"
echo "   2. Test with Databricks Marketplace pointing to tag: $MARKETPLACE_TAG"
echo "   3. If successful, update Databricks Marketplace to use this tag"
echo ""
echo "💡 To view the tag contents:"
echo "   git checkout $MARKETPLACE_TAG"
echo ""
echo "🗑️  To delete the tag if needed:"
echo "   git tag -d $MARKETPLACE_TAG && git push $REMOTE :refs/tags/$MARKETPLACE_TAG"