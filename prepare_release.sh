#!/bin/bash

# Bobsled Release Preparation Script

set -e

echo "🛷 Bobsled Release Preparation"
echo "=============================="

# Check if version is provided
if [ -z "$1" ]; then
    echo "Usage: ./prepare_release.sh <version>"
    echo "Example: ./prepare_release.sh 1.0.0"
    exit 1
fi

VERSION=$1
TAG="v$VERSION"

echo "Preparing release $VERSION..."

# Check for uncommitted changes
if ! git diff-index --quiet HEAD --; then
    echo "❌ Error: Uncommitted changes detected. Please commit or stash them."
    exit 1
fi

# Update version in Cargo.toml
echo "📝 Updating Cargo.toml version..."
if [[ "$OSTYPE" == "darwin"* ]]; then
    sed -i '' "s/version = \".*\"/version = \"$VERSION\"/" Cargo.toml
else
    sed -i "s/version = \".*\"/version = \"$VERSION\"/" Cargo.toml
fi

# Update rebar.config if it exists
if [ -f "rebar.config" ]; then
    echo "📝 Updating rebar.config version..."
    if [[ "$OSTYPE" == "darwin"* ]]; then
        sed -i '' "s/{tag, \".*\"}/{tag, \"$TAG\"}/" rebar.config
    else
        sed -i "s/{tag, \".*\"}/{tag, \"$TAG\"}/" rebar.config
    fi
fi

# Run tests
echo "🧪 Running tests..."
make clean
make compile

echo "Running Rust tests..."
cargo test

echo "Running Erlang tests..."
./run_eunit_tests.escript

# Check formatting
echo "🎨 Checking code formatting..."
cargo fmt -- --check
cargo clippy -- -D warnings

# Update CHANGELOG.md
echo "📋 Updating CHANGELOG.md..."
echo ""
echo "Please update CHANGELOG.md with the changes for version $VERSION"
echo "Press enter when done..."
read

# Commit version bump
echo "💾 Committing version bump..."
git add Cargo.toml rebar.config CHANGELOG.md
git commit -m "Bump version to $VERSION"

# Create tag
echo "🏷️  Creating tag $TAG..."
git tag -a "$TAG" -m "Release $VERSION"

echo ""
echo "✅ Release preparation complete!"
echo ""
echo "Next steps:"
echo "1. Review the changes: git show"
echo "2. Push to GitHub: git push origin main --tags"
echo "3. GitHub Actions will automatically build and create the release"
echo ""
echo "To undo these changes:"
echo "  git reset --hard HEAD~1"
echo "  git tag -d $TAG"