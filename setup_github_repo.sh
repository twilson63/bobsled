#!/bin/bash

# GitHub Repository Setup Script for Bobsled

set -e

echo "🛷 Bobsled GitHub Repository Setup"
echo "=================================="
echo ""
echo "This script will help you set up the bobsled repository on GitHub."
echo ""

# Check if git is initialized
if [ ! -d .git ]; then
    echo "📁 Initializing git repository..."
    git init
    git branch -M main
fi

# Add all files
echo "📝 Adding files to git..."
git add .

# Create initial commit
echo "💾 Creating initial commit..."
git commit -m "Initial commit: Bobsled - High-performance Erlang NIF for Sled

- Erlang NIF wrapper for Sled embedded database
- Support for basic operations (put, get, delete)
- Advanced features: CAS, batch operations, transactions
- Hierarchical key support with list operations
- Comprehensive test suite and benchmarks
- Full documentation and examples"

echo ""
echo "✅ Local repository is ready!"
echo ""
echo "Next steps:"
echo ""
echo "1. Create a new repository on GitHub:"
echo "   - Go to https://github.com/new"
echo "   - Repository name: bobsled"
echo "   - Description: High-performance Erlang NIF for Sled embedded database"
echo "   - Public repository"
echo "   - DO NOT initialize with README, license, or .gitignore"
echo ""
echo "2. After creating the empty repository on GitHub, run these commands:"
echo ""
echo "   git remote add origin https://github.com/twilson63/bobsled.git"
echo "   git push -u origin main"
echo ""
echo "3. Optional: Set up repository settings on GitHub:"
echo "   - Add topics: erlang, rust, database, key-value, nif, sled"
echo "   - Enable GitHub Pages if you want documentation"
echo "   - Set up branch protection rules for main branch"
echo ""
echo "4. Create the first release:"
echo "   ./prepare_release.sh 1.0.0"
echo ""

# Show current status
echo "Current git status:"
echo "==================="
git status --short