#!/bin/bash

# Fast UV Installation Script
# Optimized for quick installation without system admin on Windows

set -e

echo "Installing UV (Ultra-Fast Python Package Manager)..."

# Check if UV is already installed
if command -v uv &> /dev/null; then
    echo "UV already installed: $(uv --version)"
    exit 0
fi

# Method 1: Try pre-compiled binary (fastest) - no sudo needed for Windows
echo "Trying pre-compiled binary for Windows..."
if [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "cygwin" ]]; then
    # Git Bash on Windows
    curl -LsSf https://github.com/astral-sh/uv/releases/latest/download/uv-x86_64-pc-windows-msvc.zip -o uv.zip
    unzip -o uv uv.zip
    chmod +x uv/uv.exe
    mkdir -p ~/bin
    mv uv/uv.exe ~/bin/uv.exe
    rm -rf uv uv.zip
    echo "UV installed via Windows binary"
    echo " Path: ~/bin/uv.exe"
    exit 0
fi

# Method 2: Use pipx (medium speed) - no sudo needed
echo "Using pipx for installation..."
if command -v pipx &> /dev/null; then
    pipx install uv --force
    echo "UV installed via pipx"
    exit 0
fi

