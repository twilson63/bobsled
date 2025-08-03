#!/bin/bash

# Build script for Bobsled Rust NIF

set -e

echo "Building Bobsled NIF..."

# Check if Rust is installed
if ! command -v cargo &> /dev/null; then
    echo "Error: Rust/Cargo is not installed. Please install Rust from https://rustup.rs/"
    exit 1
fi

# Build the project
echo "Compiling Rust code..."
cargo build --release

# Check if the build was successful
if [ $? -eq 0 ]; then
    echo "Build successful!"
    echo "NIF binary location: target/release/libbobsled.dylib (macOS) or target/release/libbobsled.so (Linux)"
else
    echo "Build failed!"
    exit 1
fi

echo "Done!"