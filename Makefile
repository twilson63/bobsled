.PHONY: clean compile

CRATE_NAME = bobsled

# Default target
all: compile

# Compile the NIF
compile:
	cargo build --release

# Clean build artifacts
clean:
	cargo clean

# Development build (faster compilation, debug symbols)
dev:
	cargo build

# Test the Rust code
test:
	cargo test

# Check code formatting
fmt:
	cargo fmt

# Check for linting issues
clippy:
	cargo clippy

# Install dependencies and compile
install: compile

# Check if everything is ready
check:
	cargo check

# Build documentation
docs:
	cargo doc --open

# Watch for changes and rebuild (requires cargo-watch)
watch:
	cargo watch -x build