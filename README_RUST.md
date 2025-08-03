# Bobsled Rust NIF

This is the Rust Native Implemented Function (NIF) component of Bobsled, providing fast embedded key-value storage using the Sled database engine.

## Prerequisites

- Rust 1.70+ (install from [rustup.rs](https://rustup.rs/))
- Elixir/Erlang (for integration)

## Building

### Quick build
```bash
./build.sh
```

### Manual build
```bash
# Development build
make dev

# Release build
make compile

# Or directly with cargo
cargo build --release
```

## Project Structure

```
src/
├── lib.rs       # Main NIF module with exported functions
└── atoms.rs     # Erlang atom definitions

native/
└── bobsled_nif/ # Alternative build configuration

.cargo/
└── config.toml  # Cargo configuration for NIF compilation

Cargo.toml       # Main project configuration
Makefile         # Build automation
build.sh         # Simple build script
```

## Available NIF Functions

- `open_db/1` - Open a database at the specified path, returns ResourceArc<DbHandle>
- `put/3` - Store a key-value pair, returns :ok or :error atom
- `get/2` - Retrieve a value by key, returns Some(Binary) or None
- `delete/2` - Remove a key-value pair, returns :ok or :error atom
- `flush/1` - Flush database to disk, returns :ok or :error atom
- `size/1` - Get number of entries in database, returns u64
- `keys/1` - List all keys, returns Vec<Binary>
- `contains_key/2` - Check if key exists, returns boolean
- `clear/1` - Remove all data, returns :ok or :error atom

## Dependencies

- **rustler**: Erlang NIF bindings for Rust
- **sled**: Embedded database engine
- **parking_lot**: High-performance synchronization primitives
- **thiserror**: Error handling
- **serde**: Serialization framework

## Development

```bash
# Check code
make check

# Run tests
make test

# Format code
make fmt

# Lint code
make clippy

# Build documentation
make docs

# Watch for changes (requires cargo-watch)
make watch
```

## Integration with Elixir

The NIF is designed to be called from Elixir code. The module name is configured as `Elixir.Bobsled.Native` in the `rustler::init!` macro.

Example Elixir usage:
```elixir
# Open database
db = Bobsled.Native.open_db("/path/to/db")

# Store data
:ok = Bobsled.Native.put(db, "key", "value")

# Retrieve data  
case Bobsled.Native.get(db, "key") do
  nil -> :not_found
  value -> {:ok, value}
end

# Check existence
true = Bobsled.Native.contains_key(db, "key")

# Get database size
count = Bobsled.Native.size(db)

# List all keys
keys = Bobsled.Native.keys(db)

# Delete data
:ok = Bobsled.Native.delete(db, "key")

# Clear all data
:ok = Bobsled.Native.clear(db)

# Flush to disk
:ok = Bobsled.Native.flush(db)
```