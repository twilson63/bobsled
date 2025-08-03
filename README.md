# Bobsled 🛷

A high-performance Erlang NIF (Native Implemented Function) for the [Sled](https://github.com/spacejam/sled) embedded database, providing blazing-fast key-value storage with ACID guarantees for BEAM applications.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Erlang/OTP](https://img.shields.io/badge/Erlang%2FOTP-24%2B-blue.svg)](http://www.erlang.org)
[![Rust](https://img.shields.io/badge/Rust-1.70%2B-orange.svg)](https://www.rust-lang.org)

## Features

- 🚀 **Blazing Fast**: Over 500K writes/sec and 1.9M reads/sec
- 🔒 **Thread-Safe**: Lock-free operations with built-in concurrency
- 💾 **ACID Compliance**: Atomic operations with crash recovery
- 🌳 **Hierarchical Keys**: Efficient path-based key organization  
- 🔄 **Transactions**: ACID transactions with optimistic concurrency
- 📦 **Batch Operations**: Atomic batch writes for bulk updates
- 🎯 **Compare-and-Swap**: Atomic CAS operations for coordination
- 🛡️ **Panic Safety**: Rust panics won't crash the BEAM VM

## Performance

Based on benchmarks with 100,000 operations:

| Operation | Throughput | Latency |
|-----------|------------|---------|
| Sequential Writes | 578,292 ops/sec | 1.73 μs |
| Sequential Reads | 1,962,092 ops/sec | 0.51 μs |
| Concurrent Writes (10 proc) | 804,388 ops/sec | 12.4 μs |
| Concurrent Reads (10 proc) | 2,974,331 ops/sec | 3.4 μs |
| Compare-and-Swap | 763,533 ops/sec | 1.31 μs |
| List Operations (1K items) | 4,553 ops/sec | 219.6 μs |

## Installation

### Prerequisites

- Erlang/OTP 24 or later
- Rust 1.70 or later
- Cargo (Rust package manager)

### Building from Source

```bash
git clone https://github.com/twilson63/bobsled.git
cd bobsled
make compile
```

### Rebar3 Integration

Add bobsled to your `rebar.config`:

```erlang
{deps, [
    {bobsled, {git, "https://github.com/twilson63/bobsled.git", {tag, "v1.0.0"}}}
]}.
```

## Quick Start

```erlang
% Open a database
{ok, Db} = bobsled:open(<<"/tmp/mydb">>, [
    {mode, fast},
    {cache_capacity, 100_000_000}  % 100MB cache
]),

% Basic operations
ok = bobsled:put(Db, <<"user:123">>, <<"Alice">>),
{ok, <<"Alice">>} = bobsled:get(Db, <<"user:123">>),
ok = bobsled:delete(Db, <<"user:123">>),

% Batch operations
ok = bobsled:batch_put(Db, [
    {<<"user:1">>, <<"Bob">>},
    {<<"user:2">>, <<"Carol">>},
    {<<"user:3">>, <<"Dave">>}
]),

% Compare-and-swap
ok = bobsled:put(Db, <<"counter">>, <<"0">>),
ok = bobsled:compare_and_swap(Db, <<"counter">>, <<"0">>, <<"1">>),
{error, cas_failed} = bobsled:compare_and_swap(Db, <<"counter">>, <<"0">>, <<"2">>),

% Hierarchical keys and listing
ok = bobsled:put(Db, <<"config/app/name">>, <<"MyApp">>),
ok = bobsled:put(Db, <<"config/app/version">>, <<"1.0">>),
ok = bobsled:put(Db, <<"config/db/host">>, <<"localhost">>),
{ok, Keys} = bobsled:list(Db, <<"config/app/">>),
% Keys = [<<"config/app/name">>, <<"config/app/version">>]

% Close database
ok = bobsled:close(Db).
```

## API Reference

### Database Management

#### `open/2`
```erlang
open(Path :: binary(), Options :: proplists:proplist()) -> 
    {ok, db_handle()} | {error, term()}.
```
Opens or creates a database at the specified path.

**Options:**
- `{mode, fast | safe}` - Performance vs durability tradeoff (default: `fast`)
- `{cache_capacity, pos_integer()}` - Cache size in bytes (default: 1GB)
- `{compression_factor, 0..22}` - Zstd compression level (default: 3)
- `{flush_every_ms, pos_integer()}` - Background flush interval (default: 500ms)

#### `close/1`
```erlang
close(DbHandle :: db_handle()) -> ok | {error, term()}.
```
Closes the database and flushes all pending writes.

### Basic Operations

#### `put/3`
```erlang
put(DbHandle :: db_handle(), Key :: binary(), Value :: binary()) -> 
    ok | {error, term()}.
```
Stores a key-value pair in the database.

#### `get/2`
```erlang
get(DbHandle :: db_handle(), Key :: binary()) -> 
    {ok, binary()} | not_found | {error, term()}.
```
Retrieves a value by key.

#### `delete/2`
```erlang
delete(DbHandle :: db_handle(), Key :: binary()) -> 
    ok | {error, term()}.
```
Deletes a key-value pair.

### Advanced Operations

#### `compare_and_swap/4`
```erlang
compare_and_swap(DbHandle :: db_handle(), Key :: binary(), 
                 OldValue :: binary() | not_found, NewValue :: binary()) -> 
    ok | {error, cas_failed} | {error, term()}.
```
Atomically updates a value only if it matches the expected old value.

#### `batch_put/2`
```erlang
batch_put(DbHandle :: db_handle(), KVPairs :: [{binary(), binary()}]) -> 
    ok | {error, term()}.
```
Atomically writes multiple key-value pairs.

#### `list/2`
```erlang
list(DbHandle :: db_handle(), Prefix :: binary()) -> 
    {ok, [binary()]} | {error, term()}.
```
Lists all keys with the given prefix.

#### `fold/4`
```erlang
fold(DbHandle :: db_handle(), 
     Fun :: fun((Key :: binary(), Value :: binary(), Acc) -> Acc),
     InitAcc :: term(), Prefix :: binary()) -> 
    {ok, Acc} | {error, term()}.
```
Folds over all key-value pairs with the given prefix.

### Utility Functions

#### `flush/1`
```erlang
flush(DbHandle :: db_handle()) -> ok | {error, term()}.
```
Manually flushes all pending writes to disk.

#### `size_on_disk/1`
```erlang
size_on_disk(DbHandle :: db_handle()) -> {ok, pos_integer()} | {error, term()}.
```
Returns the database size on disk in bytes.

## OTP Integration

### Application Example

```erlang
-module(myapp_db).
-behaviour(gen_server).

-export([start_link/0, get/1, put/2]).
-export([init/1, handle_call/3, handle_cast/2, terminate/2]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get(Key) ->
    gen_server:call(?MODULE, {get, Key}).

put(Key, Value) ->
    gen_server:call(?MODULE, {put, Key, Value}).

init([]) ->
    {ok, Db} = bobsled:open(<<"/var/db/myapp">>, [{mode, fast}]),
    {ok, #{db => Db}}.

handle_call({get, Key}, _From, #{db := Db} = State) ->
    Reply = bobsled:get(Db, Key),
    {reply, Reply, State};
handle_call({put, Key, Value}, _From, #{db := Db} = State) ->
    Reply = bobsled:put(Db, Key, Value),
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, #{db := Db}) ->
    bobsled:close(Db),
    ok.
```

### Supervisor Setup

```erlang
-module(myapp_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Children = [
        {myapp_db, {myapp_db, start_link, []},
         permanent, 5000, worker, [myapp_db]}
    ],
    {ok, {{one_for_one, 5, 10}, Children}}.
```

## Benchmarks

Run the comprehensive benchmark suite:

```bash
# Run all benchmarks
./run_benchmarks.escript

# Run specific benchmark
./run_benchmarks.escript --test writes --count 1000000

# Run hierarchical list benchmarks
./run_list_bench.escript
```

## Testing

Run the test suite:

```bash
# Run EUnit tests
./run_eunit_tests.escript

# Run with rebar3
rebar3 eunit
```

## Architecture

Bobsled uses Rust's [Sled](https://github.com/spacejam/sled) embedded database, which provides:

- **Lock-free B+ tree**: Concurrent operations without blocking
- **Log-structured storage**: Fast writes with compression
- **MVCC**: Multi-version concurrency control for snapshots
- **Crash safety**: Atomic writes with automatic recovery

The NIF wrapper ensures:
- **Panic safety**: Rust panics are caught and converted to Erlang errors
- **Resource management**: Proper cleanup of database handles
- **Type safety**: Binary-only keys and values for consistency
- **Error translation**: Rust errors mapped to Erlang terms

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- The [Sled](https://github.com/spacejam/sled) team for the excellent embedded database
- The [Rustler](https://github.com/rusterlium/rustler) team for Erlang-Rust interop
- The Erlang/OTP community for the amazing BEAM platform