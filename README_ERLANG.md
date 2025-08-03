# Bobsled Erlang NIF Wrapper

A comprehensive Erlang wrapper for the high-performance bobsled NIF, providing a key-value database interface using the sled embedded database written in Rust.

## Overview

Bobsled provides lock-free, high-performance database operations with:

- **Lock-free Architecture**: Concurrent B+ tree operations without blocking
- **High Write Performance**: 1.7x-22.3x faster writes than traditional embedded databases  
- **Thread Safety**: All operations are inherently thread-safe
- **Zero-Copy Reads**: Efficient memory usage when data is in cache
- **Crash Safety**: Atomic writes with automatic recovery
- **Compression**: Built-in zstd compression for storage efficiency

## Quick Start

### Basic Operations

```erlang
% Open database
{ok, Db} = bobsled:open(<<"/tmp/mydb">>, [
    {cache_capacity, 1024 * 1024 * 1024},  % 1GB cache
    {mode, fast},                          % Fast mode
    {compression_factor, 3}                % Default compression
]).

% Put data
ok = bobsled:put(Db, <<"user:alice">>, <<"Alice Smith">>).

% Get data  
{ok, <<"Alice Smith">>} = bobsled:get(Db, <<"user:alice">>).
not_found = bobsled:get(Db, <<"user:nonexistent">>).

% Delete data
ok = bobsled:delete(Db, <<"user:alice">>).

% Close database
ok = bobsled:close(Db).
```

### Advanced Operations

```erlang
% Compare and swap for atomic updates
ok = bobsled:compare_and_swap(Db, <<"counter">>, <<"0">>, <<"1">>).

% Batch operations for better performance
KVPairs = [
    {<<"user:1">>, <<"Alice">>},
    {<<"user:2">>, <<"Bob">>},
    {<<"user:3">>, <<"Charlie">>}
],
ok = bobsled:batch_put(Db, KVPairs).

% List keys with prefix
{ok, UserKeys} = bobsled:list(Db, <<"user:">>).

% Fold over data with prefix
CountUsers = fun(_Key, _Value, Count) -> Count + 1 end,
{ok, UserCount} = bobsled:fold(Db, CountUsers, 0, <<"user:">>).
```

### Transactions

```erlang
% Atomic account transfer
TransferFun = fun(TxnHandle) ->
    case bobsled:get(TxnHandle, <<"account:alice">>) of
        {ok, AliceBalance} ->
            case bobsled:get(TxnHandle, <<"account:bob">>) of
                {ok, BobBalance} ->
                    NewAlice = binary_to_integer(AliceBalance) - 100,
                    NewBob = binary_to_integer(BobBalance) + 100,
                    ok = bobsled:put(TxnHandle, <<"account:alice">>, 
                                   integer_to_binary(NewAlice)),
                    ok = bobsled:put(TxnHandle, <<"account:bob">>, 
                                   integer_to_binary(NewBob)),
                    {ok, transferred};
                not_found ->
                    {error, bob_not_found}
            end;
        not_found ->
            {error, alice_not_found}
    end
end,

{ok, transferred} = bobsled:transaction(Db, TransferFun).
```

## API Reference

### Database Management

#### `bobsled:open/2`
```erlang
open(Path :: binary(), Options :: [open_option()]) -> 
    {ok, db_handle()} | {error, error_reason()}.
```

Opens or creates a database at the specified path.

**Options:**
- `{cache_capacity, Bytes}` - Cache size in bytes (default: 1GB)
- `{mode, fast | safe}` - fast = no immediate sync, safe = sync every write
- `{compression_factor, 0..22}` - Compression level (default: 3)
- `{flush_every_ms, Ms}` - Background flush interval (default: 500ms)

#### `bobsled:close/1`
```erlang
close(DbHandle :: db_handle()) -> ok | {error, error_reason()}.
```

Closes the database and releases resources.

### Basic Operations

#### `bobsled:put/3`
```erlang
put(DbHandle :: db_handle(), Key :: binary(), Value :: binary()) ->
    ok | {error, error_reason()}.
```

Writes a key-value pair to the database.

#### `bobsled:get/2`
```erlang
get(DbHandle :: db_handle(), Key :: binary()) ->
    {ok, binary()} | not_found | {error, error_reason()}.
```

Reads a value by key from the database.

#### `bobsled:delete/2`
```erlang
delete(DbHandle :: db_handle(), Key :: binary()) ->
    ok | {error, error_reason()}.
```

Deletes a key-value pair from the database.

### Advanced Operations

#### `bobsled:compare_and_swap/4`
```erlang
compare_and_swap(DbHandle :: db_handle(), Key :: binary(),
                OldValue :: binary() | not_found, NewValue :: binary()) ->
    ok | {error, cas_failed} | {error, error_reason()}.
```

Atomically updates a value only if it matches the expected value.

#### `bobsled:batch_put/2`
```erlang
batch_put(DbHandle :: db_handle(), KVPairs :: [{binary(), binary()}]) ->
    ok | {error, error_reason()}.
```

Writes multiple key-value pairs atomically.

#### `bobsled:transaction/2`
```erlang
transaction(DbHandle :: db_handle(), Fun :: transaction_fun()) ->
    {ok, term()} | {error, error_reason()}.
```

Executes multiple operations atomically within a transaction.

### Iteration Operations

#### `bobsled:list/2`
```erlang
list(DbHandle :: db_handle(), Prefix :: binary()) ->
    {ok, [binary()]} | {error, error_reason()}.
```

Lists all keys with the given prefix.

#### `bobsled:fold/4`
```erlang
fold(DbHandle :: db_handle(), Fun :: fold_fun(), 
     InitAcc :: term(), Prefix :: binary()) ->
    {ok, term()} | {error, error_reason()}.
```

Folds over key-value pairs with the given prefix.

### Utility Operations

#### `bobsled:flush/1`
```erlang
flush(DbHandle :: db_handle()) -> ok | {error, error_reason()}.
```

Manually flushes all pending writes to disk.

#### `bobsled:size_on_disk/1`
```erlang
size_on_disk(DbHandle :: db_handle()) -> 
    {ok, non_neg_integer()} | {error, error_reason()}.
```

Returns the current size of the database on disk in bytes.

## OTP Integration

### Application Structure

```erlang
% src/myapp_app.erl
-module(myapp_app).
-behaviour(application).

start(_StartType, _StartArgs) ->
    DbPath = application:get_env(myapp, db_path, <<"/var/db/myapp">>),
    DbOptions = application:get_env(myapp, db_options, [
        {cache_capacity, 1024 * 1024 * 1024},
        {mode, fast},
        {flush_every_ms, 1000}
    ]),
    bobsled_sup:start_link(DbPath, DbOptions).

stop(_State) ->
    bobsled_sup:stop().
```

### Supervisor Pattern

```erlang
% Use the provided bobsled_sup for managing database lifecycle
{ok, Pid} = bobsled_sup:start_link(<<"/var/db/myapp">>, Options).
```

### GenServer Wrapper

```erlang
% Use the provided bobsled_server for centralized access
ok = bobsled_server:put(<<"key">>, <<"value">>).
{ok, Value} = bobsled_server:get(<<"key">>).

% Or get direct handle for performance-critical code
DbHandle = bobsled_server:get_db_handle(),
ok = bobsled:put(DbHandle, <<"key">>, <<"value">>).
```

## Performance Considerations

### Write Optimization
- Use `{mode, fast}` for maximum write performance
- Batch operations for bulk updates using `batch_put/2`
- Adjust `flush_every_ms` based on durability requirements

### Read Performance  
- Increase `cache_capacity` for better cache hit rates
- Use iteration operations (`list/2`, `fold/4`) for processing multiple related entries
- Consider using direct database handle for high-frequency operations

### Memory Usage
- Monitor cache hit rates and adjust `cache_capacity` accordingly
- Use compression to reduce memory footprint
- Be aware that the database handle is shared across all processes

### Concurrency
- All operations are lock-free and thread-safe
- No reader/writer limits like traditional databases
- Consider using the gen_server wrapper for coordinated access patterns

## Error Handling

All functions return consistent error patterns:

- **Success**: `ok` or `{ok, Result}`
- **Missing Key**: `not_found` (not an error condition)
- **Errors**: `{error, Reason}` where Reason is descriptive

Common error reasons:
- `database_closed` - Operation on closed database
- `io_error` - Disk I/O failure
- `corruption` - Database corruption detected
- `out_of_memory` - Memory allocation failure
- `cas_failed` - Compare-and-swap conflict
- `{badarg, Term}` - Invalid arguments

## Testing

### Unit Tests

Run the provided unit tests:

```bash
rebar3 eunit
```

### Property-Based Testing

Example using PropEr:

```erlang
prop_put_get() ->
    ?FORALL({Key, Value}, {binary(), binary()},
        begin
            {ok, Db} = bobsled:open(test_db_path(), []),
            try
                ok = bobsled:put(Db, Key, Value),
                {ok, Value} = bobsled:get(Db, Key)
            after
                bobsled:close(Db)
            end
        end).
```

### Integration Testing

```erlang
concurrent_test() ->
    {ok, Db} = bobsled:open(<<"/tmp/concurrent_test">>, []),
    
    % Spawn multiple writers
    Writers = [spawn_link(fun() ->
        lists:foreach(fun(N) ->
            Key = iolist_to_binary([<<"worker_">>, integer_to_binary(WorkerId), 
                                  <<"_">>, integer_to_binary(N)]),
            ok = bobsled:put(Db, Key, <<"value">>)
        end, lists:seq(1, 1000))
    end) || WorkerId <- lists:seq(1, 10)],
    
    % Wait for completion and verify
    % ... test code ...
```

## Building and Installation

### Prerequisites

- Erlang/OTP 24+
- Rust 1.60+
- rebar3

### Build Steps

```bash
# Clone repository
git clone <repository>
cd bobsled

# Build NIF
make compile

# Build Erlang code
rebar3 compile

# Run tests
rebar3 eunit
```

### Integration into Your Project

Add to `rebar.config`:

```erlang
{deps, [
    {bobsled, {git, "https://github.com/your-org/bobsled.git", {tag, "v1.0.0"}}}
]}.
```

## Examples

See the `bobsled_example.erl` module for comprehensive usage examples including:

- Basic CRUD operations
- Transaction patterns  
- Batch operations
- Iteration examples
- Configuration store patterns
- User session management

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

[Apache 2.0](LICENSE)