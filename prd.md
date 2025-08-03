# Bobsled API Planning Document

Bobsled is a high-performance Erlang NIF for HyperBEAM that provides a key-value database interface using the sled embedded database written in Rust.

## Technology Overview

### Sled Database
Sled is a modern embedded database that offers:
- **Lock-free Architecture**: Concurrent B+ tree operations without blocking
- **High Write Performance**: 1.7x-22.3x faster writes than traditional embedded databases
- **Thread Safety**: All operations are inherently thread-safe
- **Zero-Copy Reads**: Efficient memory usage when data is in cache
- **Crash Safety**: Atomic writes with automatic recovery
- **Compression**: Built-in zstd compression for storage efficiency

### NIF Design Principles
- **Panic Safety**: All sled operations wrapped in panic-safe boundaries to prevent BEAM crashes
- **Resource Management**: Careful lifetime management across NIF boundary
- **Error Translation**: Consistent error handling between Rust and Erlang
- **Single Process**: Database instance shared across all Erlang processes (sled limitation) 

## 1. Database Management

### bobsled:open/2
**Purpose**: Create or open a sled database instance  
**Signature**: `open(Path :: binary(), Options :: proplists:proplist()) -> {ok, db_handle()} | {error, term()}`
**Inputs**:
- `Path` (binary): Directory path for the database files
- `Options` (proplist): Configuration options
  - `{cache_capacity, integer()}`: Cache size in bytes (default: 1GB)
  - `{mode, fast | safe}`: fast = no immediate sync, safe = sync every write
  - `{compression_factor, integer()}`: Compression level 0-22 (default: 3)
  - `{flush_every_ms, integer()}`: Background flush interval (default: 500ms)

**Output**: `{ok, DbHandle}` where `DbHandle` is an opaque database handle  
**Error**: `{error, Reason}` where Reason is an atom or tuple describing the error

**Note**: Due to sled's architecture, only one database instance can be opened per path. Multiple calls to open with the same path will return the same handle.

### bobsled:close/1
**Purpose**: Close the sled database and release resources  
**Signature**: `close(DbHandle :: db_handle()) -> ok | {error, term()}`
**Input**: `DbHandle` - Database handle from `open/2`  
**Output**: `ok`  
**Error**: `{error, Reason}` if close fails

**Note**: Closing flushes all pending writes to disk. After closing, the handle becomes invalid and any operations will fail.


## 2. Configuration and Management

### bobsled:flush/1
**Purpose**: Manually flush all pending writes to disk
**Signature**: `flush(DbHandle :: db_handle()) -> ok | {error, term()}`
**Input**: `DbHandle` - Database handle
**Output**: `ok` when flush completes
**Error**: `{error, Reason}` if flush fails

### bobsled:size_on_disk/1
**Purpose**: Get the current size of the database on disk
**Signature**: `size_on_disk(DbHandle :: db_handle()) -> {ok, integer()} | {error, term()}`
**Input**: `DbHandle` - Database handle
**Output**: `{ok, Bytes}` where Bytes is the size in bytes
**Error**: `{error, Reason}` if operation fails

## 3. Write Operations

### bobsled:put/3
**Purpose**: Write a key-value pair to the database  
**Signature**: `put(DbHandle :: db_handle(), Key :: binary(), Value :: binary()) -> ok | {error, term()}`
**Inputs**:
- `DbHandle`: Database handle
- `Key` (binary): The key to write
- `Value` (binary): The value to store

**Output**: `ok` on success  
**Error**: `{error, Reason}` where Reason describes the error

**Note**: This is an asynchronous operation using sled's lock-free architecture. Data is written to an in-memory buffer and flushed to disk based on the flush_every_ms setting.

### bobsled:delete/2
**Purpose**: Delete a key-value pair from the database
**Signature**: `delete(DbHandle :: db_handle(), Key :: binary()) -> ok | {error, term()}`
**Inputs**:
- `DbHandle`: Database handle  
- `Key` (binary): The key to delete

**Output**: `ok` regardless of whether key existed
**Error**: `{error, Reason}` if delete operation fails

### bobsled:compare_and_swap/4
**Purpose**: Atomically update a value only if it matches expected value
**Signature**: `compare_and_swap(DbHandle :: db_handle(), Key :: binary(), OldValue :: binary() | not_found, NewValue :: binary()) -> ok | {error, cas_failed} | {error, term()}`
**Inputs**:
- `DbHandle`: Database handle
- `Key` (binary): The key to update
- `OldValue` (binary | not_found): Expected current value
- `NewValue` (binary): New value to set

**Output**: `ok` if swap succeeded
**Error**: 
- `{error, cas_failed}` if current value doesn't match OldValue
- `{error, Reason}` for other errors

## 4. Read Operations

### bobsled:get/2
**Purpose**: Read a value by key from the database  
**Signature**: `get(DbHandle :: db_handle(), Key :: binary()) -> {ok, binary()} | not_found | {error, term()}`
**Inputs**:
- `DbHandle`: Database handle
- `Key` (binary): The key to read

**Output**: 
- `{ok, Value}` where `Value` is a binary
- `not_found` if key doesn't exist

**Error**: `{error, Reason}` for database errors

**Note**: Leverages sled's zero-copy reads when data is in cache for optimal performance.

## 4. Read and Iteration Operations

### bobsled:list/2  
**Purpose**: List all direct children of a key prefix
**Signature**: `list(DbHandle :: db_handle(), Prefix :: binary()) -> {ok, [binary()]} | {error, term()}`
**Inputs**:
- `DbHandle`: Database handle
- `Prefix` (binary): The key prefix to search

**Output**: `{ok, Children}` where Children is a list of keys with the given prefix
**Error**: `{error, Reason}` for database errors

**Example**:
```erlang
{ok, Children} = bobsled:list(DbHandle, <<"config/">>).
% Children = [<<"config/database">>, <<"config/server">>, <<"config/logging">>]
```

### bobsled:fold/4
**Purpose**: Fold over key-value pairs with a given prefix
**Signature**: `fold(DbHandle :: db_handle(), Fun :: fun((Key :: binary(), Value :: binary(), Acc) -> Acc), InitAcc :: term(), Prefix :: binary()) -> {ok, Acc} | {error, term()}`
**Inputs**:
- `DbHandle`: Database handle
- `Fun`: Function to apply to each key-value pair
- `InitAcc`: Initial accumulator value
- `Prefix` (binary): Key prefix to iterate over

**Output**: `{ok, FinalAcc}` where FinalAcc is the final accumulator value
**Error**: `{error, Reason}` for database errors

## 5. Batch Operations

### bobsled:batch_put/2
**Purpose**: Write multiple key-value pairs atomically
**Signature**: `batch_put(DbHandle :: db_handle(), KVPairs :: [{binary(), binary()}]) -> ok | {error, term()}`
**Inputs**:
- `DbHandle`: Database handle
- `KVPairs`: List of {Key, Value} tuples

**Output**: `ok` if all writes succeed
**Error**: `{error, Reason}` if any write fails (all writes are rolled back)

### bobsled:transaction/2
**Purpose**: Execute multiple operations atomically
**Signature**: `transaction(DbHandle :: db_handle(), Fun :: fun((txn_handle()) -> {ok, Result} | {error, term()})) -> {ok, Result} | {error, term()}`
**Inputs**:
- `DbHandle`: Database handle  
- `Fun`: Function that performs operations on the transaction handle

**Output**: `{ok, Result}` where Result is the return value of Fun
**Error**: `{error, Reason}` if transaction fails or is aborted

**Note**: Transactions use optimistic concurrency control. Conflicts are automatically retried.

## Implementation Notes

### Key Design Patterns

1. **Lock-Free Operations**: Sled's lock-free B+ tree ensures high concurrency without blocking

2. **Write Performance**: Writes are buffered in memory and flushed periodically for optimal throughput

3. **Read Consistency**: All reads see a consistent snapshot of the database

4. **Prefix Iterations**: List and fold operations use sled's efficient range queries

5. **Binary Keys/Values**: All keys and values must be binaries for efficient serialization

6. **Panic Safety**: All NIF operations are wrapped to prevent Rust panics from crashing the BEAM

### Architecture Considerations

1. **Single Process Limitation**
   - Sled allows only one open database instance per path
   - Database handle is shared across all Erlang processes
   - Requires careful resource management in OTP applications

2. **Memory Management**
   - Cache size should be tuned based on available memory
   - Background threads handle compression and flushing
   - Zero-copy operations minimize data movement

3. **Concurrency Model**
   - All operations are thread-safe and lock-free
   - No reader/writer limits like traditional databases
   - Optimistic concurrency for transactions

4. **Durability Guarantees**
   - Configurable flush intervals (fast vs safe modes)
   - Manual flush available for critical operations
   - Atomic recovery after crashes

### Error Handling

All functions follow consistent error handling:
- **Success**: `ok` or `{ok, Result}`
- **Missing Key**: `not_found` (not an error condition)
- **Errors**: `{error, Reason}` where Reason is descriptive

Common error reasons:
- `database_closed`: Operation on closed database
- `io_error`: Disk I/O failure
- `corruption`: Database corruption detected
- `out_of_memory`: Memory allocation failure
- `cas_failed`: Compare-and-swap conflict

### Performance Considerations

1. **Write Optimization**
   - Buffered writes with configurable flush intervals
   - Lock-free operations enable high concurrency
   - Batch operations for bulk updates

2. **Read Performance**
   - Zero-copy reads from cache
   - Efficient B+ tree traversal
   - Concurrent reads without blocking

3. **Memory Usage**
   - Configurable cache size
   - Automatic compression reduces memory footprint
   - Epoch-based garbage collection

4. **Background Operations**
   - Async flushing doesn't block user operations
   - Automatic space reclamation
   - Compression happens in background threads

## OTP Integration Guidelines

### Application Structure
```erlang
-module(bobsled_app).
-behaviour(application).

start(_StartType, _StartArgs) ->
    {ok, DbHandle} = bobsled:open(<<"/var/db/myapp">>, [
        {cache_capacity, 1024 * 1024 * 1024}, % 1GB
        {mode, fast},
        {flush_every_ms, 1000}
    ]),
    bobsled_sup:start_link(DbHandle).

stop(DbHandle) ->
    bobsled:close(DbHandle).
```

### Supervisor Pattern
```erlang  
-module(bobsled_sup).
-behaviour(supervisor).

init([DbHandle]) ->
    Children = [
        {bobsled_server, {bobsled_server, start_link, [DbHandle]},
         permanent, 5000, worker, [bobsled_server]}
    ],
    {ok, {{one_for_one, 5, 10}, Children}}.
```

### GenServer Wrapper
```erlang
-module(bobsled_server).
-behaviour(gen_server).

handle_call({get, Key}, _From, #{db := Db} = State) ->
    Reply = bobsled:get(Db, Key),
    {reply, Reply, State};
    
handle_call({put, Key, Value}, _From, #{db := Db} = State) ->
    Reply = bobsled:put(Db, Key, Value),
    {reply, Reply, State}.
```

### Best Practices

1. **Resource Management**
   - Open database in application start
   - Close database in application stop
   - Use a single gen_server to serialize access if needed

2. **Error Recovery**
   - Implement circuit breakers for database errors
   - Use supervisors to restart on crashes
   - Log all database errors for monitoring

3. **Performance Tuning**
   - Monitor cache hit rates
   - Adjust flush intervals based on durability needs
   - Use batch operations for bulk updates

4. **Testing Strategy**
   - Use temporary directories for test databases
   - Test crash recovery scenarios
   - Verify concurrent access patterns

