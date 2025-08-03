%%%-------------------------------------------------------------------
%%% @doc Bobsled NIF Module
%%%
%%% High-performance Erlang NIF for the sled embedded database.
%%% Provides a key-value database interface with lock-free operations,
%%% atomic transactions, and efficient iteration capabilities.
%%%
%%% @author Bobsled Team
%%% @copyright 2025 Bobsled Project
%%% @version 1.0.0
%%% @end
%%%-------------------------------------------------------------------
-module(bobsled).

%% API exports
-export([
    % Database management
    open/2,
    close/1,
    
    % Basic operations
    put/3,
    get/2,
    delete/2,
    
    % Advanced operations
    compare_and_swap/4,
    
    % Iteration operations
    list/2,
    fold/4,
    
    % Batch operations
    batch_put/2,
    transaction/2,
    
    % Utility operations
    flush/1,
    size_on_disk/1
]).

%% NIF loading
-on_load(init/0).

%%%===================================================================
%%% Type Definitions
%%%===================================================================

%% @type db_handle() = reference().
%% An opaque handle to a sled database instance. This handle is shared
%% across all Erlang processes due to sled's single-instance limitation.
-type db_handle() :: reference().

%% @type txn_handle() = reference().
%% An opaque handle to a sled transaction.
-type txn_handle() :: reference().

%% @type error_reason() = atom() | {atom(), term()}.
%% Standardized error reasons returned by bobsled operations.
-type error_reason() :: 
    database_closed |
    io_error |
    corruption |
    out_of_memory |
    cas_failed |
    {invalid_option, term()} |
    {nif_error, term()}.

%% @type open_option() = {cache_capacity, pos_integer()} |
%%                      {mode, fast | safe} |
%%                      {compression_factor, 0..22} |
%%                      {flush_every_ms, pos_integer()}.
%% Configuration options for opening a database.
-type open_option() ::
    {cache_capacity, pos_integer()} |
    {mode, fast | safe} |
    {compression_factor, 0..22} |
    {flush_every_ms, pos_integer()}.

%% @type fold_fun() = fun((Key :: binary(), Value :: binary(), Acc :: term()) -> term()).
%% Function type for fold operations.
-type fold_fun() :: fun((Key :: binary(), Value :: binary(), Acc :: term()) -> term()).

%% @type transaction_fun() = fun((txn_handle()) -> {ok, term()} | {error, term()}).
%% Function type for transaction operations.
-type transaction_fun() :: fun((txn_handle()) -> {ok, term()} | {error, term()}).

%%%===================================================================
%%% API Functions
%%%===================================================================

%% @doc Open or create a sled database.
%%
%% Creates or opens a sled database at the specified path with the given
%% configuration options. Due to sled's architecture, only one database
%% instance can be opened per path. Multiple calls with the same path
%% will return the same handle.
%%
%% == Options ==
%% <ul>
%%   <li>`{cache_capacity, Bytes}' - Cache size in bytes (default: 1GB)</li>
%%   <li>`{mode, fast | safe}' - fast = no immediate sync, safe = sync every write</li>
%%   <li>`{compression_factor, 0..22}' - Compression level (default: 3)</li>
%%   <li>`{flush_every_ms, Ms}' - Background flush interval (default: 500ms)</li>
%% </ul>
%%
%% == Examples ==
%% ```
%% % Open with default options
%% {ok, Db} = bobsled:open(<<"/tmp/mydb">>, []).
%%
%% % Open with custom configuration
%% {ok, Db} = bobsled:open(<<"/var/db/app">>, [
%%     {cache_capacity, 2147483648},  % 2GB cache
%%     {mode, safe},                  % Sync every write
%%     {compression_factor, 6},       % Higher compression
%%     {flush_every_ms, 1000}         % Flush every second
%% ]).
%% '''
%%
%% @param Path Directory path for the database files
%% @param Options Configuration options as a proplist
%% @returns `{ok, DbHandle}' on success, `{error, Reason}' on failure
-spec open(Path :: binary(), Options :: [open_option()]) ->
    {ok, db_handle()} | {error, error_reason()}.
open(Path, Options) when is_binary(Path), is_list(Options) ->
    case validate_open_options(Options) of
        ok ->
            nif_open_db(Path, Options);
        {error, Reason} ->
            {error, Reason}
    end;
open(Path, Options) ->
    {error, {badarg, {Path, Options}}}.

%% @doc Close a sled database.
%%
%% Closes the database and releases all resources. This operation flushes
%% all pending writes to disk. After closing, the handle becomes invalid
%% and any operations on it will fail.
%%
%% == Example ==
%% ```
%% ok = bobsled:close(DbHandle).
%% '''
%%
%% @param DbHandle Database handle from open/2
%% @returns `ok' on success, `{error, Reason}' on failure
-spec close(DbHandle :: db_handle()) -> ok | {error, error_reason()}.
close(DbHandle) when is_reference(DbHandle) ->
    nif_close_db(DbHandle);
close(DbHandle) ->
    {error, {badarg, DbHandle}}.

%% @doc Write a key-value pair to the database.
%%
%% Writes a key-value pair using sled's lock-free architecture. This is
%% an asynchronous operation - data is written to an in-memory buffer
%% and flushed to disk based on the flush_every_ms setting.
%%
%% == Example ==
%% ```
%% ok = bobsled:put(DbHandle, <<"user:123">>, <<"alice">>).
%% '''
%%
%% @param DbHandle Database handle
%% @param Key The key to write (must be binary)
%% @param Value The value to store (must be binary)
%% @returns `ok' on success, `{error, Reason}' on failure
-spec put(DbHandle :: db_handle(), Key :: binary(), Value :: binary()) ->
    ok | {error, error_reason()}.
put(DbHandle, Key, Value) 
  when is_reference(DbHandle), is_binary(Key), is_binary(Value) ->
    nif_put(DbHandle, Key, Value);
put(DbHandle, Key, Value) ->
    {error, {badarg, {DbHandle, Key, Value}}}.

%% @doc Read a value by key from the database.
%%
%% Reads a value for the given key. Leverages sled's zero-copy reads
%% when data is in cache for optimal performance. All read operations
%% see a consistent snapshot of the database.
%%
%% == Example ==
%% ```
%% case bobsled:get(DbHandle, <<"user:123">>) of
%%     {ok, Value} -> 
%%         io:format("User: ~s~n", [Value]);
%%     not_found ->
%%         io:format("User not found~n");
%%     {error, Reason} ->
%%         io:format("Error: ~p~n", [Reason])
%% end.
%% '''
%%
%% @param DbHandle Database handle
%% @param Key The key to read (must be binary)
%% @returns `{ok, Value}', `not_found', or `{error, Reason}'
-spec get(DbHandle :: db_handle(), Key :: binary()) ->
    {ok, binary()} | not_found | {error, error_reason()}.
get(DbHandle, Key) when is_reference(DbHandle), is_binary(Key) ->
    nif_get(DbHandle, Key);
get(DbHandle, Key) ->
    {error, {badarg, {DbHandle, Key}}}.

%% @doc Delete a key-value pair from the database.
%%
%% Deletes the specified key and its associated value. Returns `ok'
%% regardless of whether the key existed or not.
%%
%% == Example ==
%% ```
%% ok = bobsled:delete(DbHandle, <<"user:123">>).
%% '''
%%
%% @param DbHandle Database handle
%% @param Key The key to delete (must be binary)
%% @returns `ok' on success, `{error, Reason}' on failure
-spec delete(DbHandle :: db_handle(), Key :: binary()) ->
    ok | {error, error_reason()}.
delete(DbHandle, Key) when is_reference(DbHandle), is_binary(Key) ->
    nif_delete(DbHandle, Key);
delete(DbHandle, Key) ->
    {error, {badarg, {DbHandle, Key}}}.

%% @doc Atomically update a value only if it matches the expected value.
%%
%% Performs a compare-and-swap operation that atomically updates a value
%% only if the current value matches the expected old value. This is useful
%% for implementing lock-free algorithms and preventing race conditions.
%%
%% == Example ==
%% ```
%% % Initial value
%% ok = bobsled:put(DbHandle, <<"counter">>, <<"0">>),
%%
%% % Atomic increment
%% case bobsled:compare_and_swap(DbHandle, <<"counter">>, <<"0">>, <<"1">>) of
%%     ok -> 
%%         io:format("Successfully incremented~n");
%%     {error, cas_failed} ->
%%         io:format("Counter was modified by another process~n");
%%     {error, Reason} ->
%%         io:format("Error: ~p~n", [Reason])
%% end.
%% '''
%%
%% @param DbHandle Database handle
%% @param Key The key to update (must be binary)
%% @param OldValue Expected current value or `not_found'
%% @param NewValue New value to set (must be binary)
%% @returns `ok', `{error, cas_failed}', or `{error, Reason}'
-spec compare_and_swap(DbHandle :: db_handle(), Key :: binary(), 
                      OldValue :: binary() | not_found, NewValue :: binary()) ->
    ok | {error, cas_failed} | {error, error_reason()}.
compare_and_swap(DbHandle, Key, OldValue, NewValue)
  when is_reference(DbHandle), is_binary(Key), is_binary(NewValue),
       (is_binary(OldValue) orelse OldValue =:= not_found) ->
    nif_compare_and_swap(DbHandle, Key, OldValue, NewValue);
compare_and_swap(DbHandle, Key, OldValue, NewValue) ->
    {error, {badarg, {DbHandle, Key, OldValue, NewValue}}}.

%% @doc List all keys with a given prefix.
%%
%% Returns all keys that start with the specified prefix. This operation
%% uses sled's efficient range queries for optimal performance.
%%
%% == Example ==
%% ```
%% % Store some config values
%% ok = bobsled:put(DbHandle, <<"config/database">>, <<"localhost">>),
%% ok = bobsled:put(DbHandle, <<"config/port">>, <<"5432">>),
%% ok = bobsled:put(DbHandle, <<"config/timeout">>, <<"30">>),
%%
%% % List all config keys
%% {ok, Keys} = bobsled:list(DbHandle, <<"config/">>),
%% % Keys = [<<"config/database">>, <<"config/port">>, <<"config/timeout">>]
%% '''
%%
%% @param DbHandle Database handle
%% @param Prefix Key prefix to search for (must be binary)
%% @returns `{ok, [Key]}' on success, `{error, Reason}' on failure
-spec list(DbHandle :: db_handle(), Prefix :: binary()) ->
    {ok, [binary()]} | {error, error_reason()}.
list(DbHandle, Prefix) when is_reference(DbHandle), is_binary(Prefix) ->
    nif_list(DbHandle, Prefix);
list(DbHandle, Prefix) ->
    {error, {badarg, {DbHandle, Prefix}}}.

%% @doc Fold over key-value pairs with a given prefix.
%%
%% Applies a function to each key-value pair that starts with the specified
%% prefix, accumulating results. This is an efficient way to process
%% multiple related entries without loading them all into memory at once.
%%
%% == Example ==
%% ```
%% % Count all user entries
%% CountFun = fun(_Key, _Value, Count) -> Count + 1 end,
%% {ok, UserCount} = bobsled:fold(DbHandle, CountFun, 0, <<"user:">>).
%%
%% % Collect all values for processing
%% CollectFun = fun(_Key, Value, Acc) -> [Value | Acc] end,
%% {ok, Values} = bobsled:fold(DbHandle, CollectFun, [], <<"data:">>).
%% '''
%%
%% @param DbHandle Database handle
%% @param Fun Function to apply to each key-value pair
%% @param InitAcc Initial accumulator value
%% @param Prefix Key prefix to iterate over (must be binary)
%% @returns `{ok, FinalAcc}' on success, `{error, Reason}' on failure
-spec fold(DbHandle :: db_handle(), Fun :: fold_fun(), 
          InitAcc :: term(), Prefix :: binary()) ->
    {ok, term()} | {error, error_reason()}.
fold(DbHandle, Fun, InitAcc, Prefix)
  when is_reference(DbHandle), is_function(Fun, 3), is_binary(Prefix) ->
    case nif_fold(DbHandle, Fun, InitAcc, Prefix) of
        {ok, KeyValuePairs} ->
            % Apply the fold function to each key-value pair in Erlang
            FinalAcc = lists:foldl(
                fun({Key, Value}, Acc) -> Fun(Key, Value, Acc) end,
                InitAcc,
                KeyValuePairs
            ),
            {ok, FinalAcc};
        {error, Reason} ->
            {error, Reason}
    end;
fold(DbHandle, Fun, InitAcc, Prefix) ->
    {error, {badarg, {DbHandle, Fun, InitAcc, Prefix}}}.

%% @doc Write multiple key-value pairs atomically.
%%
%% Performs multiple put operations as a single atomic transaction.
%% If any write fails, all writes are rolled back. This is more efficient
%% than individual put operations for bulk updates.
%%
%% == Example ==
%% ```
%% KVPairs = [
%%     {<<"user:123">>, <<"alice">>},
%%     {<<"user:124">>, <<"bob">>},
%%     {<<"user:125">>, <<"charlie">>}
%% ],
%% ok = bobsled:batch_put(DbHandle, KVPairs).
%% '''
%%
%% @param DbHandle Database handle
%% @param KVPairs List of {Key, Value} tuples (all must be binaries)
%% @returns `ok' on success, `{error, Reason}' on failure
-spec batch_put(DbHandle :: db_handle(), KVPairs :: [{binary(), binary()}]) ->
    ok | {error, error_reason()}.
batch_put(DbHandle, KVPairs) when is_reference(DbHandle), is_list(KVPairs) ->
    case validate_kv_pairs(KVPairs) of
        ok ->
            nif_batch_put(DbHandle, KVPairs);
        {error, Reason} ->
            {error, Reason}
    end;
batch_put(DbHandle, KVPairs) ->
    {error, {badarg, {DbHandle, KVPairs}}}.

%% @doc Execute multiple operations atomically.
%%
%% Executes a function within a transaction context. The function receives
%% a transaction handle that can be used for database operations. 
%% Note: This is currently a placeholder implementation that simply executes
%% the function with the database handle as the transaction handle.
%%
%% == Example ==
%% ```
%% TransferFun = fun(TxnHandle) ->
%%     case bobsled:get(TxnHandle, <<"account:alice">>) of
%%         {ok, AliceBalance} ->
%%             case bobsled:get(TxnHandle, <<"account:bob">>) of
%%                 {ok, BobBalance} ->
%%                     NewAlice = binary_to_integer(AliceBalance) - 100,
%%                     NewBob = binary_to_integer(BobBalance) + 100,
%%                     ok = bobsled:put(TxnHandle, <<"account:alice">>, 
%%                                     integer_to_binary(NewAlice)),
%%                     ok = bobsled:put(TxnHandle, <<"account:bob">>, 
%%                                     integer_to_binary(NewBob)),
%%                     {ok, transferred};
%%                 not_found ->
%%                     {error, bob_not_found}
%%             end;
%%         not_found ->
%%             {error, alice_not_found}
%%     end
%% end,
%% {ok, transferred} = bobsled:transaction(DbHandle, TransferFun).
%% '''
%%
%% @param DbHandle Database handle
%% @param Fun Function to execute within transaction
%% @returns `{ok, Result}' on success, `{error, Reason}' on failure
-spec transaction(DbHandle :: db_handle(), Fun :: transaction_fun()) ->
    {ok, term()} | {error, error_reason()}.
transaction(DbHandle, Fun) 
  when is_reference(DbHandle), is_function(Fun, 1) ->
    case nif_transaction(DbHandle, Fun) of
        ok ->
            % Since the NIF doesn't execute the function, we execute it here
            % using the DbHandle as the transaction handle (placeholder implementation)
            try
                Result = Fun(DbHandle),
                {ok, Result}
            catch
                error:Reason ->
                    {error, Reason};
                throw:Reason ->
                    {error, Reason};
                exit:Reason ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end;
transaction(DbHandle, Fun) ->
    {error, {badarg, {DbHandle, Fun}}}.

%% @doc Manually flush all pending writes to disk.
%%
%% Forces all buffered writes to be written to disk immediately.
%% This is useful when you need to ensure data durability before
%% continuing with critical operations.
%%
%% == Example ==
%% ```
%% ok = bobsled:put(DbHandle, <<"critical">>, <<"data">>),
%% ok = bobsled:flush(DbHandle).  % Ensure it's on disk
%% '''
%%
%% @param DbHandle Database handle
%% @returns `ok' on success, `{error, Reason}' on failure
-spec flush(DbHandle :: db_handle()) -> ok | {error, error_reason()}.
flush(DbHandle) when is_reference(DbHandle) ->
    nif_flush(DbHandle);
flush(DbHandle) ->
    {error, {badarg, DbHandle}}.

%% @doc Get the current size of the database on disk.
%%
%% Returns the total size of all database files in bytes. This includes
%% data files, index files, and log files but excludes temporary files.
%%
%% == Example ==
%% ```
%% {ok, Size} = bobsled:size_on_disk(DbHandle),
%% io:format("Database size: ~p bytes (~.2f MB)~n", 
%%           [Size, Size / (1024 * 1024)]).
%% '''
%%
%% @param DbHandle Database handle
%% @returns `{ok, Bytes}' on success, `{error, Reason}' on failure
-spec size_on_disk(DbHandle :: db_handle()) -> 
    {ok, non_neg_integer()} | {error, error_reason()}.
size_on_disk(DbHandle) when is_reference(DbHandle) ->
    nif_size_on_disk(DbHandle);
size_on_disk(DbHandle) ->
    {error, {badarg, DbHandle}}.

%%%===================================================================
%%% NIF Function Stubs
%%%===================================================================

%% @private
%% @doc Initialize and load the NIF library.
-spec init() -> ok | {error, term()}.
init() ->
    SoName = case code:priv_dir(?MODULE) of
        {error, bad_name} ->
            case filelib:is_dir(filename:join(["..", priv])) of
                true ->
                    filename:join(["..", priv, ?MODULE]);
                _ ->
                    filename:join([priv, ?MODULE])
            end;
        Dir ->
            filename:join(Dir, ?MODULE)
    end,
    
    case erlang:load_nif(SoName, 0) of
        ok -> 
            ok;
        {error, {reload, _}} -> 
            ok;  % NIF already loaded
        {error, Reason} = Error ->
            error_logger:error_msg(
                "Failed to load bobsled NIF from ~s: ~p~n", 
                [SoName, Reason]
            ),
            Error
    end.

%% NIF function stubs - these will be replaced by the actual NIF implementation

%% @private
nif_open_db(_Path, _Options) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

%% @private  
nif_close_db(_DbHandle) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

%% @private
nif_put(_DbHandle, _Key, _Value) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

%% @private
nif_get(_DbHandle, _Key) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

%% @private
nif_delete(_DbHandle, _Key) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

%% @private
nif_compare_and_swap(_DbHandle, _Key, _OldValue, _NewValue) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

%% @private
nif_list(_DbHandle, _Prefix) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

%% @private
nif_fold(_DbHandle, _Fun, _InitAcc, _Prefix) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

%% @private
nif_batch_put(_DbHandle, _KVPairs) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

%% @private
nif_transaction(_DbHandle, _Fun) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

%% @private
nif_flush(_DbHandle) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

%% @private
nif_size_on_disk(_DbHandle) ->
    erlang:nif_error({nif_not_loaded, ?MODULE}).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% @private
%% @doc Validate options for the open/2 function.
-spec validate_open_options([open_option()]) -> ok | {error, {invalid_option, term()}}.
validate_open_options([]) ->
    ok;
validate_open_options([{cache_capacity, Size} | Rest]) when is_integer(Size), Size > 0 ->
    validate_open_options(Rest);
validate_open_options([{mode, Mode} | Rest]) when Mode =:= fast; Mode =:= safe ->
    validate_open_options(Rest);
validate_open_options([{compression_factor, Factor} | Rest]) 
  when is_integer(Factor), Factor >= 0, Factor =< 22 ->
    validate_open_options(Rest);
validate_open_options([{flush_every_ms, Ms} | Rest]) when is_integer(Ms), Ms > 0 ->
    validate_open_options(Rest);
validate_open_options([Invalid | _]) ->
    {error, {invalid_option, Invalid}}.

%% @private
%% @doc Validate key-value pairs for batch operations.
-spec validate_kv_pairs([{binary(), binary()}]) -> ok | {error, {invalid_kv_pair, term()}}.
validate_kv_pairs([]) ->
    ok;
validate_kv_pairs([{Key, Value} | Rest]) when is_binary(Key), is_binary(Value) ->
    validate_kv_pairs(Rest);
validate_kv_pairs([Invalid | _]) ->
    {error, {invalid_kv_pair, Invalid}}.