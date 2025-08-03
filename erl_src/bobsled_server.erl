%%%-------------------------------------------------------------------
%%% @doc Bobsled Server
%%%
%%% GenServer wrapper for managing a bobsled database instance within
%%% an OTP application. Provides a centralized interface for database
%%% operations and handles resource management.
%%%
%%% @author Bobsled Team  
%%% @copyright 2025 Bobsled Project
%%% @end
%%%-------------------------------------------------------------------
-module(bobsled_server).
-behaviour(gen_server).

%% API
-export([
    start_link/2,
    stop/0,
    
    % Database operations
    put/2,
    get/1,
    delete/1,
    compare_and_swap/3,
    
    % Batch operations
    batch_put/1,
    transaction/1,
    
    % Iteration operations
    list/1,
    fold/3,
    
    % Utility operations
    flush/0,
    size_on_disk/0,
    
    % Direct database handle access
    get_db_handle/0
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% Server name
-define(SERVER, ?MODULE).

%% Server state
-record(state, {
    db_handle :: bobsled:db_handle(),
    db_path :: binary(),
    db_options :: [bobsled:open_option()],
    stats :: #{
        operations := non_neg_integer(),
        errors := non_neg_integer(),
        start_time := erlang:timestamp()
    }
}).

-type state() :: #state{}.

%%%===================================================================
%%% API Functions
%%%===================================================================

%% @doc Start the server with database path and options.
-spec start_link(DbPath :: binary(), DbOptions :: [bobsled:open_option()]) ->
    {ok, pid()} | {error, term()}.
start_link(DbPath, DbOptions) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, {DbPath, DbOptions}, []).

%% @doc Stop the server and close the database.
-spec stop() -> ok.
stop() ->
    gen_server:stop(?SERVER).

%% @doc Put a key-value pair through the server.
-spec put(Key :: binary(), Value :: binary()) -> ok | {error, term()}.
put(Key, Value) ->
    gen_server:call(?SERVER, {put, Key, Value}).

%% @doc Get a value by key through the server.
-spec get(Key :: binary()) -> {ok, binary()} | not_found | {error, term()}.
get(Key) ->
    gen_server:call(?SERVER, {get, Key}).

%% @doc Delete a key through the server.
-spec delete(Key :: binary()) -> ok | {error, term()}.
delete(Key) ->
    gen_server:call(?SERVER, {delete, Key}).

%% @doc Compare and swap through the server.
-spec compare_and_swap(Key :: binary(), OldValue :: binary() | not_found, 
                      NewValue :: binary()) -> 
    ok | {error, cas_failed} | {error, term()}.
compare_and_swap(Key, OldValue, NewValue) ->
    gen_server:call(?SERVER, {compare_and_swap, Key, OldValue, NewValue}).

%% @doc Batch put through the server.
-spec batch_put(KVPairs :: [{binary(), binary()}]) -> ok | {error, term()}.
batch_put(KVPairs) ->
    gen_server:call(?SERVER, {batch_put, KVPairs}).

%% @doc Execute transaction through the server.
-spec transaction(Fun :: bobsled:transaction_fun()) -> {ok, term()} | {error, term()}.
transaction(Fun) ->
    gen_server:call(?SERVER, {transaction, Fun}).

%% @doc List keys with prefix through the server.
-spec list(Prefix :: binary()) -> {ok, [binary()]} | {error, term()}.
list(Prefix) ->
    gen_server:call(?SERVER, {list, Prefix}).

%% @doc Fold over keys with prefix through the server.
-spec fold(Fun :: bobsled:fold_fun(), InitAcc :: term(), Prefix :: binary()) ->
    {ok, term()} | {error, term()}.
fold(Fun, InitAcc, Prefix) ->
    gen_server:call(?SERVER, {fold, Fun, InitAcc, Prefix}).

%% @doc Flush database through the server.
-spec flush() -> ok | {error, term()}.
flush() ->
    gen_server:call(?SERVER, flush).

%% @doc Get database size through the server.
-spec size_on_disk() -> {ok, non_neg_integer()} | {error, term()}.
size_on_disk() ->
    gen_server:call(?SERVER, size_on_disk).

%% @doc Get direct access to the database handle.
%% 
%% Use this when you need to perform operations directly on the database
%% handle without going through the gen_server. This can be useful for
%% high-performance scenarios where the gen_server overhead is not desired.
%%
%% @returns Database handle for direct operations
-spec get_db_handle() -> bobsled:db_handle().
get_db_handle() ->
    gen_server:call(?SERVER, get_db_handle).

%%%===================================================================
%%% gen_server Callbacks
%%%===================================================================

%% @doc Initialize the server and open the database.
%% @private
-spec init({binary(), [bobsled:open_option()]}) -> 
    {ok, state()} | {stop, term()}.
init({DbPath, DbOptions}) ->
    process_flag(trap_exit, true),
    
    case bobsled:open(DbPath, DbOptions) of
        {ok, DbHandle} ->
            State = #state{
                db_handle = DbHandle,
                db_path = DbPath,
                db_options = DbOptions,
                stats = #{
                    operations => 0,
                    errors => 0,
                    start_time => erlang:timestamp()
                }
            },
            
            error_logger:info_msg("Bobsled server started with database: ~s~n", 
                                [DbPath]),
            {ok, State};
        {error, Reason} ->
            error_logger:error_msg("Failed to open bobsled database ~s: ~p~n", 
                                 [DbPath, Reason]),
            {stop, {database_open_failed, Reason}}
    end.

%% @doc Handle synchronous calls.
%% @private
-spec handle_call(term(), {pid(), term()}, state()) ->
    {reply, term(), state()} | {stop, term(), term(), state()}.
handle_call({put, Key, Value}, _From, State) ->
    {Reply, NewState} = execute_operation(
        fun() -> bobsled:put(State#state.db_handle, Key, Value) end,
        State
    ),
    {reply, Reply, NewState};

handle_call({get, Key}, _From, State) ->
    {Reply, NewState} = execute_operation(
        fun() -> bobsled:get(State#state.db_handle, Key) end,
        State
    ),
    {reply, Reply, NewState};

handle_call({delete, Key}, _From, State) ->
    {Reply, NewState} = execute_operation(
        fun() -> bobsled:delete(State#state.db_handle, Key) end,
        State
    ),
    {reply, Reply, NewState};

handle_call({compare_and_swap, Key, OldValue, NewValue}, _From, State) ->
    {Reply, NewState} = execute_operation(
        fun() -> bobsled:compare_and_swap(State#state.db_handle, Key, OldValue, NewValue) end,
        State
    ),
    {reply, Reply, NewState};

handle_call({batch_put, KVPairs}, _From, State) ->
    {Reply, NewState} = execute_operation(
        fun() -> bobsled:batch_put(State#state.db_handle, KVPairs) end,
        State
    ),
    {reply, Reply, NewState};

handle_call({transaction, Fun}, _From, State) ->
    {Reply, NewState} = execute_operation(
        fun() -> bobsled:transaction(State#state.db_handle, Fun) end,
        State
    ),
    {reply, Reply, NewState};

handle_call({list, Prefix}, _From, State) ->
    {Reply, NewState} = execute_operation(
        fun() -> bobsled:list(State#state.db_handle, Prefix) end,
        State
    ),
    {reply, Reply, NewState};

handle_call({fold, Fun, InitAcc, Prefix}, _From, State) ->
    {Reply, NewState} = execute_operation(
        fun() -> bobsled:fold(State#state.db_handle, Fun, InitAcc, Prefix) end,
        State
    ),
    {reply, Reply, NewState};

handle_call(flush, _From, State) ->
    {Reply, NewState} = execute_operation(
        fun() -> bobsled:flush(State#state.db_handle) end,
        State
    ),
    {reply, Reply, NewState};

handle_call(size_on_disk, _From, State) ->
    {Reply, NewState} = execute_operation(
        fun() -> bobsled:size_on_disk(State#state.db_handle) end,
        State
    ),
    {reply, Reply, NewState};

handle_call(get_db_handle, _From, State) ->
    {reply, State#state.db_handle, State};

handle_call(get_stats, _From, State) ->
    {reply, State#state.stats, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

%% @doc Handle asynchronous casts.
%% @private
-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @doc Handle info messages.
%% @private
-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info(_Info, State) ->
    {noreply, State}.

%% @doc Clean up when terminating.
%% @private
-spec terminate(term(), state()) -> ok.
terminate(Reason, State) ->
    error_logger:info_msg("Bobsled server terminating: ~p~n", [Reason]),
    
    case State#state.db_handle of
        undefined ->
            ok;
        DbHandle ->
            case bobsled:close(DbHandle) of
                ok ->
                    error_logger:info_msg("Bobsled database closed successfully~n");
                {error, CloseReason} ->
                    error_logger:error_msg("Failed to close bobsled database: ~p~n", 
                                         [CloseReason])
            end
    end,
    ok.

%% @doc Handle code changes.
%% @private
-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% @doc Execute a database operation and update statistics.
%% @private
-spec execute_operation(fun(() -> term()), state()) -> {term(), state()}.
execute_operation(Operation, State) ->
    try
        Result = Operation(),
        NewStats = update_stats(Result, State#state.stats),
        NewState = State#state{stats = NewStats},
        {Result, NewState}
    catch
        Class:Reason:Stacktrace ->
            error_logger:error_msg("Database operation failed: ~p:~p~n~p~n", 
                                 [Class, Reason, Stacktrace]),
            ErrorStats = increment_error_count(State#state.stats),
            NewState = State#state{stats = ErrorStats},
            {{error, {operation_failed, {Class, Reason}}}, NewState}
    end.

%% @doc Update operation statistics.
%% @private
-spec update_stats(term(), map()) -> map().
update_stats({error, _}, Stats) ->
    Stats#{
        operations := maps:get(operations, Stats) + 1,
        errors := maps:get(errors, Stats) + 1
    };
update_stats(_, Stats) ->
    Stats#{
        operations := maps:get(operations, Stats) + 1
    }.

%% @doc Increment error count in statistics.
%% @private
-spec increment_error_count(map()) -> map().
increment_error_count(Stats) ->
    Stats#{
        operations := maps:get(operations, Stats) + 1,
        errors := maps:get(errors, Stats) + 1
    }.