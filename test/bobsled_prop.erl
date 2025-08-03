%%%-------------------------------------------------------------------
%%% @doc Property-based tests for bobsled NIF using PropEr
%%%
%%% This module contains stateful and stateless property-based tests
%%% that verify bobsled behavior under various conditions:
%%% - Database operation invariants
%%% - Concurrent operation properties
%%% - Transaction isolation properties
%%% - Data consistency guarantees
%%%
%%% @author Bobsled Test Team
%%% @copyright 2025 Bobsled Project
%%% @end
%%%-------------------------------------------------------------------
-module(bobsled_prop).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(DB_PATH_PREFIX, "/tmp/bobsled_prop_test_").
-define(MAX_KEY_SIZE, 256).
-define(MAX_VALUE_SIZE, 1024).
-define(MAX_OPERATIONS, 100).

%%%===================================================================
%%% PropEr Generators
%%%===================================================================

%% @doc Generate a valid database key
key() ->
    ?SIZED(Size, binary(max(1, Size div 10))).

%% @doc Generate a valid database value
value() ->
    ?SIZED(Size, binary(Size div 5)).

%% @doc Generate a non-empty key
non_empty_key() ->
    ?SUCHTHAT(K, key(), byte_size(K) > 0).

%% @doc Generate a database path
db_path() ->
    ?LET(Suffix, integer(1, 1000000),
         list_to_binary(?DB_PATH_PREFIX ++ integer_to_list(Suffix))).

%% @doc Generate database open options
open_options() ->
    list(oneof([
        {cache_capacity, choose(1024, 10485760)},  % 1KB to 10MB
        {mode, oneof([fast, safe])},
        {compression_factor, choose(0, 22)},
        {flush_every_ms, choose(100, 5000)}
    ])).

%% @doc Generate a database operation
db_operation(Keys) ->
    frequency([
        {30, {put, oneof(Keys), value()}},
        {25, {get, oneof(Keys)}},
        {10, {delete, oneof(Keys)}},
        {5, {compare_and_swap, oneof(Keys), oneof([not_found, value()]), value()}},
        {3, {list, oneof(Keys)}},
        {2, {flush}}
    ]).

%% @doc Generate a sequence of database operations
operation_sequence() ->
    ?LET(Keys, non_empty(list(non_empty_key())),
         {Keys, list(db_operation(Keys))}).

%% @doc Generate key-value pairs for batch operations
kv_pairs() ->
    list({non_empty_key(), value()}).

%% @doc Generate transaction operations
transaction_operations(Keys) ->
    non_empty(list(oneof([
        {put, oneof(Keys), value()},
        {get, oneof(Keys)},
        {delete, oneof(Keys)}
    ]))).

%%%===================================================================
%%% Stateless Properties
%%%===================================================================

%% @doc Property: Put followed by get returns the same value
prop_put_get_roundtrip() ->
    ?FORALL({DbPath, Key, Value}, {db_path(), non_empty_key(), value()},
        begin
            {ok, Db} = bobsled:open(DbPath, []),
            ok = bobsled:put(Db, Key, Value),
            Result = bobsled:get(Db, Key),
            ok = bobsled:close(Db),
            cleanup_db_path(DbPath),
            Result =:= {ok, Value}
        end).

%% @doc Property: Get non-existent key returns not_found
prop_get_nonexistent() ->
    ?FORALL({DbPath, Key}, {db_path(), non_empty_key()},
        begin
            {ok, Db} = bobsled:open(DbPath, []),
            Result = bobsled:get(Db, Key),
            ok = bobsled:close(Db),
            cleanup_db_path(DbPath),
            Result =:= not_found
        end).

%% @doc Property: Delete removes key-value pair
prop_delete_removes_key() ->
    ?FORALL({DbPath, Key, Value}, {db_path(), non_empty_key(), value()},
        begin
            {ok, Db} = bobsled:open(DbPath, []),
            ok = bobsled:put(Db, Key, Value),
            ok = bobsled:delete(Db, Key),
            Result = bobsled:get(Db, Key),
            ok = bobsled:close(Db),
            cleanup_db_path(DbPath),
            Result =:= not_found
        end).

%% @doc Property: Compare-and-swap with correct old value succeeds
prop_cas_correct_old_value() ->
    ?FORALL({DbPath, Key, OldValue, NewValue}, 
            {db_path(), non_empty_key(), value(), value()},
        begin
            {ok, Db} = bobsled:open(DbPath, []),
            ok = bobsled:put(Db, Key, OldValue),
            CasResult = bobsled:compare_and_swap(Db, Key, OldValue, NewValue),
            GetResult = bobsled:get(Db, Key),
            ok = bobsled:close(Db),
            cleanup_db_path(DbPath),
            CasResult =:= ok andalso GetResult =:= {ok, NewValue}
        end).

%% @doc Property: Compare-and-swap with incorrect old value fails
prop_cas_incorrect_old_value() ->
    ?FORALL({DbPath, Key, ActualValue, WrongValue, NewValue}, 
            {db_path(), non_empty_key(), value(), value(), value()},
        ?IMPLIES(ActualValue =/= WrongValue,
            begin
                {ok, Db} = bobsled:open(DbPath, []),
                ok = bobsled:put(Db, Key, ActualValue),
                CasResult = bobsled:compare_and_swap(Db, Key, WrongValue, NewValue),
                GetResult = bobsled:get(Db, Key),
                ok = bobsled:close(Db),
                cleanup_db_path(DbPath),
                CasResult =:= {error, cas_failed} andalso 
                GetResult =:= {ok, ActualValue}
            end)).

%% @doc Property: Batch put is equivalent to individual puts
prop_batch_put_equivalence() ->
    ?FORALL({DbPath, KVPairs}, {db_path(), kv_pairs()},
        ?IMPLIES(length(KVPairs) > 0,
            begin
                % Batch put
                {ok, Db1} = bobsled:open(DbPath ++ <<"_batch">>, []),
                ok = bobsled:batch_put(Db1, KVPairs),
                BatchResults = [bobsled:get(Db1, K) || {K, _V} <- KVPairs],
                ok = bobsled:close(Db1),
                
                % Individual puts
                {ok, Db2} = bobsled:open(DbPath ++ <<"_individual">>, []),
                lists:foreach(fun({K, V}) ->
                    ok = bobsled:put(Db2, K, V)
                end, KVPairs),
                IndividualResults = [bobsled:get(Db2, K) || {K, _V} <- KVPairs],
                ok = bobsled:close(Db2),
                
                cleanup_db_path(DbPath ++ <<"_batch">>),
                cleanup_db_path(DbPath ++ <<"_individual">>),
                
                BatchResults =:= IndividualResults
            end)).

%% @doc Property: List operation returns keys with specified prefix
prop_list_prefix_matching() ->
    ?FORALL({DbPath, Prefix, Keys}, 
            {db_path(), non_empty_key(), non_empty(list(non_empty_key()))},
        begin
            {ok, Db} = bobsled:open(DbPath, []),
            
            % Create keys with and without prefix
            PrefixedKeys = [<<Prefix/binary, K/binary>> || K <- Keys],
            NonPrefixedKeys = [<<"different_", K/binary>> || K <- Keys],
            AllKeys = PrefixedKeys ++ NonPrefixedKeys,
            
            % Put all keys
            lists:foreach(fun(K) ->
                ok = bobsled:put(Db, K, <<"value">>)
            end, AllKeys),
            
            % List with prefix
            {ok, ListedKeys} = bobsled:list(Db, Prefix),
            
            ok = bobsled:close(Db),
            cleanup_db_path(DbPath),
            
            % All listed keys should have the prefix
            lists:all(fun(K) ->
                binary:longest_common_prefix([K, Prefix]) =:= byte_size(Prefix)
            end, ListedKeys) andalso
            % All prefixed keys should be in the list
            lists:all(fun(K) ->
                lists:member(K, ListedKeys)
            end, PrefixedKeys)
        end).

%% @doc Property: Fold operation processes all prefix-matching entries
prop_fold_processes_all_entries() ->
    ?FORALL({DbPath, Prefix, KVPairs}, 
            {db_path(), non_empty_key(), kv_pairs()},
        ?IMPLIES(length(KVPairs) > 0,
            begin
                {ok, Db} = bobsled:open(DbPath, []),
                
                % Create prefixed keys
                PrefixedKVs = [{<<Prefix/binary, K/binary>>, V} || {K, V} <- KVPairs],
                
                % Put all data
                lists:foreach(fun({K, V}) ->
                    ok = bobsled:put(Db, K, V)
                end, PrefixedKVs),
                
                % Count entries with fold
                CountFun = fun(_Key, _Value, Acc) -> Acc + 1 end,
                {ok, Count} = bobsled:fold(Db, CountFun, 0, Prefix),
                
                ok = bobsled:close(Db),
                cleanup_db_path(DbPath),
                
                Count =:= length(PrefixedKVs)
            end)).

%% @doc Property: Database persists data across close/open cycles
prop_persistence_across_sessions() ->
    ?FORALL({DbPath, KVPairs}, {db_path(), kv_pairs()},
        ?IMPLIES(length(KVPairs) > 0,
            begin
                % First session - write data
                {ok, Db1} = bobsled:open(DbPath, []),
                lists:foreach(fun({K, V}) ->
                    ok = bobsled:put(Db, K, V)
                end, KVPairs),
                ok = bobsled:close(Db1),
                
                % Second session - read data
                {ok, Db2} = bobsled:open(DbPath, []),
                Results = [{K, bobsled:get(Db2, K)} || {K, _V} <- KVPairs],
                ok = bobsled:close(Db2),
                
                cleanup_db_path(DbPath),
                
                % All data should be present
                lists:all(fun({{_K, ExpectedV}, {_K, {ok, ActualV}}}) ->
                    ExpectedV =:= ActualV
                end, lists:zip(KVPairs, Results))
            end)).

%% @doc Property: Invalid arguments are properly rejected
prop_invalid_arguments_rejected() ->
    ?FORALL(DbPath, db_path(),
        begin
            {ok, Db} = bobsled:open(DbPath, []),
            
            % Test various invalid argument combinations
            Results = [
                bobsled:put(not_a_ref, <<"key">>, <<"value">>),
                bobsled:put(Db, not_binary, <<"value">>),
                bobsled:put(Db, <<"key">>, not_binary),
                bobsled:get(not_a_ref, <<"key">>),
                bobsled:get(Db, not_binary),
                bobsled:delete(not_a_ref, <<"key">>),
                bobsled:delete(Db, not_binary),
                bobsled:compare_and_swap(Db, not_binary, <<"old">>, <<"new">>),
                bobsled:compare_and_swap(Db, <<"key">>, <<"old">>, not_binary)
            ],
            
            ok = bobsled:close(Db),
            cleanup_db_path(DbPath),
            
            % All should return badarg errors
            lists:all(fun(Result) ->
                case Result of
                    {error, {badarg, _}} -> true;
                    _ -> false
                end
            end, Results)
        end).

%%%===================================================================
%%% Stateful Properties
%%%===================================================================

%% @doc State for stateful property testing
-record(state, {
    db_handle :: reference() | undefined,
    db_path :: binary(),
    data :: #{binary() => binary()}  % Model of database state
}).

%% @doc Initial state for stateful testing
initial_state() ->
    #state{
        db_handle = undefined,
        db_path = undefined,
        data = #{}
    }.

%% @doc Command: Open database
command(#state{db_handle = undefined}) ->
    {call, ?MODULE, open_db, [db_path(), open_options()]};
command(#state{db_handle = Db}) when Db =/= undefined ->
    oneof([
        {call, ?MODULE, put_key, [non_empty_key(), value()]},
        {call, ?MODULE, get_key, [non_empty_key()]},
        {call, ?MODULE, delete_key, [non_empty_key()]},
        {call, ?MODULE, compare_and_swap_key, [non_empty_key(), value(), value()]},
        {call, ?MODULE, list_keys, [non_empty_key()]},
        {call, ?MODULE, batch_put_keys, [kv_pairs()]},
        {call, ?MODULE, flush_db, []},
        {call, ?MODULE, close_db, []}
    ]).

%% @doc Precondition for commands
precondition(#state{db_handle = undefined}, {call, ?MODULE, open_db, _}) ->
    true;
precondition(#state{db_handle = Db}, {call, ?MODULE, open_db, _}) when Db =/= undefined ->
    false;
precondition(#state{db_handle = undefined}, _) ->
    false;
precondition(#state{db_handle = Db}, {call, ?MODULE, close_db, []}) when Db =/= undefined ->
    true;
precondition(#state{db_handle = Db}, _) when Db =/= undefined ->
    true.

%% @doc Dynamic precondition (not used in this case)
dynamic_precondition(_State, _Call) ->
    true.

%% @doc Next state transition
next_state(State, Result, {call, ?MODULE, open_db, [DbPath, _Options]}) ->
    State#state{db_handle = {call, erlang, element, [2, Result]}, db_path = DbPath};

next_state(State = #state{data = Data}, _Result, {call, ?MODULE, put_key, [Key, Value]}) ->
    State#state{data = Data#{Key => Value}};

next_state(State = #state{data = Data}, _Result, {call, ?MODULE, delete_key, [Key]}) ->
    State#state{data = maps:remove(Key, Data)};

next_state(State = #state{data = Data}, _Result, 
           {call, ?MODULE, compare_and_swap_key, [Key, _OldValue, NewValue]}) ->
    % Note: This is simplified - real CAS might fail
    State#state{data = Data#{Key => NewValue}};

next_state(State = #state{data = Data}, _Result, {call, ?MODULE, batch_put_keys, [KVPairs]}) ->
    NewData = lists:foldl(fun({K, V}, Acc) -> Acc#{K => V} end, Data, KVPairs),
    State#state{data = NewData};

next_state(State, _Result, {call, ?MODULE, close_db, []}) ->
    State#state{db_handle = undefined};

next_state(State, _Result, _Call) ->
    State.

%% @doc Postcondition verification
postcondition(_State, {call, ?MODULE, open_db, _}, Result) ->
    case Result of
        {ok, DbHandle} when is_reference(DbHandle) -> true;
        _ -> false
    end;

postcondition(_State, {call, ?MODULE, put_key, _}, Result) ->
    Result =:= ok;

postcondition(#state{data = Data}, {call, ?MODULE, get_key, [Key]}, Result) ->
    Expected = case maps:get(Key, Data, not_found) of
        not_found -> not_found;
        Value -> {ok, Value}
    end,
    Result =:= Expected;

postcondition(_State, {call, ?MODULE, delete_key, _}, Result) ->
    Result =:= ok;

postcondition(_State, {call, ?MODULE, close_db, _}, Result) ->
    Result =:= ok;

postcondition(_State, _Call, _Result) ->
    true.

%% @doc State cleanup
cleanup(#state{db_handle = Db, db_path = DbPath}) ->
    if 
        Db =/= undefined -> 
            catch bobsled:close(Db);
        true -> 
            ok
    end,
    if 
        DbPath =/= undefined -> 
            cleanup_db_path(DbPath);
        true -> 
            ok
    end.

%% @doc Main stateful property
prop_stateful_operations() ->
    ?FORALL(Cmds, commands(?MODULE),
        begin
            {History, State, Result} = run_commands(?MODULE, Cmds),
            cleanup(State),
            ?WHENFAIL(
                io:format("History: ~p~nState: ~p~nResult: ~p~n", 
                         [History, State, Result]),
                aggregate(command_names(Cmds), Result =:= ok)
            )
        end).

%%%===================================================================
%%% Concurrent Properties
%%%===================================================================

%% @doc Property: Concurrent reads return consistent results
prop_concurrent_reads_consistent() ->
    ?FORALL({DbPath, Key, Value}, {db_path(), non_empty_key(), value()},
        begin
            {ok, Db} = bobsled:open(DbPath, []),
            ok = bobsled:put(Db, Key, Value),
            
            % Spawn multiple readers
            Parent = self(),
            NumReaders = 10,
            
            lists:foreach(fun(_) ->
                spawn(fun() ->
                    Result = bobsled:get(Db, Key),
                    Parent ! {read_result, Result}
                end)
            end, lists:seq(1, NumReaders)),
            
            % Collect results
            Results = collect_read_results(NumReaders, []),
            
            ok = bobsled:close(Db),
            cleanup_db_path(DbPath),
            
            % All reads should return the same value
            lists:all(fun(R) -> R =:= {ok, Value} end, Results)
        end).

%% @doc Property: Concurrent writes are all successful
prop_concurrent_writes_succeed() ->
    ?FORALL(DbPath, db_path(),
        begin
            {ok, Db} = bobsled:open(DbPath, []),
            
            % Spawn multiple writers with unique keys
            Parent = self(),
            NumWriters = 10,
            
            lists:foreach(fun(N) ->
                spawn(fun() ->
                    Key = <<"concurrent_key_", (integer_to_binary(N))/binary>>,
                    Value = <<"concurrent_value_", (integer_to_binary(N))/binary>>,
                    Result = bobsled:put(Db, Key, Value),
                    Parent ! {write_result, N, Result}
                end)
            end, lists:seq(1, NumWriters)),
            
            % Collect results
            WriteResults = collect_write_results(NumWriters, []),
            
            % Verify all data was written
            ReadResults = lists:map(fun(N) ->
                Key = <<"concurrent_key_", (integer_to_binary(N))/binary>>,
                ExpectedValue = <<"concurrent_value_", (integer_to_binary(N))/binary>>,
                {N, bobsled:get(Db, Key), ExpectedValue}
            end, lists:seq(1, NumWriters)),
            
            ok = bobsled:close(Db),
            cleanup_db_path(DbPath),
            
            % All writes should succeed and data should be readable
            WriteSuccess = lists:all(fun({_N, Result}) -> Result =:= ok end, WriteResults),
            ReadSuccess = lists:all(fun({_N, {ok, ActualValue}, ExpectedValue}) ->
                ActualValue =:= ExpectedValue
            end, ReadResults),
            
            WriteSuccess andalso ReadSuccess
        end).

%% @doc Property: Transactions maintain isolation
prop_transaction_isolation() ->
    ?FORALL({DbPath, Key, InitialValue, Value1, Value2}, 
            {db_path(), non_empty_key(), value(), value(), value()},
        ?IMPLIES(Value1 =/= Value2,
            begin
                {ok, Db} = bobsled:open(DbPath, []),
                ok = bobsled:put(Db, Key, InitialValue),
                
                Parent = self(),
                
                % Transaction 1: Read, modify, write
                Txn1 = spawn(fun() ->
                    TxnFun = fun(TxnHandle) ->
                        timer:sleep(10), % Add delay to increase chance of conflict
                        case bobsled:get(TxnHandle, Key) of
                            {ok, _} ->
                                ok = bobsled:put(TxnHandle, Key, Value1),
                                {ok, txn1_success};
                            Error ->
                                Error
                        end
                    end,
                    Result = bobsled:transaction(Db, TxnFun),
                    Parent ! {txn1_result, Result}
                end),
                
                % Transaction 2: Read, modify, write
                Txn2 = spawn(fun() ->
                    TxnFun = fun(TxnHandle) ->
                        timer:sleep(5), % Different timing
                        case bobsled:get(TxnHandle, Key) of
                            {ok, _} ->
                                ok = bobsled:put(TxnHandle, Key, Value2),
                                {ok, txn2_success};
                            Error ->
                                Error
                        end
                    end,
                    Result = bobsled:transaction(Db, TxnFun),
                    Parent ! {txn2_result, Result}
                end),
                
                % Collect results
                Txn1Result = receive {txn1_result, R1} -> R1 after 5000 -> timeout end,
                Txn2Result = receive {txn2_result, R2} -> R2 after 5000 -> timeout end,
                
                % Check final value
                {ok, FinalValue} = bobsled:get(Db, Key),
                
                ok = bobsled:close(Db),
                cleanup_db_path(DbPath),
                
                % At least one transaction should succeed
                % Final value should be from one of the successful transactions
                (Txn1Result =:= {ok, txn1_success} orelse Txn2Result =:= {ok, txn2_success}) andalso
                (FinalValue =:= Value1 orelse FinalValue =:= Value2)
            end)).

%%%===================================================================
%%% Command implementations for stateful testing
%%%===================================================================

%% @private
open_db(DbPath, Options) ->
    bobsled:open(DbPath, Options).

%% @private
put_key(Key, Value) ->
    Db = get(test_db_handle),
    bobsled:put(Db, Key, Value).

%% @private
get_key(Key) ->
    Db = get(test_db_handle),
    bobsled:get(Db, Key).

%% @private
delete_key(Key) ->
    Db = get(test_db_handle),
    bobsled:delete(Db, Key).

%% @private
compare_and_swap_key(Key, OldValue, NewValue) ->
    Db = get(test_db_handle),
    bobsled:compare_and_swap(Db, Key, OldValue, NewValue).

%% @private
list_keys(Prefix) ->
    Db = get(test_db_handle),
    bobsled:list(Db, Prefix).

%% @private
batch_put_keys(KVPairs) ->
    Db = get(test_db_handle),
    bobsled:batch_put(Db, KVPairs).

%% @private
flush_db() ->
    Db = get(test_db_handle),
    bobsled:flush(Db).

%% @private
close_db() ->
    Db = get(test_db_handle),
    bobsled:close(Db).

%%%===================================================================
%%% Helper Functions
%%%===================================================================

%% @private
cleanup_db_path(DbPath) when is_binary(DbPath) ->
    cleanup_db_path(binary_to_list(DbPath));
cleanup_db_path(DbPath) when is_list(DbPath) ->
    case filelib:is_dir(DbPath) of
        true -> os:cmd("rm -rf " ++ DbPath);
        false -> file:delete(DbPath)
    end.

%% @private
collect_read_results(0, Acc) -> Acc;
collect_read_results(N, Acc) ->
    receive
        {read_result, Result} ->
            collect_read_results(N - 1, [Result | Acc])
    after 5000 ->
        error({timeout_collecting_read_results, N})
    end.

%% @private
collect_write_results(0, Acc) -> Acc;
collect_write_results(N, Acc) ->
    receive
        {write_result, WriterId, Result} ->
            collect_write_results(N - 1, [{WriterId, Result} | Acc])
    after 5000 ->
        error({timeout_collecting_write_results, N})
    end.

%%%===================================================================
%%% Test Runners
%%%===================================================================

%% @doc Run all stateless properties
test_stateless() ->
    Properties = [
        prop_put_get_roundtrip,
        prop_get_nonexistent,
        prop_delete_removes_key,
        prop_cas_correct_old_value,
        prop_cas_incorrect_old_value,
        prop_batch_put_equivalence,
        prop_list_prefix_matching,
        prop_fold_processes_all_entries,
        prop_persistence_across_sessions,
        prop_invalid_arguments_rejected
    ],
    
    Results = lists:map(fun(Prop) ->
        io:format("Testing ~p... ", [Prop]),
        Result = proper:quickcheck(Prop(), [{to_file, user}, {numtests, 100}]),
        io:format("~p~n", [Result]),
        {Prop, Result}
    end, Properties),
    
    FailedTests = [Prop || {Prop, false} <- Results],
    case FailedTests of
        [] -> 
            io:format("All stateless properties passed!~n"),
            ok;
        _ -> 
            io:format("Failed properties: ~p~n", [FailedTests]),
            error
    end.

%% @doc Run stateful properties
test_stateful() ->
    io:format("Testing stateful operations...~n"),
    Result = proper:quickcheck(prop_stateful_operations(), 
                              [{to_file, user}, {numtests, 50}]),
    case Result of
        true -> 
            io:format("Stateful property passed!~n"),
            ok;
        false -> 
            io:format("Stateful property failed!~n"),
            error
    end.

%% @doc Run concurrent properties
test_concurrent() ->
    Properties = [
        prop_concurrent_reads_consistent,
        prop_concurrent_writes_succeed,
        prop_transaction_isolation
    ],
    
    Results = lists:map(fun(Prop) ->
        io:format("Testing ~p... ", [Prop]),
        Result = proper:quickcheck(Prop(), [{to_file, user}, {numtests, 20}]),
        io:format("~p~n", [Result]),
        {Prop, Result}
    end, Properties),
    
    FailedTests = [Prop || {Prop, false} <- Results],
    case FailedTests of
        [] -> 
            io:format("All concurrent properties passed!~n"),
            ok;
        _ -> 
            io:format("Failed concurrent properties: ~p~n", [FailedTests]),
            error
    end.

%% @doc Run all property-based tests
test_all() ->
    io:format("Running all property-based tests...~n~n"),
    
    StatelessResult = test_stateless(),
    StatefulResult = test_stateful(),
    ConcurrentResult = test_concurrent(),
    
    case {StatelessResult, StatefulResult, ConcurrentResult} of
        {ok, ok, ok} ->
            io:format("~nAll property-based tests passed!~n"),
            ok;
        _ ->
            io:format("~nSome property-based tests failed!~n"),
            error
    end.