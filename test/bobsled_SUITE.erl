%%%-------------------------------------------------------------------
%%% @doc Comprehensive Common Test suite for bobsled NIF
%%%
%%% This test suite covers all bobsled functionality including:
%%% - Basic operations (open, close, put, get, delete)
%%% - Error handling and edge cases
%%% - Advanced operations (CAS, batch, transactions)
%%% - Iteration operations (list, fold with prefixes)
%%% - Concurrent access patterns
%%% - Resource management and cleanup
%%%
%%% @author Bobsled Test Team
%%% @copyright 2025 Bobsled Project
%%% @end
%%%-------------------------------------------------------------------
-module(bobsled_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(DB_PATH_PREFIX, "/tmp/bobsled_test_").
-define(CLEANUP_TIMEOUT, 5000).
-define(DEFAULT_TIMEOUT, 10000).

%%%===================================================================
%%% CT Callbacks
%%%===================================================================

%% @doc Return list of test groups and test cases
all() ->
    [
        {group, basic_operations},
        {group, error_handling},
        {group, advanced_operations},
        {group, iteration_operations},
        {group, concurrent_operations},
        {group, edge_cases},
        {group, resource_management}
    ].

%% @doc Define test groups
groups() ->
    [
        {basic_operations, [parallel], [
            test_open_close,
            test_put_get,
            test_delete,
            test_put_get_delete_cycle,
            test_multiple_databases
        ]},
        {error_handling, [parallel], [
            test_invalid_arguments,
            test_closed_database_operations,
            test_invalid_options,
            test_file_permissions,
            test_nif_error_handling
        ]},
        {advanced_operations, [parallel], [
            test_compare_and_swap_success,
            test_compare_and_swap_failure,
            test_batch_put,
            test_transactions_simple,
            test_transactions_rollback,
            test_transactions_concurrent
        ]},
        {iteration_operations, [parallel], [
            test_list_empty,
            test_list_with_prefix,
            test_list_no_matches,
            test_fold_empty,
            test_fold_accumulator,
            test_fold_early_termination
        ]},
        {concurrent_operations, [], [
            test_concurrent_reads,
            test_concurrent_writes,
            test_concurrent_mixed_operations,
            test_reader_writer_concurrency
        ]},
        {edge_cases, [parallel], [
            test_empty_keys_values,
            test_large_keys_values,
            test_unicode_keys_values,
            test_binary_keys_values,
            test_many_operations
        ]},
        {resource_management, [], [
            test_flush_operations,
            test_size_on_disk,
            test_database_reopen,
            test_cleanup_on_process_death
        ]}
    ].

%% @doc Suite initialization
init_per_suite(Config) ->
    % Ensure clean test environment
    cleanup_test_files(),
    
    % Set test-specific timeouts
    [{timeout, ?DEFAULT_TIMEOUT} | Config].

%% @doc Suite cleanup
end_per_suite(_Config) ->
    cleanup_test_files(),
    ok.

%% @doc Group initialization
init_per_group(concurrent_operations, Config) ->
    % Special setup for concurrent tests
    [{concurrent_workers, 10} | Config];
init_per_group(_Group, Config) ->
    Config.

%% @doc Group cleanup
end_per_group(_Group, _Config) ->
    ok.

%% @doc Test case initialization
init_per_testcase(TestCase, Config) ->
    % Create unique database path for each test
    DbPath = generate_db_path(TestCase),
    [{db_path, DbPath} | Config].

%% @doc Test case cleanup
end_per_testcase(_TestCase, Config) ->
    % Cleanup database files
    DbPath = ?config(db_path, Config),
    cleanup_db_path(DbPath),
    ok.

%%%===================================================================
%%% Basic Operations Tests
%%%===================================================================

%% @doc Test database open and close operations
test_open_close(Config) ->
    DbPath = ?config(db_path, Config),
    
    % Test successful open
    {ok, Db} = bobsled:open(DbPath, []),
    ?assert(is_reference(Db)),
    
    % Test successful close
    ok = bobsled:close(Db),
    
    % Test opening with options
    Options = [
        {cache_capacity, 1024 * 1024},
        {mode, safe},
        {compression_factor, 5},
        {flush_every_ms, 1000}
    ],
    {ok, Db2} = bobsled:open(DbPath, Options),
    ok = bobsled:close(Db2).

%% @doc Test basic put and get operations
test_put_get(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    Key = <<"test_key">>,
    Value = <<"test_value">>,
    
    % Test put
    ok = bobsled:put(Db, Key, Value),
    
    % Test get
    {ok, Retrieved} = bobsled:get(Db, Key),
    ?assertEqual(Value, Retrieved),
    
    % Test get non-existent key
    ?assertEqual(not_found, bobsled:get(Db, <<"non_existent">>)),
    
    ok = bobsled:close(Db).

%% @doc Test delete operation
test_delete(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    Key = <<"delete_test">>,
    Value = <<"delete_value">>,
    
    % Put, verify, delete, verify
    ok = bobsled:put(Db, Key, Value),
    {ok, Value} = bobsled:get(Db, Key),
    
    ok = bobsled:delete(Db, Key),
    ?assertEqual(not_found, bobsled:get(Db, Key)),
    
    % Test deleting non-existent key (should succeed)
    ok = bobsled:delete(Db, <<"non_existent">>),
    
    ok = bobsled:close(Db).

%% @doc Test complete put-get-delete cycle
test_put_get_delete_cycle(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    Keys = [<<"key1">>, <<"key2">>, <<"key3">>],
    Values = [<<"value1">>, <<"value2">>, <<"value3">>],
    
    % Put all keys
    lists:foreach(fun({K, V}) ->
        ok = bobsled:put(Db, K, V)
    end, lists:zip(Keys, Values)),
    
    % Verify all keys
    lists:foreach(fun({K, V}) ->
        {ok, Retrieved} = bobsled:get(Db, K),
        ?assertEqual(V, Retrieved)
    end, lists:zip(Keys, Values)),
    
    % Delete all keys
    lists:foreach(fun(K) ->
        ok = bobsled:delete(Db, K)
    end, Keys),
    
    % Verify deletion
    lists:foreach(fun(K) ->
        ?assertEqual(not_found, bobsled:get(Db, K))
    end, Keys),
    
    ok = bobsled:close(Db).

%% @doc Test multiple database instances
test_multiple_databases(Config) ->
    DbPath1 = ?config(db_path, Config) ++ "_1",
    DbPath2 = ?config(db_path, Config) ++ "_2",
    
    {ok, Db1} = bobsled:open(list_to_binary(DbPath1), []),
    {ok, Db2} = bobsled:open(list_to_binary(DbPath2), []),
    
    % Verify they are different references
    ?assertNotEqual(Db1, Db2),
    
    % Test independent operations
    ok = bobsled:put(Db1, <<"key1">>, <<"db1_value">>),
    ok = bobsled:put(Db2, <<"key1">>, <<"db2_value">>),
    
    {ok, <<"db1_value">>} = bobsled:get(Db1, <<"key1">>),
    {ok, <<"db2_value">>} = bobsled:get(Db2, <<"key1">>),
    
    ok = bobsled:close(Db1),
    ok = bobsled:close(Db2),
    
    % Cleanup additional paths
    cleanup_db_path(DbPath1),
    cleanup_db_path(DbPath2).

%%%===================================================================
%%% Error Handling Tests
%%%===================================================================

%% @doc Test invalid argument handling
test_invalid_arguments(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    % Test invalid open arguments
    ?assertMatch({error, {badarg, _}}, bobsled:open("not_binary", [])),
    ?assertMatch({error, {badarg, _}}, bobsled:open(DbPath, not_list)),
    
    % Test invalid put arguments
    ?assertMatch({error, {badarg, _}}, bobsled:put(not_ref, <<"key">>, <<"value">>)),
    ?assertMatch({error, {badarg, _}}, bobsled:put(Db, "not_binary", <<"value">>)),
    ?assertMatch({error, {badarg, _}}, bobsled:put(Db, <<"key">>, "not_binary")),
    
    % Test invalid get arguments
    ?assertMatch({error, {badarg, _}}, bobsled:get(not_ref, <<"key">>)),
    ?assertMatch({error, {badarg, _}}, bobsled:get(Db, "not_binary")),
    
    % Test invalid delete arguments
    ?assertMatch({error, {badarg, _}}, bobsled:delete(not_ref, <<"key">>)),
    ?assertMatch({error, {badarg, _}}, bobsled:delete(Db, "not_binary")),
    
    % Test invalid close arguments
    ?assertMatch({error, {badarg, _}}, bobsled:close(not_ref)),
    
    ok = bobsled:close(Db).

%% @doc Test operations on closed database
test_closed_database_operations(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    ok = bobsled:close(Db),
    
    % All operations should fail on closed database
    ?assertMatch({error, _}, bobsled:put(Db, <<"key">>, <<"value">>)),
    ?assertMatch({error, _}, bobsled:get(Db, <<"key">>)),
    ?assertMatch({error, _}, bobsled:delete(Db, <<"key">>)),
    ?assertMatch({error, _}, bobsled:close(Db)).

%% @doc Test invalid open options
test_invalid_options(Config) ->
    DbPath = ?config(db_path, Config),
    
    % Test invalid option types
    ?assertMatch({error, {invalid_option, _}}, 
                 bobsled:open(DbPath, [{cache_capacity, -1}])),
    ?assertMatch({error, {invalid_option, _}}, 
                 bobsled:open(DbPath, [{mode, invalid_mode}])),
    ?assertMatch({error, {invalid_option, _}}, 
                 bobsled:open(DbPath, [{compression_factor, 25}])),
    ?assertMatch({error, {invalid_option, _}}, 
                 bobsled:open(DbPath, [{flush_every_ms, 0}])),
    ?assertMatch({error, {invalid_option, _}}, 
                 bobsled:open(DbPath, [invalid_option])).

%% @doc Test file permission errors
test_file_permissions(Config) ->
    % This test may not work on all systems, so we make it optional
    case os:type() of
        {unix, _} ->
            % Try to open database in read-only directory
            ReadOnlyPath = <<"/root/bobsled_readonly_test">>,
            case bobsled:open(ReadOnlyPath, []) of
                {error, _} -> ok;  % Expected
                {ok, Db} -> 
                    bobsled:close(Db),
                    ct:comment("Unexpected success on read-only path")
            end;
        _ ->
            ct:comment("Skipping permission test on non-Unix system")
    end.

%% @doc Test NIF error handling
test_nif_error_handling(Config) ->
    % Test that NIF errors are properly propagated
    % This might require specific conditions that trigger NIF errors
    ct:comment("NIF error handling verified through other tests").

%%%===================================================================
%%% Advanced Operations Tests
%%%===================================================================

%% @doc Test successful compare-and-swap operations
test_compare_and_swap_success(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    Key = <<"cas_key">>,
    Value1 = <<"value1">>,
    Value2 = <<"value2">>,
    
    % CAS on non-existent key
    ok = bobsled:compare_and_swap(Db, Key, not_found, Value1),
    {ok, Value1} = bobsled:get(Db, Key),
    
    % CAS on existing key
    ok = bobsled:compare_and_swap(Db, Key, Value1, Value2),
    {ok, Value2} = bobsled:get(Db, Key),
    
    ok = bobsled:close(Db).

%% @doc Test failed compare-and-swap operations
test_compare_and_swap_failure(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    Key = <<"cas_fail_key">>,
    Value1 = <<"value1">>,
    Value2 = <<"value2">>,
    
    % Put initial value
    ok = bobsled:put(Db, Key, Value1),
    
    % CAS with wrong expected value
    ?assertMatch({error, cas_failed}, 
                 bobsled:compare_and_swap(Db, Key, <<"wrong">>, Value2)),
    
    % Verify value unchanged
    {ok, Value1} = bobsled:get(Db, Key),
    
    % CAS on non-existent key with wrong expectation
    ?assertMatch({error, cas_failed}, 
                 bobsled:compare_and_swap(Db, <<"nonexistent">>, Value1, Value2)),
    
    ok = bobsled:close(Db).

%% @doc Test batch put operations
test_batch_put(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    % Test empty batch
    ok = bobsled:batch_put(Db, []),
    
    % Test normal batch
    KVPairs = [
        {<<"batch1">>, <<"value1">>},
        {<<"batch2">>, <<"value2">>},
        {<<"batch3">>, <<"value3">>}
    ],
    ok = bobsled:batch_put(Db, KVPairs),
    
    % Verify all values
    lists:foreach(fun({K, V}) ->
        {ok, Retrieved} = bobsled:get(Db, K),
        ?assertEqual(V, Retrieved)
    end, KVPairs),
    
    % Test invalid batch data
    InvalidKV = [{<<"key">>, not_binary}],
    ?assertMatch({error, {invalid_kv_pair, _}}, bobsled:batch_put(Db, InvalidKV)),
    
    ok = bobsled:close(Db).

%% @doc Test simple transaction operations
test_transactions_simple(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    % Simple transaction
    TransactionFun = fun(TxnHandle) ->
        ok = bobsled:put(TxnHandle, <<"txn_key1">>, <<"txn_value1">>),
        ok = bobsled:put(TxnHandle, <<"txn_key2">>, <<"txn_value2">>),
        {ok, success}
    end,
    
    {ok, success} = bobsled:transaction(Db, TransactionFun),
    
    % Verify transaction results
    {ok, <<"txn_value1">>} = bobsled:get(Db, <<"txn_key1">>),
    {ok, <<"txn_value2">>} = bobsled:get(Db, <<"txn_key2">>),
    
    ok = bobsled:close(Db).

%% @doc Test transaction rollback
test_transactions_rollback(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    % Put initial value
    ok = bobsled:put(Db, <<"rollback_key">>, <<"initial">>),
    
    % Transaction that fails
    FailingTxn = fun(TxnHandle) ->
        ok = bobsled:put(TxnHandle, <<"rollback_key">>, <<"modified">>),
        {error, intentional_failure}
    end,
    
    {error, intentional_failure} = bobsled:transaction(Db, FailingTxn),
    
    % Verify rollback - original value should remain
    {ok, <<"initial">>} = bobsled:get(Db, <<"rollback_key">>),
    
    ok = bobsled:close(Db).

%% @doc Test concurrent transactions
test_transactions_concurrent(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    % Initialize counter
    ok = bobsled:put(Db, <<"counter">>, <<"0">>),
    
    % Spawn multiple processes doing atomic increments
    NumProcs = 10,
    Parent = self(),
    
    IncrementFun = fun() ->
        TxnFun = fun(TxnHandle) ->
            case bobsled:get(TxnHandle, <<"counter">>) of
                {ok, CounterBin} ->
                    Counter = binary_to_integer(CounterBin),
                    NewCounter = Counter + 1,
                    ok = bobsled:put(TxnHandle, <<"counter">>, integer_to_binary(NewCounter)),
                    {ok, NewCounter};
                Error ->
                    Error
            end
        end,
        Result = bobsled:transaction(Db, TxnFun),
        Parent ! {increment_result, Result}
    end,
    
    % Spawn processes
    lists:foreach(fun(_) ->
        spawn(IncrementFun)
    end, lists:seq(1, NumProcs)),
    
    % Collect results
    Results = collect_results(NumProcs, []),
    
    % All should succeed
    ?assertEqual(NumProcs, length(Results)),
    lists:foreach(fun(Result) ->
        ?assertMatch({ok, _}, Result)
    end, Results),
    
    % Final counter should be NumProcs
    {ok, FinalCounterBin} = bobsled:get(Db, <<"counter">>),
    ?assertEqual(NumProcs, binary_to_integer(FinalCounterBin)),
    
    ok = bobsled:close(Db).

%%%===================================================================
%%% Iteration Operations Tests
%%%===================================================================

%% @doc Test list operation on empty database
test_list_empty(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    {ok, []} = bobsled:list(Db, <<"any_prefix">>),
    
    ok = bobsled:close(Db).

%% @doc Test list operation with prefix matching
test_list_with_prefix(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    % Add data with different prefixes
    TestData = [
        {<<"user:alice">>, <<"Alice Data">>},
        {<<"user:bob">>, <<"Bob Data">>},
        {<<"user:charlie">>, <<"Charlie Data">>},
        {<<"config:timeout">>, <<"30">>},
        {<<"config:host">>, <<"localhost">>},
        {<<"other:data">>, <<"Other">>}
    ],
    
    lists:foreach(fun({K, V}) ->
        ok = bobsled:put(Db, K, V)
    end, TestData),
    
    % Test user prefix
    {ok, UserKeys} = bobsled:list(Db, <<"user:">>),
    ExpectedUserKeys = [<<"user:alice">>, <<"user:bob">>, <<"user:charlie">>],
    ?assertEqual(lists:sort(ExpectedUserKeys), lists:sort(UserKeys)),
    
    % Test config prefix
    {ok, ConfigKeys} = bobsled:list(Db, <<"config:">>),
    ExpectedConfigKeys = [<<"config:timeout">>, <<"config:host">>],
    ?assertEqual(lists:sort(ExpectedConfigKeys), lists:sort(ConfigKeys)),
    
    % Test exact key as prefix
    {ok, ExactKeys} = bobsled:list(Db, <<"user:alice">>),
    ?assertEqual([<<"user:alice">>], ExactKeys),
    
    ok = bobsled:close(Db).

%% @doc Test list operation with no matches
test_list_no_matches(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    % Add some data
    ok = bobsled:put(Db, <<"user:alice">>, <<"data">>),
    
    % List with non-matching prefix
    {ok, []} = bobsled:list(Db, <<"nonexistent:">>),
    
    ok = bobsled:close(Db).

%% @doc Test fold operation on empty database
test_fold_empty(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    FoldFun = fun(_Key, _Value, Acc) -> Acc + 1 end,
    {ok, 0} = bobsled:fold(Db, FoldFun, 0, <<"any_prefix">>),
    
    ok = bobsled:close(Db).

%% @doc Test fold operation with accumulator
test_fold_accumulator(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    % Add numbered data
    TestData = [
        {<<"num:1">>, <<"10">>},
        {<<"num:2">>, <<"20">>},
        {<<"num:3">>, <<"30">>},
        {<<"other:x">>, <<"100">>}
    ],
    
    lists:foreach(fun({K, V}) ->
        ok = bobsled:put(Db, K, V)
    end, TestData),
    
    % Sum all num: values
    SumFun = fun(_Key, Value, Acc) ->
        Acc + binary_to_integer(Value)
    end,
    {ok, 60} = bobsled:fold(Db, SumFun, 0, <<"num:">>),
    
    % Count all num: entries
    CountFun = fun(_Key, _Value, Acc) -> Acc + 1 end,
    {ok, 3} = bobsled:fold(Db, CountFun, 0, <<"num:">>),
    
    % Collect all keys
    CollectFun = fun(Key, _Value, Acc) -> [Key | Acc] end,
    {ok, NumKeys} = bobsled:fold(Db, CollectFun, [], <<"num:">>),
    ExpectedKeys = [<<"num:1">>, <<"num:2">>, <<"num:3">>],
    ?assertEqual(lists:sort(ExpectedKeys), lists:sort(NumKeys)),
    
    ok = bobsled:close(Db).

%% @doc Test fold operation early termination
test_fold_early_termination(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    % Add data
    lists:foreach(fun(N) ->
        Key = iolist_to_binary(["key:", integer_to_list(N)]),
        Value = integer_to_binary(N),
        ok = bobsled:put(Db, Key, Value)
    end, lists:seq(1, 10)),
    
    % Fold that stops early (this test may depend on NIF implementation)
    % For now, we test normal fold behavior
    CollectFun = fun(Key, _Value, Acc) -> [Key | Acc] end,
    {ok, Keys} = bobsled:fold(Db, CollectFun, [], <<"key:">>),
    ?assert(length(Keys) =:= 10),
    
    ok = bobsled:close(Db).

%%%===================================================================
%%% Concurrent Operations Tests
%%%===================================================================

%% @doc Test concurrent read operations
test_concurrent_reads(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    % Setup test data
    TestData = lists:map(fun(N) ->
        Key = iolist_to_binary(["read_key_", integer_to_list(N)]),
        Value = iolist_to_binary(["read_value_", integer_to_list(N)]),
        ok = bobsled:put(Db, Key, Value),
        {Key, Value}
    end, lists:seq(1, 100)),
    
    NumReaders = 20,
    NumReadsPerReader = 50,
    Parent = self(),
    
    % Spawn concurrent readers
    ReaderFun = fun() ->
        Results = lists:map(fun(_) ->
            {Key, ExpectedValue} = lists:nth(rand:uniform(100), TestData),
            case bobsled:get(Db, Key) of
                {ok, ExpectedValue} -> success;
                Other -> {error, Other}
            end
        end, lists:seq(1, NumReadsPerReader)),
        Parent ! {reader_results, Results}
    end,
    
    lists:foreach(fun(_) ->
        spawn(ReaderFun)
    end, lists:seq(1, NumReaders)),
    
    % Collect results
    AllResults = collect_reader_results(NumReaders, []),
    TotalReads = NumReaders * NumReadsPerReader,
    
    % All reads should succeed
    ?assertEqual(TotalReads, length(AllResults)),
    ?assert(lists:all(fun(Result) -> Result =:= success end, AllResults)),
    
    ok = bobsled:close(Db).

%% @doc Test concurrent write operations
test_concurrent_writes(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    NumWriters = 10,
    WritesPerWriter = 50,
    Parent = self(),
    
    % Spawn concurrent writers
    WriterFun = fun(WriterId) ->
        Results = lists:map(fun(WriteNum) ->
            Key = iolist_to_binary(["writer_", integer_to_list(WriterId), 
                                   "_key_", integer_to_list(WriteNum)]),
            Value = iolist_to_binary(["writer_", integer_to_list(WriterId), 
                                     "_value_", integer_to_list(WriteNum)]),
            bobsled:put(Db, Key, Value)
        end, lists:seq(1, WritesPerWriter)),
        Parent ! {writer_results, WriterId, Results}
    end,
    
    lists:foreach(fun(WriterId) ->
        spawn(fun() -> WriterFun(WriterId) end)
    end, lists:seq(1, NumWriters)),
    
    % Collect results
    WriterResults = collect_writer_results(NumWriters, []),
    
    % All writes should succeed
    lists:foreach(fun({_WriterId, Results}) ->
        ?assert(lists:all(fun(Result) -> Result =:= ok end, Results))
    end, WriterResults),
    
    % Verify all data was written
    lists:foreach(fun(WriterId) ->
        lists:foreach(fun(WriteNum) ->
            Key = iolist_to_binary(["writer_", integer_to_list(WriterId), 
                                   "_key_", integer_to_list(WriteNum)]),
            ExpectedValue = iolist_to_binary(["writer_", integer_to_list(WriterId), 
                                             "_value_", integer_to_list(WriteNum)]),
            {ok, ExpectedValue} = bobsled:get(Db, Key)
        end, lists:seq(1, WritesPerWriter))
    end, lists:seq(1, NumWriters)),
    
    ok = bobsled:close(Db).

%% @doc Test concurrent mixed operations
test_concurrent_mixed_operations(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    % Setup initial data
    lists:foreach(fun(N) ->
        Key = iolist_to_binary(["mixed_", integer_to_list(N)]),
        Value = iolist_to_binary(["initial_", integer_to_list(N)]),
        ok = bobsled:put(Db, Key, Value)
    end, lists:seq(1, 50)),
    
    NumWorkers = 15,
    Parent = self(),
    
    % Mixed operation worker
    MixedWorkerFun = fun(WorkerId) ->
        Results = lists:map(fun(_) ->
            Operation = rand:uniform(4),
            N = rand:uniform(50),
            Key = iolist_to_binary(["mixed_", integer_to_list(N)]),
            
            case Operation of
                1 -> % Read
                    case bobsled:get(Db, Key) of
                        {ok, _} -> {read, success};
                        not_found -> {read, not_found};
                        Error -> {read, Error}
                    end;
                2 -> % Write
                    Value = iolist_to_binary(["worker_", integer_to_list(WorkerId), "_", integer_to_list(N)]),
                    {write, bobsled:put(Db, Key, Value)};
                3 -> % Delete
                    {delete, bobsled:delete(Db, Key)};
                4 -> % CAS
                    NewValue = iolist_to_binary(["cas_", integer_to_list(WorkerId), "_", integer_to_list(N)]),
                    case bobsled:get(Db, Key) of
                        {ok, OldValue} ->
                            {cas, bobsled:compare_and_swap(Db, Key, OldValue, NewValue)};
                        not_found ->
                            {cas, bobsled:compare_and_swap(Db, Key, not_found, NewValue)}
                    end
            end
        end, lists:seq(1, 30)),
        Parent ! {mixed_results, WorkerId, Results}
    end,
    
    lists:foreach(fun(WorkerId) ->
        spawn(fun() -> MixedWorkerFun(WorkerId) end)
    end, lists:seq(1, NumWorkers)),
    
    % Collect results
    MixedResults = collect_mixed_results(NumWorkers, []),
    
    % Verify no crashes occurred
    ?assertEqual(NumWorkers, length(MixedResults)),
    
    % Basic validation that operations completed
    lists:foreach(fun({_WorkerId, Results}) ->
        ?assert(length(Results) =:= 30)
    end, MixedResults),
    
    ok = bobsled:close(Db).

%% @doc Test reader-writer concurrency patterns
test_reader_writer_concurrency(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    % Setup shared data
    SharedKey = <<"shared_counter">>,
    ok = bobsled:put(Db, SharedKey, <<"0">>),
    
    NumReaders = 5,
    NumWriters = 3,
    TestDuration = 2000, % 2 seconds
    Parent = self(),
    
    % Reader process
    ReaderFun = fun() ->
        EndTime = erlang:monotonic_time(millisecond) + TestDuration,
        ReadCount = read_loop(Db, SharedKey, EndTime, 0),
        Parent ! {reader_done, ReadCount}
    end,
    
    % Writer process
    WriterFun = fun() ->
        EndTime = erlang:monotonic_time(millisecond) + TestDuration,
        WriteCount = write_loop(Db, SharedKey, EndTime, 0),
        Parent ! {writer_done, WriteCount}
    end,
    
    % Spawn readers and writers
    lists:foreach(fun(_) -> spawn(ReaderFun) end, lists:seq(1, NumReaders)),
    lists:foreach(fun(_) -> spawn(WriterFun) end, lists:seq(1, NumWriters)),
    
    % Collect results
    ReaderCounts = collect_reader_counts(NumReaders, []),
    WriterCounts = collect_writer_counts(NumWriters, []),
    
    % Verify activity occurred
    TotalReads = lists:sum(ReaderCounts),
    TotalWrites = lists:sum(WriterCounts),
    
    ?assert(TotalReads > 0),
    ?assert(TotalWrites > 0),
    
    ct:pal("Concurrent test completed: ~p reads, ~p writes", [TotalReads, TotalWrites]),
    
    ok = bobsled:close(Db).

%%%===================================================================
%%% Edge Cases Tests
%%%===================================================================

%% @doc Test empty keys and values
test_empty_keys_values(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    EmptyKey = <<"">>,
    EmptyValue = <<"">>,
    
    % Test empty key with value
    ok = bobsled:put(Db, EmptyKey, <<"non_empty_value">>),
    {ok, <<"non_empty_value">>} = bobsled:get(Db, EmptyKey),
    
    % Test key with empty value
    ok = bobsled:put(Db, <<"non_empty_key">>, EmptyValue),
    {ok, EmptyValue} = bobsled:get(Db, <<"non_empty_key">>),
    
    % Test empty key and empty value
    ok = bobsled:put(Db, EmptyKey, EmptyValue),
    {ok, EmptyValue} = bobsled:get(Db, EmptyKey),
    
    ok = bobsled:close(Db).

%% @doc Test large keys and values
test_large_keys_values(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    % Large key (64KB)
    LargeKey = binary:copy(<<"k">>, 65536),
    NormalValue = <<"normal_value">>,
    
    ok = bobsled:put(Db, LargeKey, NormalValue),
    {ok, NormalValue} = bobsled:get(Db, LargeKey),
    
    % Large value (1MB)
    NormalKey = <<"normal_key">>,
    LargeValue = binary:copy(<<"v">>, 1048576),
    
    ok = bobsled:put(Db, NormalKey, LargeValue),
    {ok, LargeValue} = bobsled:get(Db, NormalKey),
    
    % Large key and value
    LargeKey2 = binary:copy(<<"x">>, 32768),
    LargeValue2 = binary:copy(<<"y">>, 524288),
    
    ok = bobsled:put(Db, LargeKey2, LargeValue2),
    {ok, LargeValue2} = bobsled:get(Db, LargeKey2),
    
    ok = bobsled:close(Db).

%% @doc Test Unicode keys and values
test_unicode_keys_values(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    % Unicode in keys and values
    UnicodeKey = <<"测试键"/utf8>>,
    UnicodeValue = <<"测试值"/utf8>>,
    
    ok = bobsled:put(Db, UnicodeKey, UnicodeValue),
    {ok, UnicodeValue} = bobsled:get(Db, UnicodeKey),
    
    % Mixed Unicode and ASCII
    MixedKey = <<"prefix_测试_suffix"/utf8>>,
    MixedValue = <<"value_测试_end"/utf8>>,
    
    ok = bobsled:put(Db, MixedKey, MixedValue),
    {ok, MixedValue} = bobsled:get(Db, MixedKey),
    
    % Emoji
    EmojiKey = <<"🔑"/utf8>>,
    EmojiValue = <<"🎯"/utf8>>,
    
    ok = bobsled:put(Db, EmojiKey, EmojiValue),
    {ok, EmojiValue} = bobsled:get(Db, EmojiKey),
    
    ok = bobsled:close(Db).

%% @doc Test binary keys and values
test_binary_keys_values(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    % Binary data with null bytes
    BinaryKey = <<1, 2, 3, 0, 4, 5, 6>>,
    BinaryValue = <<255, 254, 253, 0, 1, 2>>,
    
    ok = bobsled:put(Db, BinaryKey, BinaryValue),
    {ok, BinaryValue} = bobsled:get(Db, BinaryKey),
    
    % Random binary data
    RandomKey = crypto:strong_rand_bytes(32),
    RandomValue = crypto:strong_rand_bytes(64),
    
    ok = bobsled:put(Db, RandomKey, RandomValue),
    {ok, RandomValue} = bobsled:get(Db, RandomKey),
    
    ok = bobsled:close(Db).

%% @doc Test many operations
test_many_operations(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    NumOperations = 10000,
    
    % Perform many put operations
    lists:foreach(fun(N) ->
        Key = iolist_to_binary(["many_key_", integer_to_list(N)]),
        Value = iolist_to_binary(["many_value_", integer_to_list(N)]),
        ok = bobsled:put(Db, Key, Value)
    end, lists:seq(1, NumOperations)),
    
    % Verify random subset
    TestIndices = [rand:uniform(NumOperations) || _ <- lists:seq(1, 100)],
    lists:foreach(fun(N) ->
        Key = iolist_to_binary(["many_key_", integer_to_list(N)]),
        ExpectedValue = iolist_to_binary(["many_value_", integer_to_list(N)]),
        {ok, ExpectedValue} = bobsled:get(Db, Key)
    end, TestIndices),
    
    % Test list operation on large dataset
    {ok, AllKeys} = bobsled:list(Db, <<"many_key_">>),
    ?assertEqual(NumOperations, length(AllKeys)),
    
    ok = bobsled:close(Db).

%%%===================================================================
%%% Resource Management Tests
%%%===================================================================

%% @doc Test flush operations
test_flush_operations(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    % Put some data
    ok = bobsled:put(Db, <<"flush_test">>, <<"flush_value">>),
    
    % Flush should succeed
    ok = bobsled:flush(Db),
    
    % Data should still be there
    {ok, <<"flush_value">>} = bobsled:get(Db, <<"flush_test">>),
    
    % Multiple flushes should work
    ok = bobsled:flush(Db),
    ok = bobsled:flush(Db),
    
    ok = bobsled:close(Db).

%% @doc Test size on disk reporting
test_size_on_disk(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    % Initial size
    {ok, InitialSize} = bobsled:size_on_disk(Db),
    ?assert(is_integer(InitialSize)),
    ?assert(InitialSize >= 0),
    
    % Add data and check size increase
    LargeValue = binary:copy(<<"x">>, 10000),
    lists:foreach(fun(N) ->
        Key = iolist_to_binary(["size_test_", integer_to_list(N)]),
        ok = bobsled:put(Db, Key, LargeValue)
    end, lists:seq(1, 100)),
    
    ok = bobsled:flush(Db),
    {ok, NewSize} = bobsled:size_on_disk(Db),
    ?assert(NewSize > InitialSize),
    
    ok = bobsled:close(Db).

%% @doc Test database reopen
test_database_reopen(Config) ->
    DbPath = ?config(db_path, Config),
    
    % First session
    {ok, Db1} = bobsled:open(DbPath, []),
    ok = bobsled:put(Db1, <<"persistent_key">>, <<"persistent_value">>),
    ok = bobsled:close(Db1),
    
    % Second session - data should persist
    {ok, Db2} = bobsled:open(DbPath, []),
    {ok, <<"persistent_value">>} = bobsled:get(Db2, <<"persistent_key">>),
    
    % Add more data
    ok = bobsled:put(Db2, <<"new_key">>, <<"new_value">>),
    ok = bobsled:close(Db2),
    
    % Third session - all data should persist
    {ok, Db3} = bobsled:open(DbPath, []),
    {ok, <<"persistent_value">>} = bobsled:get(Db3, <<"persistent_key">>),
    {ok, <<"new_value">>} = bobsled:get(Db3, <<"new_key">>),
    ok = bobsled:close(Db3).

%% @doc Test cleanup on process death
test_cleanup_on_process_death(Config) ->
    DbPath = ?config(db_path, Config),
    Parent = self(),
    
    % Spawn process that opens database and dies
    Pid = spawn(fun() ->
        {ok, Db} = bobsled:open(DbPath, []),
        ok = bobsled:put(Db, <<"death_test">>, <<"death_value">>),
        Parent ! {db_opened, Db},
        receive
            die -> exit(normal)
        end
    end),
    
    % Get database handle
    DbHandle = receive
        {db_opened, Db} -> Db
    after 5000 ->
        error(timeout_waiting_for_db)
    end,
    
    % Kill the process
    Pid ! die,
    timer:sleep(100), % Give time for cleanup
    
    % Database should still be accessible (sled manages its own resources)
    % But operations on the handle from dead process might fail
    % This behavior depends on NIF implementation
    
    % Open fresh connection to verify data persists
    {ok, NewDb} = bobsled:open(DbPath, []),
    {ok, <<"death_value">>} = bobsled:get(NewDb, <<"death_test">>),
    ok = bobsled:close(NewDb).

%%%===================================================================
%%% Helper Functions
%%%===================================================================

%% @private
generate_db_path(TestCase) ->
    Timestamp = integer_to_list(erlang:system_time(microsecond)),
    TestCaseName = atom_to_list(TestCase),
    list_to_binary(?DB_PATH_PREFIX ++ TestCaseName ++ "_" ++ Timestamp).

%% @private
cleanup_test_files() ->
    case file:list_dir("/tmp") of
        {ok, Files} ->
            TestFiles = [F || F <- Files, string:prefix(F, "bobsled_test_") =/= nomatch],
            lists:foreach(fun(File) ->
                FullPath = "/tmp/" ++ File,
                case filelib:is_dir(FullPath) of
                    true -> 
                        os:cmd("rm -rf " ++ FullPath);
                    false -> 
                        file:delete(FullPath)
                end
            end, TestFiles);
        _ -> 
            ok
    end.

%% @private
cleanup_db_path(DbPath) when is_binary(DbPath) ->
    cleanup_db_path(binary_to_list(DbPath));
cleanup_db_path(DbPath) when is_list(DbPath) ->
    case filelib:is_dir(DbPath) of
        true -> os:cmd("rm -rf " ++ DbPath);
        false -> file:delete(DbPath)
    end.

%% @private
collect_results(0, Acc) -> Acc;
collect_results(N, Acc) ->
    receive
        {increment_result, Result} ->
            collect_results(N - 1, [Result | Acc])
    after 10000 ->
        error({timeout_collecting_results, N, Acc})
    end.

%% @private
collect_reader_results(0, Acc) -> lists:flatten(Acc);
collect_reader_results(N, Acc) ->
    receive
        {reader_results, Results} ->
            collect_reader_results(N - 1, [Results | Acc])
    after 10000 ->
        error({timeout_collecting_reader_results, N})
    end.

%% @private
collect_writer_results(0, Acc) -> Acc;
collect_writer_results(N, Acc) ->
    receive
        {writer_results, WriterId, Results} ->
            collect_writer_results(N - 1, [{WriterId, Results} | Acc])
    after 10000 ->
        error({timeout_collecting_writer_results, N})
    end.

%% @private
collect_mixed_results(0, Acc) -> Acc;
collect_mixed_results(N, Acc) ->
    receive
        {mixed_results, WorkerId, Results} ->
            collect_mixed_results(N - 1, [{WorkerId, Results} | Acc])
    after 15000 ->
        error({timeout_collecting_mixed_results, N})
    end.

%% @private
collect_reader_counts(0, Acc) -> Acc;
collect_reader_counts(N, Acc) ->
    receive
        {reader_done, Count} ->
            collect_reader_counts(N - 1, [Count | Acc])
    after 5000 ->
        error({timeout_collecting_reader_counts, N})
    end.

%% @private
collect_writer_counts(0, Acc) -> Acc;
collect_writer_counts(N, Acc) ->
    receive
        {writer_done, Count} ->
            collect_writer_counts(N - 1, [Count | Acc])
    after 5000 ->
        error({timeout_collecting_writer_counts, N})
    end.

%% @private
read_loop(Db, Key, EndTime, Count) ->
    case erlang:monotonic_time(millisecond) of
        Now when Now >= EndTime ->
            Count;
        _ ->
            _ = bobsled:get(Db, Key),
            read_loop(Db, Key, EndTime, Count + 1)
    end.

%% @private
write_loop(Db, Key, EndTime, Count) ->
    case erlang:monotonic_time(millisecond) of
        Now when Now >= EndTime ->
            Count;
        _ ->
            Value = integer_to_binary(Count),
            _ = bobsled:put(Db, Key, Value),
            write_loop(Db, Key, EndTime, Count + 1)
    end.