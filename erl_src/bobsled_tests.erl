%%%-------------------------------------------------------------------
%%% @doc Bobsled Unit Tests
%%%
%%% Basic unit tests for the bobsled NIF wrapper. These tests demonstrate
%%% proper testing patterns and can be extended with property-based
%%% testing using PropEr.
%%%
%%% @author Bobsled Team
%%% @copyright 2025 Bobsled Project
%%% @end
%%%-------------------------------------------------------------------
-module(bobsled_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Test Setup and Cleanup
%%%===================================================================

setup_test_db() ->
    TestDir = <<"/tmp/bobsled_test_", (integer_to_binary(erlang:system_time()))/binary>>,
    {ok, Db} = bobsled:open(TestDir, [{mode, fast}]),
    {Db, TestDir}.

cleanup_test_db({Db, TestDir}) ->
    bobsled:close(Db),
    % Clean up test directory
    os:cmd("rm -rf " ++ binary_to_list(TestDir)).

%%%===================================================================
%%% Basic Operation Tests
%%%===================================================================

basic_operations_test() ->
    {Db, _} = DbSetup = setup_test_db(),
    
    try
        % Test put and get
        ?assertEqual(ok, bobsled:put(Db, <<"key1">>, <<"value1">>)),
        ?assertEqual({ok, <<"value1">>}, bobsled:get(Db, <<"key1">>)),
        
        % Test get non-existent key
        ?assertEqual(not_found, bobsled:get(Db, <<"nonexistent">>)),
        
        % Test delete
        ?assertEqual(ok, bobsled:delete(Db, <<"key1">>)),
        ?assertEqual(not_found, bobsled:get(Db, <<"key1">>)),
        
        % Test delete non-existent key (should not fail)
        ?assertEqual(ok, bobsled:delete(Db, <<"nonexistent">>))
    after
        cleanup_test_db(DbSetup)
    end.

compare_and_swap_test() ->
    {Db, _} = DbSetup = setup_test_db(),
    
    try
        % Test CAS on non-existent key
        ?assertEqual(ok, bobsled:compare_and_swap(Db, <<"counter">>, 
                                                 not_found, <<"1">>)),
        ?assertEqual({ok, <<"1">>}, bobsled:get(Db, <<"counter">>)),
        
        % Test successful CAS
        ?assertEqual(ok, bobsled:compare_and_swap(Db, <<"counter">>, 
                                                 <<"1">>, <<"2">>)),
        ?assertEqual({ok, <<"2">>}, bobsled:get(Db, <<"counter">>)),
        
        % Test failed CAS
        ?assertEqual({error, cas_failed}, 
                    bobsled:compare_and_swap(Db, <<"counter">>, 
                                           <<"1">>, <<"3">>)),
        ?assertEqual({ok, <<"2">>}, bobsled:get(Db, <<"counter">>))
    after
        cleanup_test_db(DbSetup)
    end.

batch_operations_test() ->
    {Db, _} = DbSetup = setup_test_db(),
    
    try
        % Test batch put
        KVPairs = [
            {<<"batch:1">>, <<"value1">>},
            {<<"batch:2">>, <<"value2">>},
            {<<"batch:3">>, <<"value3">>}
        ],
        
        ?assertEqual(ok, bobsled:batch_put(Db, KVPairs)),
        
        % Verify all keys were inserted
        ?assertEqual({ok, <<"value1">>}, bobsled:get(Db, <<"batch:1">>)),
        ?assertEqual({ok, <<"value2">>}, bobsled:get(Db, <<"batch:2">>)),
        ?assertEqual({ok, <<"value3">>}, bobsled:get(Db, <<"batch:3">>))
    after
        cleanup_test_db(DbSetup)
    end.

iteration_test() ->
    {Db, _} = DbSetup = setup_test_db(),
    
    try
        % Insert test data
        TestData = [
            {<<"prefix:a">>, <<"value_a">>},
            {<<"prefix:b">>, <<"value_b">>},
            {<<"prefix:c">>, <<"value_c">>},
            {<<"other:x">>, <<"value_x">>}
        ],
        
        ?assertEqual(ok, bobsled:batch_put(Db, TestData)),
        
        % Test list with prefix
        {ok, Keys} = bobsled:list(Db, <<"prefix:">>),
        ?assertEqual(3, length(Keys)),
        ?assert(lists:member(<<"prefix:a">>, Keys)),
        ?assert(lists:member(<<"prefix:b">>, Keys)),
        ?assert(lists:member(<<"prefix:c">>, Keys)),
        
        % Test fold operation
        CountFun = fun(_Key, _Value, Count) -> Count + 1 end,
        {ok, Count} = bobsled:fold(Db, CountFun, 0, <<"prefix:">>),
        ?assertEqual(3, Count),
        
        % Test fold with accumulator
        CollectFun = fun(_Key, Value, Acc) -> [Value | Acc] end,
        {ok, Values} = bobsled:fold(Db, CollectFun, [], <<"prefix:">>),
        ?assertEqual(3, length(Values))
    after
        cleanup_test_db(DbSetup)
    end.

utility_operations_test() ->
    {Db, _} = DbSetup = setup_test_db(),
    
    try
        % Test flush
        ?assertEqual(ok, bobsled:put(Db, <<"test">>, <<"data">>)),
        ?assertEqual(ok, bobsled:flush(Db)),
        
        % Test size_on_disk
        {ok, Size} = bobsled:size_on_disk(Db),
        ?assert(is_integer(Size)),
        ?assert(Size >= 0)
    after
        cleanup_test_db(DbSetup)
    end.

%%%===================================================================
%%% Error Handling Tests
%%%===================================================================

invalid_arguments_test() ->
    {Db, _} = DbSetup = setup_test_db(),
    
    try
        % Test invalid arguments
        ?assertMatch({error, {badarg, _}}, bobsled:put(invalid_handle, <<"key">>, <<"value">>)),
        ?assertMatch({error, {badarg, _}}, bobsled:put(Db, not_binary, <<"value">>)),
        ?assertMatch({error, {badarg, _}}, bobsled:put(Db, <<"key">>, not_binary)),
        
        ?assertMatch({error, {badarg, _}}, bobsled:get(invalid_handle, <<"key">>)),
        ?assertMatch({error, {badarg, _}}, bobsled:get(Db, not_binary)),
        
        ?assertMatch({error, {badarg, _}}, bobsled:delete(invalid_handle, <<"key">>)),
        ?assertMatch({error, {badarg, _}}, bobsled:delete(Db, not_binary))
    after
        cleanup_test_db(DbSetup)
    end.

invalid_open_options_test() ->
    TestDir = <<"/tmp/bobsled_invalid_test">>,
    
    % Test invalid options
    ?assertMatch({error, {invalid_option, _}}, 
                bobsled:open(TestDir, [{invalid_option, value}])),
    ?assertMatch({error, {invalid_option, _}}, 
                bobsled:open(TestDir, [{cache_capacity, -1}])),
    ?assertMatch({error, {invalid_option, _}}, 
                bobsled:open(TestDir, [{mode, invalid_mode}])),
    ?assertMatch({error, {invalid_option, _}}, 
                bobsled:open(TestDir, [{compression_factor, 25}])),
    ?assertMatch({error, {invalid_option, _}}, 
                bobsled:open(TestDir, [{flush_every_ms, 0}])).

invalid_batch_data_test() ->
    {Db, _} = DbSetup = setup_test_db(),
    
    try
        % Test invalid batch data
        ?assertMatch({error, {invalid_kv_pair, _}}, 
                    bobsled:batch_put(Db, [{not_binary, <<"value">>}])),
        ?assertMatch({error, {invalid_kv_pair, _}}, 
                    bobsled:batch_put(Db, [{<<"key">>, not_binary}])),
        ?assertMatch({error, {invalid_kv_pair, _}}, 
                    bobsled:batch_put(Db, [invalid_tuple]))
    after
        cleanup_test_db(DbSetup)
    end.

closed_database_test() ->
    {Db, TestDir} = setup_test_db(),
    
    % Close the database
    ?assertEqual(ok, bobsled:close(Db)),
    
    % Clean up directory manually since we closed early
    os:cmd("rm -rf " ++ binary_to_list(TestDir)).

%%%===================================================================
%%% Performance/Stress Tests
%%%===================================================================

concurrent_access_test() ->
    {Db, _} = DbSetup = setup_test_db(),
    
    try
        % Test concurrent put operations
        NumProcesses = 10,
        NumOpsPerProcess = 100,
        
        Parent = self(),
        
        Workers = [spawn_link(fun() ->
            lists:foreach(fun(N) ->
                Key = iolist_to_binary([<<"worker_">>, 
                                       integer_to_binary(WorkerId), 
                                       <<"_">>, 
                                       integer_to_binary(N)]),
                Value = iolist_to_binary([<<"value_">>, integer_to_binary(N)]),
                ok = bobsled:put(Db, Key, Value)
            end, lists:seq(1, NumOpsPerProcess)),
            Parent ! {done, WorkerId}
        end) || WorkerId <- lists:seq(1, NumProcesses)],
        
        % Wait for all workers to complete
        lists:foreach(fun(WorkerId) ->
            receive
                {done, WorkerId} -> ok
            after 5000 ->
                error({timeout, WorkerId})
            end
        end, lists:seq(1, NumProcesses)),
        
        % Verify some data was written
        {ok, Size} = bobsled:size_on_disk(Db),
        ?assert(Size > 0),
        
        % Clean up worker processes
        lists:foreach(fun(Pid) ->
            case is_process_alive(Pid) of
                true -> exit(Pid, kill);
                false -> ok
            end
        end, Workers)
    after
        cleanup_test_db(DbSetup)
    end.

large_data_test() ->
    {Db, _} = DbSetup = setup_test_db(),
    
    try
        % Test with larger data
        LargeValue = binary:copy(<<"x">>, 10000),  % 10KB value
        Key = <<"large_data">>,
        
        ?assertEqual(ok, bobsled:put(Db, Key, LargeValue)),
        ?assertEqual({ok, LargeValue}, bobsled:get(Db, Key)),
        
        % Test batch with large data
        LargeBatch = [{iolist_to_binary([<<"large_">>, integer_to_binary(N)]), 
                      binary:copy(<<"y">>, 1000)} || N <- lists:seq(1, 50)],
        
        ?assertEqual(ok, bobsled:batch_put(Db, LargeBatch)),
        
        % Flush to ensure data is written to disk
        ?assertEqual(ok, bobsled:flush(Db)),
        
        % Verify size increased
        {ok, Size} = bobsled:size_on_disk(Db),
        ?assert(Size > 50000)  % Should be at least 50KB
    after
        cleanup_test_db(DbSetup)
    end.

-endif. % TEST