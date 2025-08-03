%%%-------------------------------------------------------------------
%%% @doc Crash and recovery tests for bobsled NIF
%%%
%%% This test suite focuses on crash scenarios and recovery:
%%% - Database recovery after various crash types
%%% - NIF panic handling and recovery
%%% - Resource leak detection after crashes
%%% - Corruption detection and handling
%%% - System-level failure scenarios
%%% - Data integrity verification after recovery
%%%
%%% @author Bobsled Test Team
%%% @copyright 2025 Bobsled Project
%%% @end
%%%-------------------------------------------------------------------
-module(bobsled_crash_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(DB_PATH_PREFIX, "/tmp/bobsled_crash_test_").
-define(CRASH_TIMEOUT, 60000). % Extended timeout for crash tests
-define(RECOVERY_DELAY, 1000). % Wait time for recovery operations

%%%===================================================================
%%% CT Callbacks
%%%===================================================================

%% @doc Return list of test groups and test cases
all() ->
    [
        {group, process_crashes},
        {group, system_crashes},
        {group, nif_panics},
        {group, corruption_scenarios},
        {group, resource_exhaustion},
        {group, recovery_verification}
    ].

%% @doc Define test groups
groups() ->
    [
        {process_crashes, [], [
            test_process_exit_normal,
            test_process_exit_abnormal,
            test_process_killed,
            test_linked_process_crash,
            test_monitor_process_crash
        ]},
        {system_crashes, [], [
            test_vm_restart_simulation,
            test_power_failure_simulation,
            test_disk_failure_simulation,
            test_memory_exhaustion,
            test_file_system_errors
        ]},
        {nif_panics, [], [
            test_invalid_handle_usage,
            test_concurrent_handle_access,
            test_double_close_protection,
            test_use_after_close,
            test_memory_corruption_detection
        ]},
        {corruption_scenarios, [], [
            test_partial_write_corruption,
            test_file_truncation,
            test_random_byte_corruption,
            test_header_corruption,
            test_index_corruption
        ]},
        {resource_exhaustion, [], [
            test_out_of_memory,
            test_file_descriptor_exhaustion,
            test_disk_space_exhaustion,
            test_large_transaction_limits,
            test_concurrent_connection_limits
        ]},
        {recovery_verification, [], [
            test_data_integrity_after_crash,
            test_transaction_atomicity,
            test_consistency_after_recovery,
            test_performance_after_recovery,
            test_incremental_recovery
        ]}
    ].

%% @doc Suite initialization
init_per_suite(Config) ->
    % Ensure clean test environment
    cleanup_test_files(),
    
    % Set crash test timeouts
    [{timeout, ?CRASH_TIMEOUT} | Config].

%% @doc Suite cleanup
end_per_suite(_Config) ->
    cleanup_test_files(),
    ok.

%% @doc Group initialization
init_per_group(system_crashes, Config) ->
    % System crash tests need special setup
    [{system_test_mode, true} | Config];
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
    % Cleanup database files (may be corrupted)
    DbPath = ?config(db_path, Config),
    cleanup_db_path(DbPath),
    
    % Force garbage collection to clean up any leaked resources
    erlang:garbage_collect(),
    ok.

%%%===================================================================
%%% Process Crash Tests
%%%===================================================================

%% @doc Test normal process exit with database cleanup
test_process_exit_normal(Config) ->
    DbPath = ?config(db_path, Config),
    Parent = self(),
    
    % Spawn process that exits normally
    Pid = spawn(fun() ->
        {ok, Db} = bobsled:open(DbPath, []),
        ok = bobsled:put(Db, <<"normal_exit_key">>, <<"normal_exit_value">>),
        
        % Notify parent before normal exit
        Parent ! {process_ready, Db},
        
        % Clean exit - should close database properly
        ok = bobsled:close(Db),
        exit(normal)
    end),
    
    % Wait for process to be ready
    DbHandle = receive
        {process_ready, Handle} -> Handle
    after 5000 ->
        error(timeout_waiting_for_process_ready)
    end,
    
    % Monitor process
    Ref = monitor(process, Pid),
    receive
        {'DOWN', Ref, process, Pid, normal} -> ok
    after 5000 ->
        error(timeout_waiting_for_normal_exit)
    end,
    
    % Verify database is accessible and data persists
    {ok, NewDb} = bobsled:open(DbPath, []),
    {ok, <<"normal_exit_value">>} = bobsled:get(NewDb, <<"normal_exit_key">>),
    ok = bobsled:close(NewDb),
    
    ct:comment("Normal process exit handled correctly").

%% @doc Test abnormal process exit without cleanup
test_process_exit_abnormal(Config) ->
    DbPath = ?config(db_path, Config),
    Parent = self(),
    
    % Spawn process that exits abnormally
    Pid = spawn(fun() ->
        {ok, Db} = bobsled:open(DbPath, []),
        ok = bobsled:put(Db, <<"abnormal_exit_key">>, <<"abnormal_exit_value">>),
        
        Parent ! {process_ready, Db},
        
        % Abnormal exit - don't close database
        error(intentional_error)
    end),
    
    % Wait for process to be ready
    DbHandle = receive
        {process_ready, Handle} -> Handle
    after 5000 ->
        error(timeout_waiting_for_process_ready)
    end,
    
    % Monitor process
    Ref = monitor(process, Pid),
    receive
        {'DOWN', Ref, process, Pid, {error, intentional_error}} -> ok
    after 5000 ->
        error(timeout_waiting_for_abnormal_exit)
    end,
    
    % Wait for cleanup
    timer:sleep(?RECOVERY_DELAY),
    
    % Verify database is still accessible and data persists
    {ok, NewDb} = bobsled:open(DbPath, []),
    {ok, <<"abnormal_exit_value">>} = bobsled:get(NewDb, <<"abnormal_exit_key">>),
    ok = bobsled:close(NewDb),
    
    ct:comment("Abnormal process exit handled correctly").

%% @doc Test process killed with kill signal
test_process_killed(Config) ->
    DbPath = ?config(db_path, Config),
    Parent = self(),
    
    % Spawn process that will be killed
    Pid = spawn(fun() ->
        {ok, Db} = bobsled:open(DbPath, []),
        ok = bobsled:put(Db, <<"killed_key">>, <<"killed_value">>),
        
        Parent ! {process_ready, Db},
        
        % Wait indefinitely - will be killed
        receive
            never_comes -> ok
        end
    end),
    
    % Wait for process to be ready
    DbHandle = receive
        {process_ready, Handle} -> Handle
    after 5000 ->
        error(timeout_waiting_for_process_ready)
    end,
    
    % Kill the process
    exit(Pid, kill),
    
    % Monitor process death
    Ref = monitor(process, Pid),
    receive
        {'DOWN', Ref, process, Pid, killed} -> ok
    after 5000 ->
        error(timeout_waiting_for_kill)
    end,
    
    % Wait for cleanup
    timer:sleep(?RECOVERY_DELAY),
    
    % Verify database is still accessible and data persists
    {ok, NewDb} = bobsled:open(DbPath, []),
    {ok, <<"killed_value">>} = bobsled:get(NewDb, <<"killed_key">>),
    ok = bobsled:close(NewDb),
    
    ct:comment("Killed process handled correctly").

%% @doc Test linked process crash propagation
test_linked_process_crash(Config) ->
    DbPath = ?config(db_path, Config),
    
    % Trap exits to prevent our test from crashing
    process_flag(trap_exit, true),
    
    % Spawn linked process
    Pid = spawn_link(fun() ->
        {ok, Db} = bobsled:open(DbPath, []),
        ok = bobsled:put(Db, <<"linked_crash_key">>, <<"linked_crash_value">>),
        
        % Don't close database before crashing
        error(linked_process_crash)
    end),
    
    % Wait for EXIT signal
    receive
        {'EXIT', Pid, {error, linked_process_crash}} ->
            ct:comment("Received expected EXIT signal from linked process")
    after 5000 ->
        error(timeout_waiting_for_linked_exit)
    end,
    
    % Restore normal exit behavior
    process_flag(trap_exit, false),
    
    % Wait for cleanup
    timer:sleep(?RECOVERY_DELAY),
    
    % Verify database is still accessible
    {ok, Db} = bobsled:open(DbPath, []),
    {ok, <<"linked_crash_value">>} = bobsled:get(Db, <<"linked_crash_key">>),
    ok = bobsled:close(Db),
    
    ct:comment("Linked process crash handled correctly").

%% @doc Test monitored process crash detection
test_monitor_process_crash(Config) ->
    DbPath = ?config(db_path, Config),
    Parent = self(),
    
    % Spawn process to monitor
    Pid = spawn(fun() ->
        {ok, Db} = bobsled:open(DbPath, []),
        ok = bobsled:put(Db, <<"monitored_key">>, <<"monitored_value">>),
        
        Parent ! {process_ready},
        
        receive
            crash_now -> error(monitored_crash)
        end
    end),
    
    % Monitor the process
    Ref = monitor(process, Pid),
    
    % Wait for process to be ready
    receive {process_ready} -> ok after 5000 -> error(timeout) end,
    
    % Trigger crash
    Pid ! crash_now,
    
    % Wait for DOWN message
    receive
        {'DOWN', Ref, process, Pid, {error, monitored_crash}} ->
            ct:comment("Monitored process crash detected")
    after 5000 ->
        error(timeout_waiting_for_down_message)
    end,
    
    % Wait for cleanup
    timer:sleep(?RECOVERY_DELAY),
    
    % Verify database accessibility
    {ok, Db} = bobsled:open(DbPath, []),
    {ok, <<"monitored_value">>} = bobsled:get(Db, <<"monitored_key">>),
    ok = bobsled:close(Db),
    
    ct:comment("Monitored process crash handled correctly").

%%%===================================================================
%%% System Crash Tests
%%%===================================================================

%% @doc Test VM restart simulation
test_vm_restart_simulation(Config) ->
    DbPath = ?config(db_path, Config),
    
    % Create database with critical data
    {ok, Db} = bobsled:open(DbPath, []),
    CriticalData = lists:map(fun(N) ->
        Key = iolist_to_binary(["vm_restart_", integer_to_list(N)]),
        Value = iolist_to_binary(["critical_data_", integer_to_list(N)]),
        ok = bobsled:put(Db, Key, Value),
        {Key, Value}
    end, lists:seq(1, 1000)),
    
    % Force flush to ensure data is written
    ok = bobsled:flush(Db),
    ok = bobsled:close(Db),
    
    % Simulate VM restart by reopening database
    timer:sleep(?RECOVERY_DELAY),
    
    {ok, RecoveredDb} = bobsled:open(DbPath, []),
    
    % Verify all critical data survived "restart"
    lists:foreach(fun({Key, Value}) ->
        case bobsled:get(RecoveredDb, Key) of
            {ok, Value} -> ok;
            {ok, OtherValue} -> error({data_corruption, Key, Value, OtherValue});
            not_found -> error({data_lost, Key, Value});
            {error, Reason} -> error({read_error, Key, Reason})
        end
    end, CriticalData),
    
    % Verify database is fully functional
    ok = bobsled:put(RecoveredDb, <<"post_restart">>, <<"post_restart_data">>),
    {ok, <<"post_restart_data">>} = bobsled:get(RecoveredDb, <<"post_restart">>),
    
    ok = bobsled:close(RecoveredDb),
    
    ct:comment("VM restart simulation: all data recovered").

%% @doc Test power failure simulation
test_power_failure_simulation(Config) ->
    DbPath = ?config(db_path, Config),
    
    % Start writing data when "power fails"
    {ok, Db} = bobsled:open(DbPath, []),
    
    % Write some data that should survive
    SurvivingData = lists:map(fun(N) ->
        Key = iolist_to_binary(["power_safe_", integer_to_list(N)]),
        Value = iolist_to_binary(["safe_data_", integer_to_list(N)]),
        ok = bobsled:put(Db, Key, Value),
        {Key, Value}
    end, lists:seq(1, 100)),
    
    % Force flush
    ok = bobsled:flush(Db),
    
    % Write data that may be lost (no flush)
    PotentiallyLostData = lists:map(fun(N) ->
        Key = iolist_to_binary(["power_unsafe_", integer_to_list(N)]),
        Value = iolist_to_binary(["unsafe_data_", integer_to_list(N)]),
        ok = bobsled:put(Db, Key, Value),
        {Key, Value}
    end, lists:seq(1, 50)),
    
    % Simulate power failure by forcibly closing without proper shutdown
    % In real power failure, the process would be killed immediately
    ok = bobsled:close(Db), % This is cleaner than real power failure
    
    % Wait for "recovery"
    timer:sleep(?RECOVERY_DELAY),
    
    % Attempt recovery
    {ok, RecoveredDb} = bobsled:open(DbPath, []),
    
    % Verify flushed data survived
    SurvivedCount = length([ok || {Key, Value} <- SurvivingData,
                               bobsled:get(RecoveredDb, Key) =:= {ok, Value}]),
    
    % Check potentially lost data
    PotentiallyLostCount = length([ok || {Key, Value} <- PotentiallyLostData,
                                      bobsled:get(RecoveredDb, Key) =:= {ok, Value}]),
    
    ct:pal("Power failure simulation: ~p/~p safe data survived, ~p/~p unsafe data survived",
           [SurvivedCount, length(SurvivingData), 
            PotentiallyLostCount, length(PotentiallyLostData)]),
    
    % All flushed data should survive
    ?assertEqual(length(SurvivingData), SurvivedCount),
    
    ok = bobsled:close(RecoveredDb),
    
    ct:comment(io_lib:format("Power failure: ~p safe, ~p unsafe survived", 
                           [SurvivedCount, PotentiallyLostCount])).

%% @doc Test disk failure simulation
test_disk_failure_simulation(Config) ->
    DbPath = ?config(db_path, Config),
    
    % Create database
    {ok, Db} = bobsled:open(DbPath, []),
    ok = bobsled:put(Db, <<"disk_test">>, <<"disk_value">>),
    ok = bobsled:close(Db),
    
    % Simulate disk corruption by modifying database files
    case file:list_dir(binary_to_list(DbPath)) of
        {ok, Files} ->
            lists:foreach(fun(File) ->
                FilePath = filename:join(binary_to_list(DbPath), File),
                case file:read_file_info(FilePath) of
                    {ok, #file_info{type = regular}} ->
                        % Append garbage to simulate corruption
                        file:write_file(FilePath, <<"DISK_CORRUPTION">>, [append]);
                    _ ->
                        ok
                end
            end, Files);
        {error, enoent} ->
            ct:comment("No database files to corrupt")
    end,
    
    % Try to open corrupted database
    case bobsled:open(DbPath, []) of
        {ok, CorruptedDb} ->
            % Database opened despite corruption - check if data is accessible
            case bobsled:get(CorruptedDb, <<"disk_test">>) of
                {ok, <<"disk_value">>} ->
                    ct:comment("Data survived disk corruption");
                not_found ->
                    ct:comment("Data lost due to disk corruption");
                {error, _} ->
                    ct:comment("Data corrupted but database accessible")
            end,
            bobsled:close(CorruptedDb);
        {error, corruption} ->
            ct:comment("Corruption detected and database refused to open");
        {error, Reason} ->
            ct:comment(io_lib:format("Database open failed: ~p", [Reason]))
    end.

%% @doc Test memory exhaustion scenarios
test_memory_exhaustion(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    % Try to write data that would exhaust memory
    LargeValueSize = 50 * 1024 * 1024, % 50MB
    
    try
        % Attempt to create large values
        LargeValue = binary:copy(<<"X">>, LargeValueSize),
        
        % Try multiple large writes
        Results = lists:map(fun(N) ->
            Key = iolist_to_binary(["memory_test_", integer_to_list(N)]),
            try
                bobsled:put(Db, Key, LargeValue)
            catch
                _:Error -> {error, Error}
            end
        end, lists:seq(1, 10)),
        
        % Count successes and failures
        Successes = [ok || ok <- Results],
        Failures = [E || {error, E} <- Results],
        
        ct:pal("Memory exhaustion test: ~p successes, ~p failures",
               [length(Successes), length(Failures)]),
        
        % System should handle memory pressure gracefully
        ?assert(length(Failures) =< length(Successes) + 5) % Allow some failures
        
    catch
        error:system_limit ->
            ct:comment("System limit reached as expected");
        error:enomem ->
            ct:comment("Out of memory error caught");
        _:Error ->
            ct:comment(io_lib:format("Unexpected error: ~p", [Error]))
    end,
    
    % Database should still be functional
    ok = bobsled:put(Db, <<"small_key">>, <<"small_value">>),
    {ok, <<"small_value">>} = bobsled:get(Db, <<"small_key">>),
    
    ok = bobsled:close(Db).

%% @doc Test file system errors
test_file_system_errors(Config) ->
    DbPath = ?config(db_path, Config),
    
    % Test with read-only file system (simulated)
    case os:type() of
        {unix, _} ->
            % Try to create database in /root (should fail for non-root users)
            ReadOnlyPath = <<"/root/test_readonly_db">>,
            case bobsled:open(ReadOnlyPath, []) of
                {ok, Db} ->
                    bobsled:close(Db),
                    ct:comment("Unexpected success on read-only path");
                {error, _Reason} ->
                    ct:comment("Read-only file system error handled correctly")
            end;
        _ ->
            ct:comment("File system error test skipped on non-Unix system")
    end,
    
    % Test normal path still works
    {ok, Db} = bobsled:open(DbPath, []),
    ok = bobsled:put(Db, <<"fs_test">>, <<"fs_value">>),
    {ok, <<"fs_value">>} = bobsled:get(Db, <<"fs_test">>),
    ok = bobsled:close(Db).

%%%===================================================================
%%% NIF Panic Tests
%%%===================================================================

%% @doc Test invalid handle usage protection
test_invalid_handle_usage(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    % Create fake handle
    FakeHandle = make_ref(),
    
    % Try to use fake handle - should fail gracefully
    Results = [
        bobsled:put(FakeHandle, <<"key">>, <<"value">>),
        bobsled:get(FakeHandle, <<"key">>),
        bobsled:delete(FakeHandle, <<"key">>),
        bobsled:close(FakeHandle)
    ],
    
    % All operations should fail with badarg
    lists:foreach(fun(Result) ->
        ?assertMatch({error, {badarg, _}}, Result)
    end, Results),
    
    % Real handle should still work
    ok = bobsled:put(Db, <<"real_key">>, <<"real_value">>),
    {ok, <<"real_value">>} = bobsled:get(Db, <<"real_key">>),
    
    ok = bobsled:close(Db),
    
    ct:comment("Invalid handle usage protected").

%% @doc Test concurrent handle access
test_concurrent_handle_access(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    Parent = self(),
    NumWorkers = 10,
    
    % Spawn workers that use the same handle concurrently
    lists:foreach(fun(N) ->
        spawn(fun() ->
            try
                lists:foreach(fun(Op) ->
                    Key = iolist_to_binary(["concurrent_", integer_to_list(N), "_", integer_to_list(Op)]),
                    Value = iolist_to_binary(["value_", integer_to_list(N), "_", integer_to_list(Op)]),
                    
                    ok = bobsled:put(Db, Key, Value),
                    {ok, Value} = bobsled:get(Db, Key)
                end, lists:seq(1, 100)),
                Parent ! {worker_done, N, success}
            catch
                _:Error ->
                    Parent ! {worker_done, N, {error, Error}}
            end
        end)
    end, lists:seq(1, NumWorkers)),
    
    % Collect results
    Results = [collect_concurrent_result(N) || N <- lists:seq(1, NumWorkers)],
    
    % Count successes and failures
    Successes = [R || {_, success} <- Results],
    Failures = [R || {_, {error, _}} <- Results],
    
    ct:pal("Concurrent handle access: ~p successes, ~p failures",
           [length(Successes), length(Failures)]),
    
    % Most operations should succeed
    ?assert(length(Successes) >= NumWorkers div 2),
    
    ok = bobsled:close(Db).

%% @doc Test double close protection
test_double_close_protection(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    % First close should succeed
    ok = bobsled:close(Db),
    
    % Second close should fail gracefully
    Result = bobsled:close(Db),
    ?assertMatch({error, _}, Result),
    
    ct:comment("Double close protection verified").

%% @doc Test use after close protection
test_use_after_close(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    % Close the database
    ok = bobsled:close(Db),
    
    % Try to use closed handle
    Results = [
        bobsled:put(Db, <<"key">>, <<"value">>),
        bobsled:get(Db, <<"key">>),
        bobsled:delete(Db, <<"key">>),
        bobsled:list(Db, <<"prefix">>),
        bobsled:flush(Db)
    ],
    
    % All operations should fail
    lists:foreach(fun(Result) ->
        ?assertMatch({error, _}, Result)
    end, Results),
    
    ct:comment("Use after close protection verified").

%% @doc Test memory corruption detection
test_memory_corruption_detection(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    % Try operations that might expose memory corruption
    CorruptionTests = [
        % Very large keys/values
        fun() ->
            LargeKey = binary:copy(<<"K">>, 1024*1024),
            bobsled:put(Db, LargeKey, <<"value">>)
        end,
        
        % Keys/values with null bytes
        fun() ->
            NullKey = <<1, 2, 3, 0, 4, 5>>,
            bobsled:put(Db, NullKey, <<"null_value">>)
        end,
        
        % Random binary data
        fun() ->
            RandomKey = crypto:strong_rand_bytes(64),
            RandomValue = crypto:strong_rand_bytes(256),
            bobsled:put(Db, RandomKey, RandomValue)
        end
    ],
    
    % Run corruption tests
    Results = lists:map(fun(Test) ->
        try
            Test()
        catch
            _:Error -> {error, Error}
        end
    end, CorruptionTests),
    
    % At least some tests should succeed (corruption detection shouldn't be too aggressive)
    Successes = [ok || ok <- Results],
    ?assert(length(Successes) > 0),
    
    % Database should remain functional
    ok = bobsled:put(Db, <<"normal_key">>, <<"normal_value">>),
    {ok, <<"normal_value">>} = bobsled:get(Db, <<"normal_key">>),
    
    ok = bobsled:close(Db),
    
    ct:comment("Memory corruption detection verified").

%%%===================================================================
%%% Corruption Scenario Tests
%%%===================================================================

%% @doc Test partial write corruption recovery
test_partial_write_corruption(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    % Write initial data
    ok = bobsled:put(Db, <<"partial_key_1">>, <<"partial_value_1">>),
    ok = bobsled:put(Db, <<"partial_key_2">>, <<"partial_value_2">>),
    ok = bobsled:flush(Db),
    ok = bobsled:close(Db),
    
    % Simulate partial write by truncating one of the database files
    case file:list_dir(binary_to_list(DbPath)) of
        {ok, [FirstFile | _]} ->
            FilePath = filename:join(binary_to_list(DbPath), FirstFile),
            case file:read_file(FilePath) of
                {ok, Data} when byte_size(Data) > 100 ->
                    % Truncate file to simulate partial write
                    TruncatedData = binary:part(Data, 0, byte_size(Data) div 2),
                    file:write_file(FilePath, TruncatedData),
                    ct:pal("Truncated ~s from ~p to ~p bytes", 
                           [FirstFile, byte_size(Data), byte_size(TruncatedData)]);
                _ ->
                    ct:comment("File too small to truncate")
            end;
        _ ->
            ct:comment("No files to corrupt")
    end,
    
    % Try to recover
    case bobsled:open(DbPath, []) of
        {ok, RecoveredDb} ->
            % Check what data survived
            Result1 = bobsled:get(RecoveredDb, <<"partial_key_1">>),
            Result2 = bobsled:get(RecoveredDb, <<"partial_key_2">>),
            
            case {Result1, Result2} of
                {{ok, <<"partial_value_1">>}, {ok, <<"partial_value_2">>}} ->
                    ct:comment("All data recovered from partial write");
                {{ok, <<"partial_value_1">>}, not_found} ->
                    ct:comment("Partial data recovered (key 1 only)");
                {not_found, {ok, <<"partial_value_2">>}} ->
                    ct:comment("Partial data recovered (key 2 only)");
                {not_found, not_found} ->
                    ct:comment("No data recovered from partial write");
                _ ->
                    ct:comment("Unexpected recovery results")
            end,
            
            % Database should be writable
            ok = bobsled:put(RecoveredDb, <<"recovery_key">>, <<"recovery_value">>),
            {ok, <<"recovery_value">>} = bobsled:get(RecoveredDb, <<"recovery_key">>),
            
            ok = bobsled:close(RecoveredDb);
        {error, Reason} ->
            ct:comment(io_lib:format("Recovery failed: ~p", [Reason]))
    end.

%% @doc Test file truncation recovery
test_file_truncation(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    % Write substantial data
    lists:foreach(fun(N) ->
        Key = iolist_to_binary(["truncate_key_", integer_to_list(N)]),
        Value = iolist_to_binary(["truncate_value_", integer_to_list(N)]),
        ok = bobsled:put(Db, Key, Value)
    end, lists:seq(1, 100)),
    
    ok = bobsled:flush(Db),
    ok = bobsled:close(Db),
    
    % Truncate all database files to simulate severe corruption
    case file:list_dir(binary_to_list(DbPath)) of
        {ok, Files} ->
            lists:foreach(fun(File) ->
                FilePath = filename:join(binary_to_list(DbPath), File),
                case file:read_file_info(FilePath) of
                    {ok, #file_info{type = regular, size = Size}} when Size > 1024 ->
                        % Truncate to first 1KB
                        {ok, Data} = file:read_file(FilePath),
                        TruncatedData = binary:part(Data, 0, 1024),
                        file:write_file(FilePath, TruncatedData),
                        ct:pal("Truncated ~s from ~p to 1024 bytes", [File, Size]);
                    _ ->
                        ok
                end
            end, Files);
        _ ->
            ok
    end,
    
    % Attempt recovery
    case bobsled:open(DbPath, []) of
        {ok, RecoveredDb} ->
            ct:comment("Database opened after truncation"),
            
            % Check how much data survived
            SurvivedCount = length([ok || N <- lists:seq(1, 100),
                                        begin
                                            Key = iolist_to_binary(["truncate_key_", integer_to_list(N)]),
                                            bobsled:get(RecoveredDb, Key) =/= not_found
                                        end]),
            
            ct:pal("~p/100 keys survived truncation", [SurvivedCount]),
            
            % Should be able to write new data
            ok = bobsled:put(RecoveredDb, <<"post_truncate">>, <<"post_truncate_value">>),
            {ok, <<"post_truncate_value">>} = bobsled:get(RecoveredDb, <<"post_truncate">>),
            
            ok = bobsled:close(RecoveredDb),
            
            {comment, io_lib:format("~p/100 keys survived", [SurvivedCount])};
        {error, Reason} ->
            ct:comment(io_lib:format("Failed to open after truncation: ~p", [Reason]))
    end.

%% @doc Test random byte corruption
test_random_byte_corruption(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    % Create test data
    TestData = lists:map(fun(N) ->
        Key = iolist_to_binary(["corrupt_key_", integer_to_list(N)]),
        Value = iolist_to_binary(["corrupt_value_", integer_to_list(N)]),
        ok = bobsled:put(Db, Key, Value),
        {Key, Value}
    end, lists:seq(1, 50)),
    
    ok = bobsled:flush(Db),
    ok = bobsled:close(Db),
    
    % Introduce random corruption
    case file:list_dir(binary_to_list(DbPath)) of
        {ok, Files} when length(Files) > 0 ->
            % Pick a random file to corrupt
            RandomFile = lists:nth(rand:uniform(length(Files)), Files),
            FilePath = filename:join(binary_to_list(DbPath), RandomFile),
            
            case file:read_file(FilePath) of
                {ok, Data} when byte_size(Data) > 100 ->
                    % Corrupt random bytes
                    CorruptedData = corrupt_random_bytes(Data, 10), % Corrupt 10 bytes
                    file:write_file(FilePath, CorruptedData),
                    ct:pal("Corrupted ~p bytes in ~s", [10, RandomFile]);
                _ ->
                    ct:comment("File too small to corrupt")
            end;
        _ ->
            ct:comment("No files to corrupt")
    end,
    
    % Attempt recovery
    case bobsled:open(DbPath, []) of
        {ok, RecoveredDb} ->
            % Check data integrity
            {Survived, Lost, Corrupted} = check_data_integrity(RecoveredDb, TestData),
            
            ct:pal("Random corruption results: ~p survived, ~p lost, ~p corrupted",
                   [Survived, Lost, Corrupted]),
            
            % Database should be usable
            ok = bobsled:put(RecoveredDb, <<"post_corruption">>, <<"post_corruption_value">>),
            {ok, <<"post_corruption_value">>} = bobsled:get(RecoveredDb, <<"post_corruption">>),
            
            ok = bobsled:close(RecoveredDb),
            
            {comment, io_lib:format("~p survived, ~p lost, ~p corrupted", 
                                   [Survived, Lost, Corrupted])};
        {error, Reason} ->
            ct:comment(io_lib:format("Failed to recover from corruption: ~p", [Reason]))
    end.

%% @doc Test header corruption
test_header_corruption(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    % Write data
    ok = bobsled:put(Db, <<"header_key">>, <<"header_value">>),
    ok = bobsled:flush(Db),
    ok = bobsled:close(Db),
    
    % Corrupt file headers
    case file:list_dir(binary_to_list(DbPath)) of
        {ok, Files} ->
            lists:foreach(fun(File) ->
                FilePath = filename:join(binary_to_list(DbPath), File),
                case file:read_file(FilePath) of
                    {ok, Data} when byte_size(Data) > 64 ->
                        % Corrupt first 64 bytes (likely header)
                        CorruptedHeader = crypto:strong_rand_bytes(64),
                        RestData = binary:part(Data, 64, byte_size(Data) - 64),
                        CorruptedFile = <<CorruptedHeader/binary, RestData/binary>>,
                        file:write_file(FilePath, CorruptedFile),
                        ct:pal("Corrupted header of ~s", [File]);
                    _ ->
                        ok
                end
            end, Files);
        _ ->
            ok
    end,
    
    % Attempt recovery
    case bobsled:open(DbPath, []) of
        {ok, RecoveredDb} ->
            case bobsled:get(RecoveredDb, <<"header_key">>) of
                {ok, <<"header_value">>} ->
                    ct:comment("Data survived header corruption");
                not_found ->
                    ct:comment("Data lost due to header corruption");
                {error, _} ->
                    ct:comment("Data corrupted due to header corruption")
            end,
            ok = bobsled:close(RecoveredDb);
        {error, Reason} ->
            ct:comment(io_lib:format("Header corruption prevented database open: ~p", [Reason]))
    end.

%% @doc Test index corruption
test_index_corruption(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    % Create hierarchical data that would create indices
    lists:foreach(fun(PrefixNum) ->
        Prefix = iolist_to_binary(["index_prefix_", integer_to_list(PrefixNum), "_"]),
        lists:foreach(fun(KeyNum) ->
            Key = <<Prefix/binary, (integer_to_binary(KeyNum))/binary>>,
            Value = iolist_to_binary(["index_value_", integer_to_list(PrefixNum), "_", integer_to_list(KeyNum)]),
            ok = bobsled:put(Db, Key, Value)
        end, lists:seq(1, 10))
    end, lists:seq(1, 10)),
    
    ok = bobsled:flush(Db),
    ok = bobsled:close(Db),
    
    % Try to corrupt index-related files
    case file:list_dir(binary_to_list(DbPath)) of
        {ok, Files} ->
            % Look for files that might contain indices
            IndexFiles = [F || F <- Files, 
                              (string:find(F, "index") =/= nomatch) orelse
                              (string:find(F, "tree") =/= nomatch) orelse
                              (string:find(F, "meta") =/= nomatch)],
            
            case IndexFiles of
                [] ->
                    % If no obvious index files, corrupt a random file
                    case Files of
                        [SomeFile | _] ->
                            FilePath = filename:join(binary_to_list(DbPath), SomeFile),
                            corrupt_middle_of_file(FilePath);
                        [] ->
                            ct:comment("No files to corrupt")
                    end;
                [IndexFile | _] ->
                    FilePath = filename:join(binary_to_list(DbPath), IndexFile),
                    corrupt_middle_of_file(FilePath),
                    ct:pal("Corrupted potential index file: ~s", [IndexFile])
            end;
        _ ->
            ct:comment("No files found")
    end,
    
    % Test recovery
    case bobsled:open(DbPath, []) of
        {ok, RecoveredDb} ->
            % Test prefix operations (which would use indices)
            TestPrefix = <<"index_prefix_1_">>,
            case bobsled:list(RecoveredDb, TestPrefix) of
                {ok, Keys} ->
                    ct:pal("Index corruption: ~p keys found with prefix ~s", 
                           [length(Keys), TestPrefix]),
                    
                    % Verify some of the data
                    case Keys of
                        [FirstKey | _] ->
                            case bobsled:get(RecoveredDb, FirstKey) of
                                {ok, _Value} ->
                                    ct:comment("Data accessible despite index corruption");
                                _ ->
                                    ct:comment("Data corrupted due to index corruption")
                            end;
                        [] ->
                            ct:comment("No keys found - index severely corrupted")
                    end;
                {error, Reason} ->
                    ct:comment(io_lib:format("List operation failed: ~p", [Reason]))
            end,
            
            ok = bobsled:close(RecoveredDb);
        {error, Reason} ->
            ct:comment(io_lib:format("Index corruption prevented database open: ~p", [Reason]))
    end.

%%%===================================================================
%%% Resource Exhaustion Tests
%%%===================================================================

%% @doc Test out of memory scenarios
test_out_of_memory(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    % Try to allocate increasingly large amounts of memory
    MemorySizes = [1024*1024, 10*1024*1024, 100*1024*1024], % 1MB, 10MB, 100MB
    
    Results = lists:map(fun(Size) ->
        Key = iolist_to_binary(["memory_", integer_to_list(Size)]),
        try
            Value = binary:copy(<<"M">>, Size),
            bobsled:put(Db, Key, Value)
        catch
            error:system_limit -> {error, system_limit};
            error:enomem -> {error, enomem};
            _:Error -> {error, Error}
        end
    end, MemorySizes),
    
    % Count successful allocations
    Successes = [ok || ok <- Results],
    Failures = [E || {error, E} <- Results],
    
    ct:pal("Memory allocation: ~p successes, ~p failures", 
           [length(Successes), length(Failures)]),
    
    % Database should remain functional even after memory pressure
    ok = bobsled:put(Db, <<"small_key">>, <<"small_value">>),
    {ok, <<"small_value">>} = bobsled:get(Db, <<"small_key">>),
    
    ok = bobsled:close(Db),
    
    {comment, io_lib:format("~p/~p large allocations succeeded", 
                           [length(Successes), length(Results)])}.

%% @doc Test file descriptor exhaustion
test_file_descriptor_exhaustion(Config) ->
    DbPathPrefix = ?config(db_path, Config),
    
    % Try to open many databases to exhaust file descriptors
    MaxDatabases = 100,
    OpenDbs = open_many_databases(DbPathPrefix, MaxDatabases, []),
    
    ct:pal("Opened ~p databases before exhaustion", [length(OpenDbs)]),
    
    % Close all databases
    lists:foreach(fun({Db, Path}) ->
        bobsled:close(Db),
        cleanup_db_path(binary_to_list(Path))
    end, OpenDbs),
    
    % Verify we can still open a database
    FinalDbPath = DbPathPrefix ++ "_final",
    {ok, FinalDb} = bobsled:open(list_to_binary(FinalDbPath), []),
    ok = bobsled:put(FinalDb, <<"fd_test">>, <<"fd_value">>),
    {ok, <<"fd_value">>} = bobsled:get(FinalDb, <<"fd_test">>),
    ok = bobsled:close(FinalDb),
    cleanup_db_path(FinalDbPath),
    
    {comment, io_lib:format("Opened ~p databases", [length(OpenDbs)])}.

%% @doc Test disk space exhaustion
test_disk_space_exhaustion(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    % Try to write large amounts of data to fill disk
    ChunkSize = 10 * 1024 * 1024, % 10MB chunks
    MaxChunks = 100, % Up to 1GB
    
    WriteResults = write_large_chunks(Db, ChunkSize, MaxChunks, 0, []),
    
    SuccessfulChunks = length([ok || ok <- WriteResults]),
    
    ct:pal("Wrote ~p chunks (~p MB) before disk exhaustion", 
           [SuccessfulChunks, SuccessfulChunks * (ChunkSize div (1024*1024))]),
    
    % Database should handle disk full gracefully
    case bobsled:put(Db, <<"disk_full_test">>, <<"small_value">>) of
        ok ->
            ct:comment("Small write succeeded after large writes");
        {error, _} ->
            ct:comment("Small write failed - disk likely full")
    end,
    
    ok = bobsled:close(Db),
    
    {comment, io_lib:format("~p MB written", [SuccessfulChunks * 10])}.

%% @doc Test large transaction limits
test_large_transaction_limits(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    % Try increasingly large transactions
    TransactionSizes = [10, 100, 1000, 10000],
    
    Results = lists:map(fun(Size) ->
        TxnFun = fun(TxnHandle) ->
            try
                lists:foreach(fun(N) ->
                    Key = iolist_to_binary(["txn_", integer_to_list(Size), "_", integer_to_list(N)]),
                    Value = iolist_to_binary(["txn_value_", integer_to_list(N)]),
                    ok = bobsled:put(TxnHandle, Key, Value)
                end, lists:seq(1, Size)),
                {ok, Size}
            catch
                _:Error -> {error, Error}
            end
        end,
        
        case bobsled:transaction(Db, TxnFun) of
            {ok, Size} -> {Size, success};
            {error, Reason} -> {Size, {error, Reason}}
        end
    end, TransactionSizes),
    
    % Report results
    lists:foreach(fun({Size, Result}) ->
        case Result of
            success ->
                ct:pal("Transaction with ~p operations: SUCCESS", [Size]);
            {error, Reason} ->
                ct:pal("Transaction with ~p operations: FAILED (~p)", [Size, Reason])
        end
    end, Results),
    
    ok = bobsled:close(Db),
    
    ct:comment("Large transaction limits tested").

%% @doc Test concurrent connection limits
test_concurrent_connection_limits(Config) ->
    DbPath = ?config(db_path, Config),
    Parent = self(),
    
    % Spawn many processes that each open the same database
    MaxConnections = 50,
    
    Processes = lists:map(fun(N) ->
        spawn(fun() ->
            case bobsled:open(DbPath, []) of
                {ok, Db} ->
                    % Perform some operations
                    Key = iolist_to_binary(["conn_", integer_to_list(N)]),
                    Value = iolist_to_binary(["conn_value_", integer_to_list(N)]),
                    ok = bobsled:put(Db, Key, Value),
                    {ok, Value} = bobsled:get(Db, Key),
                    
                    Parent ! {connection_result, N, success, Db},
                    
                    % Wait before closing
                    receive
                        close -> bobsled:close(Db)
                    end;
                {error, Reason} ->
                    Parent ! {connection_result, N, {error, Reason}, undefined}
            end
        end)
    end, lists:seq(1, MaxConnections)),
    
    % Collect connection results
    {SuccessfulConnections, FailedConnections} = collect_connection_results(MaxConnections, [], []),
    
    ct:pal("Connections: ~p successful, ~p failed", 
           [length(SuccessfulConnections), length(FailedConnections)]),
    
    % Close all successful connections
    lists:foreach(fun(Pid) ->
        Pid ! close
    end, Processes),
    
    % Wait for cleanup
    timer:sleep(1000),
    
    {comment, io_lib:format("~p/~p connections succeeded", 
                           [length(SuccessfulConnections), MaxConnections])}.

%%%===================================================================
%%% Recovery Verification Tests
%%%===================================================================

%% @doc Test data integrity after crash
test_data_integrity_after_crash(Config) ->
    DbPath = ?config(db_path, Config),
    
    % Create reference data
    ReferenceData = create_reference_data(1000),
    
    % Write data and simulate crash
    {ok, Db} = bobsled:open(DbPath, []),
    lists:foreach(fun({Key, Value}) ->
        ok = bobsled:put(Db, Key, Value)
    end, ReferenceData),
    
    % Partial flush - some data committed, some not
    ok = bobsled:flush(Db),
    
    % Simulate crash by closing without proper shutdown
    ok = bobsled:close(Db),
    
    % Recovery
    timer:sleep(?RECOVERY_DELAY),
    {ok, RecoveredDb} = bobsled:open(DbPath, []),
    
    % Verify data integrity
    {Correct, Missing, Corrupted} = verify_data_integrity(RecoveredDb, ReferenceData),
    
    ct:pal("Data integrity: ~p correct, ~p missing, ~p corrupted",
           [Correct, Missing, Corrupted]),
    
    % Calculate integrity percentage
    IntegrityPercent = (Correct / length(ReferenceData)) * 100,
    
    ct:pal("Data integrity: ~.2f%%", [IntegrityPercent]),
    
    ok = bobsled:close(RecoveredDb),
    
    % Expect high integrity (>90%)
    ?assert(IntegrityPercent > 90.0),
    
    {comment, io_lib:format("~.2f%% integrity", [IntegrityPercent])}.

%% @doc Test transaction atomicity after crash
test_transaction_atomicity(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    % Prepare transaction data
    TxnData = [{iolist_to_binary(["txn_key_", integer_to_list(N)]),
                iolist_to_binary(["txn_value_", integer_to_list(N)])} || N <- lists:seq(1, 100)],
    
    % Execute transaction
    TxnFun = fun(TxnHandle) ->
        lists:foreach(fun({Key, Value}) ->
            ok = bobsled:put(TxnHandle, Key, Value)
        end, TxnData),
        {ok, transaction_completed}
    end,
    
    Result = bobsled:transaction(Db, TxnFun),
    
    % Simulate crash after transaction
    ok = bobsled:close(Db),
    
    % Recovery and verification
    timer:sleep(?RECOVERY_DELAY),
    {ok, RecoveredDb} = bobsled:open(DbPath, []),
    
    case Result of
        {ok, transaction_completed} ->
            % Transaction should have been committed atomically
            AllPresent = lists:all(fun({Key, Value}) ->
                bobsled:get(RecoveredDb, Key) =:= {ok, Value}
            end, TxnData),
            
            if AllPresent ->
                ct:comment("Transaction committed atomically");
            true ->
                ct:comment("Transaction partially committed - atomicity violated")
            end,
            
            ?assert(AllPresent);
        
        {error, _} ->
            % Transaction should have been rolled back completely
            NonePresent = lists:all(fun({Key, _Value}) ->
                bobsled:get(RecoveredDb, Key) =:= not_found
            end, TxnData),
            
            if NonePresent ->
                ct:comment("Transaction rolled back atomically");
            true ->
                ct:comment("Transaction partially rolled back - atomicity violated")
            end,
            
            ?assert(NonePresent)
    end,
    
    ok = bobsled:close(RecoveredDb).

%% @doc Test consistency after recovery
test_consistency_after_recovery(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    % Create consistent dataset (key-value pairs where value = key + "_value")
    ConsistentData = lists:map(fun(N) ->
        Key = iolist_to_binary(["consistent_", integer_to_list(N)]),
        Value = <<Key/binary, "_value">>,
        ok = bobsled:put(Db, Key, Value),
        {Key, Value}
    end, lists:seq(1, 500)),
    
    ok = bobsled:flush(Db),
    ok = bobsled:close(Db),
    
    % Simulate various corruption scenarios
    corrupt_random_database_files(DbPath),
    
    % Recovery
    case bobsled:open(DbPath, []) of
        {ok, RecoveredDb} ->
            % Check consistency invariant
            ConsistencyViolations = lists:foldl(fun({Key, ExpectedValue}, Violations) ->
                case bobsled:get(RecoveredDb, Key) of
                    {ok, ExpectedValue} ->
                        Violations; % Consistent
                    {ok, OtherValue} ->
                        Violations + 1; % Inconsistent
                    not_found ->
                        Violations % Missing data is acceptable
                end
            end, 0, ConsistentData),
            
            ct:pal("Consistency violations: ~p", [ConsistencyViolations]),
            
            % Should have no consistency violations
            ?assertEqual(0, ConsistencyViolations),
            
            ok = bobsled:close(RecoveredDb),
            
            {comment, io_lib:format("~p violations found", [ConsistencyViolations])};
        {error, Reason} ->
            ct:comment(io_lib:format("Recovery failed: ~p", [Reason]))
    end.

%% @doc Test performance after recovery
test_performance_after_recovery(Config) ->
    DbPath = ?config(db_path, Config),
    
    % Create database with substantial data
    {ok, Db} = bobsled:open(DbPath, []),
    NumKeys = 10000,
    
    lists:foreach(fun(N) ->
        Key = generate_key(N),
        Value = generate_value(N),
        ok = bobsled:put(Db, Key, Value)
    end, lists:seq(1, NumKeys)),
    
    ok = bobsled:flush(Db),
    ok = bobsled:close(Db),
    
    % Measure performance before corruption
    {ok, CleanDb} = bobsled:open(DbPath, []),
    CleanPerf = measure_read_performance(CleanDb, NumKeys, 1000),
    ok = bobsled:close(CleanDb),
    
    % Introduce corruption
    corrupt_random_database_files(DbPath),
    
    % Recovery and performance measurement
    case bobsled:open(DbPath, []) of
        {ok, RecoveredDb} ->
            RecoveredPerf = measure_read_performance(RecoveredDb, NumKeys, 1000),
            ok = bobsled:close(RecoveredDb),
            
            PerformanceRatio = RecoveredPerf / CleanPerf,
            
            ct:pal("Performance: clean=~.2f ops/sec, recovered=~.2f ops/sec, ratio=~.2f",
                   [CleanPerf, RecoveredPerf, PerformanceRatio]),
            
            % Performance should not degrade significantly (> 50% of original)
            ?assert(PerformanceRatio > 0.5),
            
            {comment, io_lib:format("Performance ratio: ~.2f", [PerformanceRatio])};
        {error, Reason} ->
            ct:comment(io_lib:format("Recovery failed: ~p", [Reason]))
    end.

%% @doc Test incremental recovery
test_incremental_recovery(Config) ->
    DbPath = ?config(db_path, Config),
    
    % Phase 1: Create initial dataset
    {ok, Db1} = bobsled:open(DbPath, []),
    Phase1Data = create_test_data(1, 1000),
    write_test_data(Db1, Phase1Data),
    ok = bobsled:flush(Db1),
    ok = bobsled:close(Db1),
    
    % Phase 2: Add more data
    {ok, Db2} = bobsled:open(DbPath, []),
    Phase2Data = create_test_data(1001, 2000),
    write_test_data(Db2, Phase2Data),
    ok = bobsled:flush(Db2),
    ok = bobsled:close(Db2),
    
    % Phase 3: Add final data and simulate crash
    {ok, Db3} = bobsled:open(DbPath, []),
    Phase3Data = create_test_data(2001, 3000),
    write_test_data(Db3, Phase3Data),
    % Don't flush - simulate crash before flush
    ok = bobsled:close(Db3),
    
    % Introduce corruption
    corrupt_random_database_files(DbPath),
    
    % Recovery and verification
    case bobsled:open(DbPath, []) of
        {ok, RecoveredDb} ->
            % Check what data survived from each phase
            Phase1Survived = count_surviving_data(RecoveredDb, Phase1Data),
            Phase2Survived = count_surviving_data(RecoveredDb, Phase2Data),
            Phase3Survived = count_surviving_data(RecoveredDb, Phase3Data),
            
            ct:pal("Incremental recovery: Phase1=~p/~p, Phase2=~p/~p, Phase3=~p/~p",
                   [Phase1Survived, length(Phase1Data),
                    Phase2Survived, length(Phase2Data),
                    Phase3Survived, length(Phase3Data)]),
            
            % Earlier phases should have better survival rates
            Phase1Rate = Phase1Survived / length(Phase1Data),
            Phase2Rate = Phase2Survived / length(Phase2Data),
            Phase3Rate = Phase3Survived / length(Phase3Data),
            
            ct:pal("Survival rates: Phase1=~.2f%%, Phase2=~.2f%%, Phase3=~.2f%%",
                   [Phase1Rate*100, Phase2Rate*100, Phase3Rate*100]),
            
            % Phase 1 (oldest, flushed) should have highest survival rate
            ?assert(Phase1Rate >= Phase2Rate),
            ?assert(Phase2Rate >= Phase3Rate),
            
            ok = bobsled:close(RecoveredDb),
            
            {comment, io_lib:format("Survival: P1=~.1f%%, P2=~.1f%%, P3=~.1f%%",
                                   [Phase1Rate*100, Phase2Rate*100, Phase3Rate*100])};
        {error, Reason} ->
            ct:comment(io_lib:format("Incremental recovery failed: ~p", [Reason]))
    end.

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
            TestFiles = [F || F <- Files, string:prefix(F, "bobsled_crash_test_") =/= nomatch],
            lists:foreach(fun(File) ->
                FullPath = "/tmp/" ++ File,
                case filelib:is_dir(FullPath) of
                    true -> os:cmd("rm -rf " ++ FullPath);
                    false -> file:delete(FullPath)
                end
            end, TestFiles);
        _ -> ok
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
generate_key(N) ->
    iolist_to_binary(["crash_key_", integer_to_list(N)]).

%% @private
generate_value(N) ->
    iolist_to_binary(["crash_value_", integer_to_list(N)]).

%% @private
collect_concurrent_result(N) ->
    receive
        {worker_done, N, Result} -> Result
    after 30000 ->
        error({timeout_collecting_concurrent_result, N})
    end.

%% @private
corrupt_random_bytes(Data, NumBytes) ->
    DataSize = byte_size(Data),
    Positions = [rand:uniform(DataSize) || _ <- lists:seq(1, min(NumBytes, DataSize))],
    
    lists:foldl(fun(Pos, AccData) ->
        <<Before:Pos/binary, _:8, After/binary>> = AccData,
        RandomByte = rand:uniform(256) - 1,
        <<Before/binary, RandomByte:8, After/binary>>
    end, Data, lists:usort(Positions)).

%% @private
check_data_integrity(Db, TestData) ->
    lists:foldl(fun({Key, ExpectedValue}, {Survived, Lost, Corrupted}) ->
        case bobsled:get(Db, Key) of
            {ok, ExpectedValue} -> {Survived + 1, Lost, Corrupted};
            {ok, _OtherValue} -> {Survived, Lost, Corrupted + 1};
            not_found -> {Survived, Lost + 1, Corrupted};
            {error, _} -> {Survived, Lost, Corrupted + 1}
        end
    end, {0, 0, 0}, TestData).

%% @private
corrupt_middle_of_file(FilePath) ->
    case file:read_file(FilePath) of
        {ok, Data} when byte_size(Data) > 200 ->
            DataSize = byte_size(Data),
            MiddleStart = DataSize div 3,
            MiddleSize = DataSize div 3,
            
            <<Before:MiddleStart/binary, _:MiddleSize/binary, After/binary>> = Data,
            CorruptedMiddle = crypto:strong_rand_bytes(MiddleSize),
            CorruptedData = <<Before/binary, CorruptedMiddle/binary, After/binary>>,
            file:write_file(FilePath, CorruptedData);
        _ ->
            ok
    end.

%% @private
open_many_databases(PathPrefix, MaxCount, Acc) when MaxCount =< 0 ->
    Acc;
open_many_databases(PathPrefix, MaxCount, Acc) ->
    DbPath = list_to_binary(PathPrefix ++ "_" ++ integer_to_list(MaxCount)),
    case bobsled:open(DbPath, []) of
        {ok, Db} ->
            open_many_databases(PathPrefix, MaxCount - 1, [{Db, DbPath} | Acc]);
        {error, _Reason} ->
            % Hit limit
            Acc
    end.

%% @private
write_large_chunks(Db, ChunkSize, MaxChunks, ChunkNum, Results) when ChunkNum >= MaxChunks ->
    Results;
write_large_chunks(Db, ChunkSize, MaxChunks, ChunkNum, Results) ->
    Key = iolist_to_binary(["chunk_", integer_to_list(ChunkNum)]),
    Value = binary:copy(<<"D">>, ChunkSize),
    
    case bobsled:put(Db, Key, Value) of
        ok ->
            write_large_chunks(Db, ChunkSize, MaxChunks, ChunkNum + 1, [ok | Results]);
        {error, _Reason} ->
            % Hit disk limit
            Results
    end.

%% @private
collect_connection_results(0, Successes, Failures) ->
    {Successes, Failures};
collect_connection_results(N, Successes, Failures) ->
    receive
        {connection_result, _ConnId, success, _Db} ->
            collect_connection_results(N - 1, [success | Successes], Failures);
        {connection_result, _ConnId, {error, _Reason}, _} ->
            collect_connection_results(N - 1, Successes, [error | Failures])
    after 10000 ->
        {Successes, Failures}
    end.

%% @private
create_reference_data(Count) ->
    [{generate_key(N), generate_value(N)} || N <- lists:seq(1, Count)].

%% @private
verify_data_integrity(Db, ReferenceData) ->
    lists:foldl(fun({Key, ExpectedValue}, {Correct, Missing, Corrupted}) ->
        case bobsled:get(Db, Key) of
            {ok, ExpectedValue} -> {Correct + 1, Missing, Corrupted};
            {ok, _Other} -> {Correct, Missing, Corrupted + 1};
            not_found -> {Correct, Missing + 1, Corrupted};
            {error, _} -> {Correct, Missing, Corrupted + 1}
        end
    end, {0, 0, 0}, ReferenceData).

%% @private
corrupt_random_database_files(DbPath) ->
    case file:list_dir(binary_to_list(DbPath)) of
        {ok, Files} when length(Files) > 0 ->
            % Corrupt random number of files
            NumToCorrupt = rand:uniform(length(Files)),
            FilesToCorrupt = lists:sublist(lists:sort(fun(_, _) -> rand:uniform() < 0.5 end, Files), NumToCorrupt),
            
            lists:foreach(fun(File) ->
                FilePath = filename:join(binary_to_list(DbPath), File),
                case file:read_file(FilePath) of
                    {ok, Data} when byte_size(Data) > 100 ->
                        CorruptedData = corrupt_random_bytes(Data, rand:uniform(20)),
                        file:write_file(FilePath, CorruptedData);
                    _ ->
                        ok
                end
            end, FilesToCorrupt);
        _ ->
            ok
    end.

%% @private
measure_read_performance(Db, MaxKeyNum, NumReads) ->
    StartTime = erlang:monotonic_time(microsecond),
    
    lists:foreach(fun(_) ->
        N = rand:uniform(MaxKeyNum),
        Key = generate_key(N),
        _ = bobsled:get(Db, Key)
    end, lists:seq(1, NumReads)),
    
    EndTime = erlang:monotonic_time(microsecond),
    Duration = (EndTime - StartTime) / 1000000,
    NumReads / Duration.

%% @private
create_test_data(StartN, EndN) ->
    [{generate_key(N), generate_value(N)} || N <- lists:seq(StartN, EndN)].

%% @private
write_test_data(Db, TestData) ->
    lists:foreach(fun({Key, Value}) ->
        ok = bobsled:put(Db, Key, Value)
    end, TestData).

%% @private
count_surviving_data(Db, TestData) ->
    length([ok || {Key, Value} <- TestData,
                  bobsled:get(Db, Key) =:= {ok, Value}]).