%%%-------------------------------------------------------------------
%%% @doc Performance benchmark tests for bobsled NIF
%%%
%%% This test suite measures and validates performance characteristics:
%%% - Read/write throughput benchmarks
%%% - Concurrent operation scalability
%%% - Memory usage patterns
%%% - Large dataset handling
%%% - Operation latency measurements
%%% - Stress testing under load
%%%
%%% @author Bobsled Test Team
%%% @copyright 2025 Bobsled Project
%%% @end
%%%-------------------------------------------------------------------
-module(bobsled_perf_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(DB_PATH_PREFIX, "/tmp/bobsled_perf_test_").
-define(PERF_TIMEOUT, 300000). % 5 minutes for performance tests
-define(BENCHMARK_DURATION, 10000). % 10 seconds per benchmark
-define(WARMUP_DURATION, 2000). % 2 seconds warmup

%%%===================================================================
%%% CT Callbacks
%%%===================================================================

%% @doc Return list of test groups and test cases
all() ->
    [
        {group, throughput_benchmarks},
        {group, latency_benchmarks},
        {group, concurrency_benchmarks},
        {group, memory_benchmarks},
        {group, large_data_benchmarks},
        {group, stress_tests}
    ].

%% @doc Define test groups
groups() ->
    [
        {throughput_benchmarks, [], [
            bench_sequential_writes,
            bench_sequential_reads,
            bench_mixed_operations,
            bench_batch_operations,
            bench_iteration_operations
        ]},
        {latency_benchmarks, [], [
            bench_single_operation_latency,
            bench_transaction_latency,
            bench_cas_latency,
            bench_prefix_scan_latency
        ]},
        {concurrency_benchmarks, [], [
            bench_concurrent_reads,
            bench_concurrent_writes,
            bench_concurrent_mixed,
            bench_reader_writer_scaling,
            bench_transaction_contention
        ]},
        {memory_benchmarks, [], [
            bench_memory_usage_growth,
            bench_cache_efficiency,
            bench_large_value_handling,
            bench_gc_pressure
        ]},
        {large_data_benchmarks, [], [
            bench_million_keys,
            bench_large_values,
            bench_deep_prefix_trees,
            bench_database_size_growth
        ]},
        {stress_tests, [], [
            stress_sustained_load,
            stress_rapid_opens_closes,
            stress_memory_pressure,
            stress_mixed_workload
        ]}
    ].

%% @doc Suite initialization
init_per_suite(Config) ->
    % Ensure clean test environment
    cleanup_test_files(),
    
    % Set performance test timeouts
    [{timeout, ?PERF_TIMEOUT} | Config].

%% @doc Suite cleanup
end_per_suite(_Config) ->
    cleanup_test_files(),
    ok.

%% @doc Group initialization
init_per_group(_Group, Config) ->
    % Create performance metrics collector
    ets:new(perf_metrics, [named_table, public]),
    Config.

%% @doc Group cleanup
end_per_group(_Group, _Config) ->
    case ets:info(perf_metrics) of
        undefined -> ok;
        _ -> ets:delete(perf_metrics)
    end,
    ok.

%% @doc Test case initialization
init_per_testcase(TestCase, Config) ->
    % Create unique database path for each test
    DbPath = generate_db_path(TestCase),
    
    % Initialize performance tracking
    put(test_start_time, erlang:monotonic_time(microsecond)),
    put(operation_count, 0),
    put(error_count, 0),
    
    [{db_path, DbPath} | Config].

%% @doc Test case cleanup
end_per_testcase(TestCase, Config) ->
    % Calculate and report basic metrics
    EndTime = erlang:monotonic_time(microsecond),
    StartTime = get(test_start_time),
    Duration = EndTime - StartTime,
    
    OpCount = get(operation_count),
    ErrorCount = get(error_count),
    
    ct:pal("~p Performance Summary:", [TestCase]),
    ct:pal("  Duration: ~.2f seconds", [Duration / 1000000]),
    ct:pal("  Operations: ~p", [OpCount]),
    ct:pal("  Errors: ~p", [ErrorCount]),
    
    if OpCount > 0 ->
        Throughput = OpCount / (Duration / 1000000),
        AvgLatency = Duration / OpCount,
        ct:pal("  Throughput: ~.2f ops/sec", [Throughput]),
        ct:pal("  Avg Latency: ~.2f μs", [AvgLatency]);
    true ->
        ok
    end,
    
    % Cleanup database files
    DbPath = ?config(db_path, Config),
    cleanup_db_path(DbPath),
    ok.

%%%===================================================================
%%% Throughput Benchmarks
%%%===================================================================

%% @doc Benchmark sequential write operations
bench_sequential_writes(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    ct:pal("Benchmarking sequential writes..."),
    
    % Warmup
    warmup_writes(Db, 1000),
    
    % Benchmark
    NumOps = 100000,
    KeySize = 32,
    ValueSize = 256,
    
    StartTime = erlang:monotonic_time(microsecond),
    
    lists:foreach(fun(N) ->
        Key = generate_key(N, KeySize),
        Value = generate_value(N, ValueSize),
        ok = bobsled:put(Db, Key, Value),
        increment_operation_count()
    end, lists:seq(1, NumOps)),
    
    EndTime = erlang:monotonic_time(microsecond),
    Duration = (EndTime - StartTime) / 1000000,
    Throughput = NumOps / Duration,
    
    ct:pal("Sequential Writes: ~.2f ops/sec (~p ops in ~.2f sec)", 
           [Throughput, NumOps, Duration]),
    
    % Store metric
    store_metric(sequential_writes_throughput, Throughput),
    
    ok = bobsled:close(Db),
    
    % Verify minimum performance
    ?assert(Throughput > 10000), % At least 10k ops/sec
    
    {comment, io_lib:format("~.2f ops/sec", [Throughput])}.

%% @doc Benchmark sequential read operations
bench_sequential_reads(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    ct:pal("Benchmarking sequential reads..."),
    
    % Setup data
    NumOps = 100000,
    KeySize = 32,
    ValueSize = 256,
    
    ct:pal("Setting up ~p records...", [NumOps]),
    lists:foreach(fun(N) ->
        Key = generate_key(N, KeySize),
        Value = generate_value(N, ValueSize),
        ok = bobsled:put(Db, Key, Value)
    end, lists:seq(1, NumOps)),
    
    % Warmup reads
    warmup_reads(Db, 1000, KeySize),
    
    % Benchmark reads
    StartTime = erlang:monotonic_time(microsecond),
    
    lists:foreach(fun(N) ->
        Key = generate_key(N, KeySize),
        {ok, _Value} = bobsled:get(Db, Key),
        increment_operation_count()
    end, lists:seq(1, NumOps)),
    
    EndTime = erlang:monotonic_time(microsecond),
    Duration = (EndTime - StartTime) / 1000000,
    Throughput = NumOps / Duration,
    
    ct:pal("Sequential Reads: ~.2f ops/sec (~p ops in ~.2f sec)", 
           [Throughput, NumOps, Duration]),
    
    store_metric(sequential_reads_throughput, Throughput),
    
    ok = bobsled:close(Db),
    
    % Verify minimum performance
    ?assert(Throughput > 20000), % At least 20k ops/sec for reads
    
    {comment, io_lib:format("~.2f ops/sec", [Throughput])}.

%% @doc Benchmark mixed read/write operations
bench_mixed_operations(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    ct:pal("Benchmarking mixed read/write operations..."),
    
    % Setup initial data
    InitialKeys = 10000,
    KeySize = 32,
    ValueSize = 256,
    
    lists:foreach(fun(N) ->
        Key = generate_key(N, KeySize),
        Value = generate_value(N, ValueSize),
        ok = bobsled:put(Db, Key, Value)
    end, lists:seq(1, InitialKeys)),
    
    % Mixed workload: 70% reads, 20% writes, 10% deletes
    NumOps = 50000,
    
    StartTime = erlang:monotonic_time(microsecond),
    
    lists:foreach(fun(N) ->
        OpType = case N rem 10 of
            X when X < 7 -> read;  % 70% reads
            X when X < 9 -> write; % 20% writes
            _ -> delete            % 10% deletes
        end,
        
        KeyId = (N rem InitialKeys) + 1,
        Key = generate_key(KeyId, KeySize),
        
        case OpType of
            read ->
                _ = bobsled:get(Db, Key);
            write ->
                Value = generate_value(N, ValueSize),
                ok = bobsled:put(Db, Key, Value);
            delete ->
                ok = bobsled:delete(Db, Key)
        end,
        increment_operation_count()
    end, lists:seq(1, NumOps)),
    
    EndTime = erlang:monotonic_time(microsecond),
    Duration = (EndTime - StartTime) / 1000000,
    Throughput = NumOps / Duration,
    
    ct:pal("Mixed Operations: ~.2f ops/sec (~p ops in ~.2f sec)", 
           [Throughput, NumOps, Duration]),
    
    store_metric(mixed_operations_throughput, Throughput),
    
    ok = bobsled:close(Db),
    
    {comment, io_lib:format("~.2f ops/sec", [Throughput])}.

%% @doc Benchmark batch operations
bench_batch_operations(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    ct:pal("Benchmarking batch operations..."),
    
    KeySize = 32,
    ValueSize = 256,
    BatchSize = 1000,
    NumBatches = 100,
    
    StartTime = erlang:monotonic_time(microsecond),
    
    lists:foreach(fun(BatchNum) ->
        KVPairs = lists:map(fun(N) ->
            Key = generate_key(BatchNum * BatchSize + N, KeySize),
            Value = generate_value(BatchNum * BatchSize + N, ValueSize),
            {Key, Value}
        end, lists:seq(1, BatchSize)),
        
        ok = bobsled:batch_put(Db, KVPairs),
        increment_operation_count(BatchSize)
    end, lists:seq(0, NumBatches - 1)),
    
    EndTime = erlang:monotonic_time(microsecond),
    Duration = (EndTime - StartTime) / 1000000,
    TotalOps = BatchSize * NumBatches,
    Throughput = TotalOps / Duration,
    
    ct:pal("Batch Operations: ~.2f ops/sec (~p ops in ~p batches, ~.2f sec)", 
           [Throughput, TotalOps, NumBatches, Duration]),
    
    store_metric(batch_operations_throughput, Throughput),
    
    ok = bobsled:close(Db),
    
    {comment, io_lib:format("~.2f ops/sec", [Throughput])}.

%% @doc Benchmark iteration operations
bench_iteration_operations(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    ct:pal("Benchmarking iteration operations..."),
    
    % Setup hierarchical data
    NumPrefixes = 100,
    KeysPerPrefix = 1000,
    KeySize = 32,
    ValueSize = 256,
    
    ct:pal("Setting up hierarchical data..."),
    lists:foreach(fun(PrefixNum) ->
        Prefix = iolist_to_binary(["prefix_", integer_to_list(PrefixNum), "_"]),
        lists:foreach(fun(KeyNum) ->
            Key = <<Prefix/binary, (generate_key(KeyNum, KeySize))/binary>>,
            Value = generate_value(PrefixNum * 1000 + KeyNum, ValueSize),
            ok = bobsled:put(Db, Key, Value)
        end, lists:seq(1, KeysPerPrefix))
    end, lists:seq(1, NumPrefixes)),
    
    % Benchmark list operations
    ListStartTime = erlang:monotonic_time(microsecond),
    
    lists:foreach(fun(PrefixNum) ->
        Prefix = iolist_to_binary(["prefix_", integer_to_list(PrefixNum), "_"]),
        {ok, Keys} = bobsled:list(Db, Prefix),
        ?assertEqual(KeysPerPrefix, length(Keys)),
        increment_operation_count()
    end, lists:seq(1, NumPrefixes)),
    
    ListEndTime = erlang:monotonic_time(microsecond),
    ListDuration = (ListEndTime - ListStartTime) / 1000000,
    ListThroughput = NumPrefixes / ListDuration,
    
    ct:pal("List Operations: ~.2f ops/sec (~p prefixes in ~.2f sec)", 
           [ListThroughput, NumPrefixes, ListDuration]),
    
    % Benchmark fold operations
    FoldStartTime = erlang:monotonic_time(microsecond),
    
    lists:foreach(fun(PrefixNum) ->
        Prefix = iolist_to_binary(["prefix_", integer_to_list(PrefixNum), "_"]),
        CountFun = fun(_Key, _Value, Acc) -> Acc + 1 end,
        {ok, Count} = bobsled:fold(Db, CountFun, 0, Prefix),
        ?assertEqual(KeysPerPrefix, Count),
        increment_operation_count()
    end, lists:seq(1, NumPrefixes)),
    
    FoldEndTime = erlang:monotonic_time(microsecond),
    FoldDuration = (FoldEndTime - FoldStartTime) / 1000000,
    FoldThroughput = NumPrefixes / FoldDuration,
    
    ct:pal("Fold Operations: ~.2f ops/sec (~p prefixes in ~.2f sec)", 
           [FoldThroughput, NumPrefixes, FoldDuration]),
    
    store_metric(list_operations_throughput, ListThroughput),
    store_metric(fold_operations_throughput, FoldThroughput),
    
    ok = bobsled:close(Db),
    
    {comment, io_lib:format("List: ~.2f, Fold: ~.2f ops/sec", 
                           [ListThroughput, FoldThroughput])}.

%%%===================================================================
%%% Latency Benchmarks
%%%===================================================================

%% @doc Benchmark single operation latency
bench_single_operation_latency(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    ct:pal("Benchmarking single operation latency..."),
    
    KeySize = 32,
    ValueSize = 256,
    NumSamples = 10000,
    
    % Setup some existing data
    lists:foreach(fun(N) ->
        Key = generate_key(N, KeySize),
        Value = generate_value(N, ValueSize),
        ok = bobsled:put(Db, Key, Value)
    end, lists:seq(1, NumSamples div 2)),
    
    % Measure put latency
    PutLatencies = lists:map(fun(N) ->
        Key = generate_key(N + NumSamples, KeySize),
        Value = generate_value(N + NumSamples, ValueSize),
        
        StartTime = erlang:monotonic_time(nanosecond),
        ok = bobsled:put(Db, Key, Value),
        EndTime = erlang:monotonic_time(nanosecond),
        
        (EndTime - StartTime) / 1000  % Convert to microseconds
    end, lists:seq(1, 1000)),
    
    % Measure get latency
    GetLatencies = lists:map(fun(N) ->
        Key = generate_key(N, KeySize),
        
        StartTime = erlang:monotonic_time(nanosecond),
        {ok, _Value} = bobsled:get(Db, Key),
        EndTime = erlang:monotonic_time(nanosecond),
        
        (EndTime - StartTime) / 1000  % Convert to microseconds
    end, lists:seq(1, 1000)),
    
    % Measure delete latency
    DeleteLatencies = lists:map(fun(N) ->
        Key = generate_key(N, KeySize),
        
        StartTime = erlang:monotonic_time(nanosecond),
        ok = bobsled:delete(Db, Key),
        EndTime = erlang:monotonic_time(nanosecond),
        
        (EndTime - StartTime) / 1000  % Convert to microseconds
    end, lists:seq(1, 1000)),
    
    % Calculate statistics
    PutStats = calculate_latency_stats(PutLatencies),
    GetStats = calculate_latency_stats(GetLatencies),
    DeleteStats = calculate_latency_stats(DeleteLatencies),
    
    ct:pal("Operation Latencies (μs):"),
    ct:pal("  PUT  - Mean: ~.2f, P50: ~.2f, P95: ~.2f, P99: ~.2f", 
           [maps:get(mean, PutStats), maps:get(p50, PutStats),
            maps:get(p95, PutStats), maps:get(p99, PutStats)]),
    ct:pal("  GET  - Mean: ~.2f, P50: ~.2f, P95: ~.2f, P99: ~.2f", 
           [maps:get(mean, GetStats), maps:get(p50, GetStats),
            maps:get(p95, GetStats), maps:get(p99, GetStats)]),
    ct:pal("  DEL  - Mean: ~.2f, P50: ~.2f, P95: ~.2f, P99: ~.2f", 
           [maps:get(mean, DeleteStats), maps:get(p50, DeleteStats),
            maps:get(p95, DeleteStats), maps:get(p99, DeleteStats)]),
    
    store_metric(put_latency_p99, maps:get(p99, PutStats)),
    store_metric(get_latency_p99, maps:get(p99, GetStats)),
    store_metric(delete_latency_p99, maps:get(p99, DeleteStats)),
    
    ok = bobsled:close(Db),
    
    {comment, io_lib:format("PUT P99: ~.2f μs, GET P99: ~.2f μs", 
                           [maps:get(p99, PutStats), maps:get(p99, GetStats)])}.

%% @doc Benchmark transaction latency
bench_transaction_latency(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    ct:pal("Benchmarking transaction latency..."),
    
    KeySize = 32,
    ValueSize = 256,
    NumTxns = 1000,
    
    % Setup initial data
    lists:foreach(fun(N) ->
        Key = generate_key(N, KeySize),
        Value = generate_value(N, ValueSize),
        ok = bobsled:put(Db, Key, Value)
    end, lists:seq(1, NumTxns)),
    
    % Measure simple transaction latency
    SimpleTxnLatencies = lists:map(fun(N) ->
        TxnFun = fun(TxnHandle) ->
            Key = generate_key(N + NumTxns, KeySize),
            Value = generate_value(N + NumTxns, ValueSize),
            ok = bobsled:put(TxnHandle, Key, Value),
            {ok, success}
        end,
        
        StartTime = erlang:monotonic_time(nanosecond),
        {ok, success} = bobsled:transaction(Db, TxnFun),
        EndTime = erlang:monotonic_time(nanosecond),
        
        (EndTime - StartTime) / 1000  % Convert to microseconds
    end, lists:seq(1, NumTxns)),
    
    % Measure read-modify-write transaction latency
    RMWTxnLatencies = lists:map(fun(N) ->
        TxnFun = fun(TxnHandle) ->
            Key = generate_key(N, KeySize),
            case bobsled:get(TxnHandle, Key) of
                {ok, OldValue} ->
                    NewValue = <<OldValue/binary, "_modified">>,
                    ok = bobsled:put(TxnHandle, Key, NewValue),
                    {ok, modified};
                not_found ->
                    {ok, not_found}
            end
        end,
        
        StartTime = erlang:monotonic_time(nanosecond),
        {ok, _} = bobsled:transaction(Db, TxnFun),
        EndTime = erlang:monotonic_time(nanosecond),
        
        (EndTime - StartTime) / 1000  % Convert to microseconds
    end, lists:seq(1, NumTxns)),
    
    SimpleTxnStats = calculate_latency_stats(SimpleTxnLatencies),
    RMWTxnStats = calculate_latency_stats(RMWTxnLatencies),
    
    ct:pal("Transaction Latencies (μs):"),
    ct:pal("  Simple - Mean: ~.2f, P50: ~.2f, P95: ~.2f, P99: ~.2f", 
           [maps:get(mean, SimpleTxnStats), maps:get(p50, SimpleTxnStats),
            maps:get(p95, SimpleTxnStats), maps:get(p99, SimpleTxnStats)]),
    ct:pal("  RMW    - Mean: ~.2f, P50: ~.2f, P95: ~.2f, P99: ~.2f", 
           [maps:get(mean, RMWTxnStats), maps:get(p50, RMWTxnStats),
            maps:get(p95, RMWTxnStats), maps:get(p99, RMWTxnStats)]),
    
    store_metric(simple_txn_latency_p99, maps:get(p99, SimpleTxnStats)),
    store_metric(rmw_txn_latency_p99, maps:get(p99, RMWTxnStats)),
    
    ok = bobsled:close(Db),
    
    {comment, io_lib:format("Simple P99: ~.2f μs, RMW P99: ~.2f μs", 
                           [maps:get(p99, SimpleTxnStats), maps:get(p99, RMWTxnStats)])}.

%% @doc Benchmark compare-and-swap latency
bench_cas_latency(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    ct:pal("Benchmarking compare-and-swap latency..."),
    
    KeySize = 32,
    ValueSize = 256,
    NumOps = 1000,
    
    % Setup initial data
    lists:foreach(fun(N) ->
        Key = generate_key(N, KeySize),
        Value = generate_value(N, ValueSize),
        ok = bobsled:put(Db, Key, Value)
    end, lists:seq(1, NumOps)),
    
    % Measure successful CAS latency
    SuccessfulCASLatencies = lists:map(fun(N) ->
        Key = generate_key(N, KeySize),
        OldValue = generate_value(N, ValueSize),
        NewValue = generate_value(N + 1000, ValueSize),
        
        StartTime = erlang:monotonic_time(nanosecond),
        ok = bobsled:compare_and_swap(Db, Key, OldValue, NewValue),
        EndTime = erlang:monotonic_time(nanosecond),
        
        (EndTime - StartTime) / 1000  % Convert to microseconds
    end, lists:seq(1, NumOps)),
    
    % Measure failed CAS latency
    FailedCASLatencies = lists:map(fun(N) ->
        Key = generate_key(N, KeySize),
        WrongValue = <<"wrong_value">>,
        NewValue = generate_value(N + 2000, ValueSize),
        
        StartTime = erlang:monotonic_time(nanosecond),
        {error, cas_failed} = bobsled:compare_and_swap(Db, Key, WrongValue, NewValue),
        EndTime = erlang:monotonic_time(nanosecond),
        
        (EndTime - StartTime) / 1000  % Convert to microseconds
    end, lists:seq(1, NumOps)),
    
    SuccessfulStats = calculate_latency_stats(SuccessfulCASLatencies),
    FailedStats = calculate_latency_stats(FailedCASLatencies),
    
    ct:pal("CAS Latencies (μs):"),
    ct:pal("  Successful - Mean: ~.2f, P50: ~.2f, P95: ~.2f, P99: ~.2f", 
           [maps:get(mean, SuccessfulStats), maps:get(p50, SuccessfulStats),
            maps:get(p95, SuccessfulStats), maps:get(p99, SuccessfulStats)]),
    ct:pal("  Failed     - Mean: ~.2f, P50: ~.2f, P95: ~.2f, P99: ~.2f", 
           [maps:get(mean, FailedStats), maps:get(p50, FailedStats),
            maps:get(p95, FailedStats), maps:get(p99, FailedStats)]),
    
    store_metric(cas_success_latency_p99, maps:get(p99, SuccessfulStats)),
    store_metric(cas_failed_latency_p99, maps:get(p99, FailedStats)),
    
    ok = bobsled:close(Db),
    
    {comment, io_lib:format("Success P99: ~.2f μs, Failed P99: ~.2f μs", 
                           [maps:get(p99, SuccessfulStats), maps:get(p99, FailedStats)])}.

%% @doc Benchmark prefix scan latency
bench_prefix_scan_latency(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    ct:pal("Benchmarking prefix scan latency..."),
    
    % Setup data with various prefix lengths
    PrefixLengths = [10, 100, 1000, 10000],
    
    lists:foreach(fun(PrefixLen) ->
        Prefix = iolist_to_binary(["prefix_", integer_to_list(PrefixLen), "_"]),
        lists:foreach(fun(N) ->
            Key = <<Prefix/binary, (generate_key(N, 16))/binary>>,
            Value = generate_value(N, 64),
            ok = bobsled:put(Db, Key, Value)
        end, lists:seq(1, PrefixLen))
    end, PrefixLengths),
    
    % Measure list operation latency for different prefix sizes
    ListResults = lists:map(fun(PrefixLen) ->
        Prefix = iolist_to_binary(["prefix_", integer_to_list(PrefixLen), "_"]),
        
        Latencies = lists:map(fun(_) ->
            StartTime = erlang:monotonic_time(nanosecond),
            {ok, Keys} = bobsled:list(Db, Prefix),
            EndTime = erlang:monotonic_time(nanosecond),
            
            ?assertEqual(PrefixLen, length(Keys)),
            (EndTime - StartTime) / 1000  % Convert to microseconds
        end, lists:seq(1, 10)),  % 10 samples per prefix length
        
        Stats = calculate_latency_stats(Latencies),
        {PrefixLen, Stats}
    end, PrefixLengths),
    
    ct:pal("List Operation Latencies by Result Size:"),
    lists:foreach(fun({PrefixLen, Stats}) ->
        ct:pal("  ~p keys - Mean: ~.2f μs, P99: ~.2f μs", 
               [PrefixLen, maps:get(mean, Stats), maps:get(p99, Stats)])
    end, ListResults),
    
    ok = bobsled:close(Db),
    
    {comment, "Prefix scan latency measured"}.

%%%===================================================================
%%% Concurrency Benchmarks
%%%===================================================================

%% @doc Benchmark concurrent read performance
bench_concurrent_reads(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    ct:pal("Benchmarking concurrent reads..."),
    
    % Setup data
    NumKeys = 10000,
    KeySize = 32,
    ValueSize = 256,
    
    ct:pal("Setting up ~p keys...", [NumKeys]),
    lists:foreach(fun(N) ->
        Key = generate_key(N, KeySize),
        Value = generate_value(N, ValueSize),
        ok = bobsled:put(Db, Key, Value)
    end, lists:seq(1, NumKeys)),
    
    % Test different numbers of concurrent readers
    ReaderCounts = [1, 2, 4, 8, 16, 32],
    
    Results = lists:map(fun(NumReaders) ->
        ct:pal("Testing with ~p concurrent readers...", [NumReaders]),
        
        Parent = self(),
        Duration = 5000, % 5 seconds per test
        
        % Spawn readers
        lists:foreach(fun(ReaderId) ->
            spawn(fun() ->
                read_worker(Db, ReaderId, NumKeys, KeySize, Duration, Parent)
            end)
        end, lists:seq(1, NumReaders)),
        
        % Collect results
        TotalOps = collect_worker_results(NumReaders, 0),
        Throughput = TotalOps / (Duration / 1000),
        
        ct:pal("~p readers: ~.2f ops/sec (~p ops total)", 
               [NumReaders, Throughput, TotalOps]),
        
        {NumReaders, Throughput}
    end, ReaderCounts),
    
    % Store best result
    {_, BestThroughput} = lists:max(Results),
    store_metric(concurrent_reads_peak_throughput, BestThroughput),
    
    ok = bobsled:close(Db),
    
    {comment, io_lib:format("Peak: ~.2f ops/sec", [BestThroughput])}.

%% @doc Benchmark concurrent write performance
bench_concurrent_writes(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    ct:pal("Benchmarking concurrent writes..."),
    
    % Test different numbers of concurrent writers
    WriterCounts = [1, 2, 4, 8, 16],
    KeySize = 32,
    ValueSize = 256,
    
    Results = lists:map(fun(NumWriters) ->
        ct:pal("Testing with ~p concurrent writers...", [NumWriters]),
        
        Parent = self(),
        Duration = 5000, % 5 seconds per test
        
        % Spawn writers
        lists:foreach(fun(WriterId) ->
            spawn(fun() ->
                write_worker(Db, WriterId, KeySize, ValueSize, Duration, Parent)
            end)
        end, lists:seq(1, NumWriters)),
        
        % Collect results
        TotalOps = collect_worker_results(NumWriters, 0),
        Throughput = TotalOps / (Duration / 1000),
        
        ct:pal("~p writers: ~.2f ops/sec (~p ops total)", 
               [NumWriters, Throughput, TotalOps]),
        
        {NumWriters, Throughput}
    end, WriterCounts),
    
    % Store best result
    {_, BestThroughput} = lists:max(Results),
    store_metric(concurrent_writes_peak_throughput, BestThroughput),
    
    ok = bobsled:close(Db),
    
    {comment, io_lib:format("Peak: ~.2f ops/sec", [BestThroughput])}.

%% @doc Benchmark concurrent mixed operations
bench_concurrent_mixed(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    ct:pal("Benchmarking concurrent mixed operations..."),
    
    % Setup initial data
    NumKeys = 10000,
    KeySize = 32,
    ValueSize = 256,
    
    lists:foreach(fun(N) ->
        Key = generate_key(N, KeySize),
        Value = generate_value(N, ValueSize),
        ok = bobsled:put(Db, Key, Value)
    end, lists:seq(1, NumKeys)),
    
    % Test mixed workload with readers and writers
    MixedConfigs = [
        {8, 2},   % 8 readers, 2 writers
        {4, 4},   % 4 readers, 4 writers
        {2, 8}    % 2 readers, 8 writers
    ],
    
    Results = lists:map(fun({NumReaders, NumWriters}) ->
        ct:pal("Testing with ~p readers, ~p writers...", [NumReaders, NumWriters]),
        
        Parent = self(),
        Duration = 5000, % 5 seconds per test
        
        % Spawn readers
        lists:foreach(fun(ReaderId) ->
            spawn(fun() ->
                read_worker(Db, ReaderId, NumKeys, KeySize, Duration, Parent)
            end)
        end, lists:seq(1, NumReaders)),
        
        % Spawn writers
        lists:foreach(fun(WriterId) ->
            spawn(fun() ->
                write_worker(Db, WriterId + 10000, KeySize, ValueSize, Duration, Parent)
            end)
        end, lists:seq(1, NumWriters)),
        
        % Collect results
        TotalOps = collect_worker_results(NumReaders + NumWriters, 0),
        Throughput = TotalOps / (Duration / 1000),
        
        ct:pal("~pR/~pW: ~.2f ops/sec (~p ops total)", 
               [NumReaders, NumWriters, Throughput, TotalOps]),
        
        {{NumReaders, NumWriters}, Throughput}
    end, MixedConfigs),
    
    % Store best result
    {_, BestThroughput} = lists:max(Results),
    store_metric(concurrent_mixed_peak_throughput, BestThroughput),
    
    ok = bobsled:close(Db),
    
    {comment, io_lib:format("Peak: ~.2f ops/sec", [BestThroughput])}.

%% @doc Benchmark reader-writer scaling
bench_reader_writer_scaling(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    ct:pal("Benchmarking reader-writer scaling..."),
    
    % Setup initial data
    NumKeys = 10000,
    KeySize = 32,
    ValueSize = 256,
    
    lists:foreach(fun(N) ->
        Key = generate_key(N, KeySize),
        Value = generate_value(N, ValueSize),
        ok = bobsled:put(Db, Key, Value)
    end, lists:seq(1, NumKeys)),
    
    % Test scaling with increasing total workers (maintaining 80/20 read/write ratio)
    TotalWorkerCounts = [4, 8, 16, 32],
    
    Results = lists:map(fun(TotalWorkers) ->
        NumReaders = round(TotalWorkers * 0.8),
        NumWriters = TotalWorkers - NumReaders,
        
        ct:pal("Testing scaling with ~p total workers (~pR/~pW)...", 
               [TotalWorkers, NumReaders, NumWriters]),
        
        Parent = self(),
        Duration = 5000, % 5 seconds per test
        
        % Spawn workers
        lists:foreach(fun(WorkerId) ->
            if WorkerId =< NumReaders ->
                spawn(fun() ->
                    read_worker(Db, WorkerId, NumKeys, KeySize, Duration, Parent)
                end);
            true ->
                spawn(fun() ->
                    write_worker(Db, WorkerId + 10000, KeySize, ValueSize, Duration, Parent)
                end)
            end
        end, lists:seq(1, TotalWorkers)),
        
        % Collect results
        TotalOps = collect_worker_results(TotalWorkers, 0),
        Throughput = TotalOps / (Duration / 1000),
        ThroughputPerWorker = Throughput / TotalWorkers,
        
        ct:pal("~p workers: ~.2f ops/sec total, ~.2f ops/sec/worker", 
               [TotalWorkers, Throughput, ThroughputPerWorker]),
        
        {TotalWorkers, Throughput, ThroughputPerWorker}
    end, TotalWorkerCounts),
    
    ok = bobsled:close(Db),
    
    {comment, "Reader-writer scaling measured"}.

%% @doc Benchmark transaction contention
bench_transaction_contention(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    ct:pal("Benchmarking transaction contention..."),
    
    % Setup shared counters
    NumCounters = 10,
    lists:foreach(fun(N) ->
        Key = iolist_to_binary(["counter_", integer_to_list(N)]),
        ok = bobsled:put(Db, Key, <<"0">>)
    end, lists:seq(1, NumCounters)),
    
    % Test contention with different numbers of workers
    WorkerCounts = [1, 2, 4, 8, 16],
    
    Results = lists:map(fun(NumWorkers) ->
        ct:pal("Testing transaction contention with ~p workers...", [NumWorkers]),
        
        Parent = self(),
        Duration = 5000, % 5 seconds per test
        
        % Spawn transaction workers
        lists:foreach(fun(WorkerId) ->
            spawn(fun() ->
                transaction_worker(Db, WorkerId, NumCounters, Duration, Parent)
            end)
        end, lists:seq(1, NumWorkers)),
        
        % Collect results
        {TotalSuccesses, TotalRetries} = collect_transaction_results(NumWorkers, {0, 0}),
        SuccessRate = TotalSuccesses / (TotalSuccesses + TotalRetries),
        Throughput = TotalSuccesses / (Duration / 1000),
        
        ct:pal("~p workers: ~.2f%% success rate, ~.2f successful txns/sec", 
               [NumWorkers, SuccessRate * 100, Throughput]),
        
        {NumWorkers, SuccessRate, Throughput}
    end, WorkerCounts),
    
    ok = bobsled:close(Db),
    
    {comment, "Transaction contention measured"}.

%%%===================================================================
%%% Memory Benchmarks
%%%===================================================================

%% @doc Benchmark memory usage growth
bench_memory_usage_growth(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    ct:pal("Benchmarking memory usage growth..."),
    
    % Measure memory at different data sizes
    DataSizes = [1000, 10000, 100000, 1000000],
    KeySize = 32,
    ValueSize = 256,
    
    InitialMemory = get_process_memory(),
    
    Results = lists:map(fun(DataSize) ->
        ct:pal("Adding ~p records...", [DataSize]),
        
        lists:foreach(fun(N) ->
            Key = generate_key(N, KeySize),
            Value = generate_value(N, ValueSize),
            ok = bobsled:put(Db, Key, Value)
        end, lists:seq(1, DataSize)),
        
        % Force flush to get accurate disk size
        ok = bobsled:flush(Db),
        
        CurrentMemory = get_process_memory(),
        {ok, DiskSize} = bobsled:size_on_disk(Db),
        
        MemoryUsed = CurrentMemory - InitialMemory,
        
        ct:pal("~p records: ~p KB memory, ~p KB disk", 
               [DataSize, MemoryUsed div 1024, DiskSize div 1024]),
        
        {DataSize, MemoryUsed, DiskSize}
    end, DataSizes),
    
    ok = bobsled:close(Db),
    
    {comment, "Memory usage patterns measured"}.

%% @doc Benchmark cache efficiency
bench_cache_efficiency(Config) ->
    DbPath = ?config(db_path, Config),
    
    % Test with different cache sizes
    CacheSizes = [1024*1024, 10*1024*1024, 100*1024*1024], % 1MB, 10MB, 100MB
    
    Results = lists:map(fun(CacheSize) ->
        ct:pal("Testing cache efficiency with ~p MB cache...", [CacheSize div (1024*1024)]),
        
        {ok, Db} = bobsled:open(DbPath, [{cache_capacity, CacheSize}]),
        
        % Create dataset larger than cache
        NumKeys = 100000,
        KeySize = 32,
        ValueSize = 256,
        
        % Fill database
        lists:foreach(fun(N) ->
            Key = generate_key(N, KeySize),
            Value = generate_value(N, ValueSize),
            ok = bobsled:put(Db, Key, Value)
        end, lists:seq(1, NumKeys)),
        
        % Measure read performance (cache hits vs misses)
        NumReads = 10000,
        
        % Sequential reads (likely cache misses)
        SeqStartTime = erlang:monotonic_time(microsecond),
        lists:foreach(fun(N) ->
            Key = generate_key(N, KeySize),
            {ok, _Value} = bobsled:get(Db, Key)
        end, lists:seq(1, NumReads)),
        SeqEndTime = erlang:monotonic_time(microsecond),
        SeqDuration = SeqEndTime - SeqStartTime,
        
        % Random reads (mixed cache hits/misses)
        RandomStartTime = erlang:monotonic_time(microsecond),
        lists:foreach(fun(_) ->
            N = rand:uniform(NumKeys),
            Key = generate_key(N, KeySize),
            {ok, _Value} = bobsled:get(Db, Key)
        end, lists:seq(1, NumReads)),
        RandomEndTime = erlang:monotonic_time(microsecond),
        RandomDuration = RandomEndTime - RandomStartTime,
        
        % Repeated reads (likely cache hits)
        RepeatKeys = [generate_key(N, KeySize) || N <- lists:seq(1, 100)],
        RepeatStartTime = erlang:monotonic_time(microsecond),
        lists:foreach(fun(_) ->
            Key = lists:nth(rand:uniform(100), RepeatKeys),
            {ok, _Value} = bobsled:get(Db, Key)
        end, lists:seq(1, NumReads)),
        RepeatEndTime = erlang:monotonic_time(microsecond),
        RepeatDuration = RepeatEndTime - RepeatStartTime,
        
        ok = bobsled:close(Db),
        cleanup_db_path(DbPath),
        
        SeqThroughput = NumReads / (SeqDuration / 1000000),
        RandomThroughput = NumReads / (RandomDuration / 1000000),
        RepeatThroughput = NumReads / (RepeatDuration / 1000000),
        
        ct:pal("  Sequential reads: ~.2f ops/sec", [SeqThroughput]),
        ct:pal("  Random reads:     ~.2f ops/sec", [RandomThroughput]),
        ct:pal("  Repeated reads:   ~.2f ops/sec", [RepeatThroughput]),
        
        {CacheSize, SeqThroughput, RandomThroughput, RepeatThroughput}
    end, CacheSizes),
    
    {comment, "Cache efficiency patterns measured"}.

%% @doc Benchmark large value handling
bench_large_value_handling(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    ct:pal("Benchmarking large value handling..."),
    
    % Test different value sizes
    ValueSizes = [1024, 10*1024, 100*1024, 1024*1024], % 1KB, 10KB, 100KB, 1MB
    KeySize = 32,
    NumOps = 1000,
    
    Results = lists:map(fun(ValueSize) ->
        ct:pal("Testing ~p KB values...", [ValueSize div 1024]),
        
        % Write test
        WriteStartTime = erlang:monotonic_time(microsecond),
        lists:foreach(fun(N) ->
            Key = generate_key(N, KeySize),
            Value = generate_value(N, ValueSize),
            ok = bobsled:put(Db, Key, Value)
        end, lists:seq(1, NumOps)),
        WriteEndTime = erlang:monotonic_time(microsecond),
        WriteDuration = (WriteEndTime - WriteStartTime) / 1000000,
        WriteThroughput = NumOps / WriteDuration,
        WriteBandwidth = (NumOps * ValueSize) / WriteDuration / (1024*1024), % MB/s
        
        % Read test
        ReadStartTime = erlang:monotonic_time(microsecond),
        lists:foreach(fun(N) ->
            Key = generate_key(N, KeySize),
            {ok, _Value} = bobsled:get(Db, Key)
        end, lists:seq(1, NumOps)),
        ReadEndTime = erlang:monotonic_time(microsecond),
        ReadDuration = (ReadEndTime - ReadStartTime) / 1000000,
        ReadThroughput = NumOps / ReadDuration,
        ReadBandwidth = (NumOps * ValueSize) / ReadDuration / (1024*1024), % MB/s
        
        ct:pal("  Write: ~.2f ops/sec, ~.2f MB/s", [WriteThroughput, WriteBandwidth]),
        ct:pal("  Read:  ~.2f ops/sec, ~.2f MB/s", [ReadThroughput, ReadBandwidth]),
        
        {ValueSize, WriteThroughput, ReadThroughput, WriteBandwidth, ReadBandwidth}
    end, ValueSizes),
    
    ok = bobsled:close(Db),
    
    {comment, "Large value handling measured"}.

%% @doc Benchmark garbage collection pressure
bench_gc_pressure(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    ct:pal("Benchmarking GC pressure..."),
    
    % Monitor GC stats before test
    {InitialGCs, InitialGCTime} = get_gc_stats(),
    
    % Perform operations that create garbage
    NumOps = 100000,
    KeySize = 32,
    ValueSize = 256,
    
    StartTime = erlang:monotonic_time(microsecond),
    
    lists:foreach(fun(N) ->
        Key = generate_key(N, KeySize),
        Value = generate_value(N, ValueSize),
        ok = bobsled:put(Db, Key, Value),
        
        % Occasionally read and create temporary binaries
        if N rem 100 =:= 0 ->
            {ok, _} = bobsled:get(Db, Key),
            _ = binary:copy(Value, 2); % Create garbage
        true ->
            ok
        end
    end, lists:seq(1, NumOps)),
    
    EndTime = erlang:monotonic_time(microsecond),
    Duration = (EndTime - StartTime) / 1000000,
    
    % Monitor GC stats after test
    {FinalGCs, FinalGCTime} = get_gc_stats(),
    
    GCCount = FinalGCs - InitialGCs,
    GCTime = FinalGCTime - InitialGCTime,
    Throughput = NumOps / Duration,
    
    ct:pal("GC Impact:"),
    ct:pal("  Operations: ~p in ~.2f seconds (~.2f ops/sec)", [NumOps, Duration, Throughput]),
    ct:pal("  GC cycles: ~p", [GCCount]),
    ct:pal("  GC time: ~.2f ms", [GCTime / 1000]),
    ct:pal("  GC overhead: ~.2f%%", [(GCTime / (Duration * 1000000)) * 100]),
    
    ok = bobsled:close(Db),
    
    {comment, io_lib:format("~p GCs, ~.2f%% overhead", 
                           [GCCount, (GCTime / (Duration * 1000000)) * 100])}.

%%%===================================================================
%%% Large Data Benchmarks
%%%===================================================================

%% @doc Benchmark million key operations
bench_million_keys(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    ct:pal("Benchmarking million key operations..."),
    
    NumKeys = 1000000,
    KeySize = 32,
    ValueSize = 64,
    
    ct:pal("Writing ~p keys...", [NumKeys]),
    WriteStartTime = erlang:monotonic_time(microsecond),
    
    lists:foreach(fun(N) ->
        Key = generate_key(N, KeySize),
        Value = generate_value(N, ValueSize),
        ok = bobsled:put(Db, Key, Value),
        
        if N rem 100000 =:= 0 ->
            ct:pal("  Written ~p keys", [N]);
        true ->
            ok
        end
    end, lists:seq(1, NumKeys)),
    
    WriteEndTime = erlang:monotonic_time(microsecond),
    WriteDuration = (WriteEndTime - WriteStartTime) / 1000000,
    WriteThroughput = NumKeys / WriteDuration,
    
    ct:pal("Write complete: ~.2f ops/sec (~.2f minutes)", 
           [WriteThroughput, WriteDuration / 60]),
    
    % Test random reads
    ct:pal("Testing random reads..."),
    NumReads = 100000,
    ReadStartTime = erlang:monotonic_time(microsecond),
    
    lists:foreach(fun(_) ->
        N = rand:uniform(NumKeys),
        Key = generate_key(N, KeySize),
        {ok, _Value} = bobsled:get(Db, Key)
    end, lists:seq(1, NumReads)),
    
    ReadEndTime = erlang:monotonic_time(microsecond),
    ReadDuration = (ReadEndTime - ReadStartTime) / 1000000,
    ReadThroughput = NumReads / ReadDuration,
    
    ct:pal("Random reads: ~.2f ops/sec", [ReadThroughput]),
    
    % Check database size
    {ok, DiskSize} = bobsled:size_on_disk(Db),
    ct:pal("Database size: ~.2f MB", [DiskSize / (1024 * 1024)]),
    
    store_metric(million_keys_write_throughput, WriteThroughput),
    store_metric(million_keys_read_throughput, ReadThroughput),
    
    ok = bobsled:close(Db),
    
    {comment, io_lib:format("Write: ~.2f, Read: ~.2f ops/sec", 
                           [WriteThroughput, ReadThroughput])}.

%% @doc Benchmark large values
bench_large_values(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    ct:pal("Benchmarking large values..."),
    
    % Test with 10MB values
    NumKeys = 100,
    KeySize = 32,
    ValueSize = 10 * 1024 * 1024, % 10MB
    
    ct:pal("Writing ~p x ~p MB values...", [NumKeys, ValueSize div (1024*1024)]),
    WriteStartTime = erlang:monotonic_time(microsecond),
    
    lists:foreach(fun(N) ->
        Key = generate_key(N, KeySize),
        Value = generate_value(N, ValueSize),
        ok = bobsled:put(Db, Key, Value),
        ct:pal("  Written key ~p", [N])
    end, lists:seq(1, NumKeys)),
    
    WriteEndTime = erlang:monotonic_time(microsecond),
    WriteDuration = (WriteEndTime - WriteStartTime) / 1000000,
    WriteThroughput = NumKeys / WriteDuration,
    WriteBandwidth = (NumKeys * ValueSize) / WriteDuration / (1024*1024), % MB/s
    
    ct:pal("Large value writes: ~.2f ops/sec, ~.2f MB/s", 
           [WriteThroughput, WriteBandwidth]),
    
    % Test reads
    ct:pal("Reading large values..."),
    ReadStartTime = erlang:monotonic_time(microsecond),
    
    lists:foreach(fun(N) ->
        Key = generate_key(N, KeySize),
        {ok, _Value} = bobsled:get(Db, Key),
        ct:pal("  Read key ~p", [N])
    end, lists:seq(1, NumKeys)),
    
    ReadEndTime = erlang:monotonic_time(microsecond),
    ReadDuration = (ReadEndTime - ReadStartTime) / 1000000,
    ReadThroughput = NumKeys / ReadDuration,
    ReadBandwidth = (NumKeys * ValueSize) / ReadDuration / (1024*1024), % MB/s
    
    ct:pal("Large value reads: ~.2f ops/sec, ~.2f MB/s", 
           [ReadThroughput, ReadBandwidth]),
    
    ok = bobsled:close(Db),
    
    {comment, io_lib:format("Write: ~.2f MB/s, Read: ~.2f MB/s", 
                           [WriteBandwidth, ReadBandwidth])}.

%% @doc Benchmark deep prefix trees
bench_deep_prefix_trees(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    ct:pal("Benchmarking deep prefix trees..."),
    
    % Create hierarchical data: level1/level2/level3/key
    Levels = 3,
    BranchingFactor = 10,
    LeafKeysPerBranch = 100,
    
    TotalKeys = round(math:pow(BranchingFactor, Levels) * LeafKeysPerBranch),
    ct:pal("Creating ~p keys in ~p-level hierarchy...", [TotalKeys, Levels]),
    
    WriteStartTime = erlang:monotonic_time(microsecond),
    KeyCount = create_hierarchical_data(Db, Levels, BranchingFactor, LeafKeysPerBranch, []),
    WriteEndTime = erlang:monotonic_time(microsecond),
    
    WriteDuration = (WriteEndTime - WriteStartTime) / 1000000,
    ct:pal("Created ~p keys in ~.2f seconds", [KeyCount, WriteDuration]),
    
    % Test prefix scans at different levels
    lists:foreach(fun(Level) ->
        ct:pal("Testing level ~p prefix scans...", [Level]),
        
        Prefix = create_level_prefix(Level, BranchingFactor),
        
        ScanStartTime = erlang:monotonic_time(microsecond),
        {ok, Keys} = bobsled:list(Db, Prefix),
        ScanEndTime = erlang:monotonic_time(microsecond),
        
        ScanDuration = (ScanEndTime - ScanStartTime) / 1000,
        ct:pal("  Level ~p: ~p keys in ~.2f ms", [Level, length(Keys), ScanDuration])
    end, lists:seq(1, Levels)),
    
    ok = bobsled:close(Db),
    
    {comment, io_lib:format("~p keys in hierarchy", [KeyCount])}.

%% @doc Benchmark database size growth
bench_database_size_growth(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    ct:pal("Benchmarking database size growth..."),
    
    KeySize = 32,
    ValueSize = 256,
    Increments = [10000, 50000, 100000, 500000],
    
    CurrentKeys = 0,
    
    lists:foreach(fun(Increment) ->
        ct:pal("Adding ~p more keys (total will be ~p)...", 
               [Increment, CurrentKeys + Increment]),
        
        StartTime = erlang:monotonic_time(microsecond),
        
        lists:foreach(fun(N) ->
            Key = generate_key(CurrentKeys + N, KeySize),
            Value = generate_value(CurrentKeys + N, ValueSize),
            ok = bobsled:put(Db, Key, Value)
        end, lists:seq(1, Increment)),
        
        EndTime = erlang:monotonic_time(microsecond),
        Duration = (EndTime - StartTime) / 1000000,
        
        NewCurrentKeys = CurrentKeys + Increment,
        
        ok = bobsled:flush(Db),
        {ok, DiskSize} = bobsled:size_on_disk(Db),
        
        Throughput = Increment / Duration,
        SizePerKey = DiskSize / NewCurrentKeys,
        
        ct:pal("  Total keys: ~p", [NewCurrentKeys]),
        ct:pal("  Throughput: ~.2f ops/sec", [Throughput]),
        ct:pal("  Database size: ~.2f MB", [DiskSize / (1024*1024)]),
        ct:pal("  Bytes per key: ~.2f", [SizePerKey]),
        
        put(current_keys, NewCurrentKeys)
    end, Increments),
    
    ok = bobsled:close(Db),
    
    {comment, "Database size growth measured"}.

%%%===================================================================
%%% Stress Tests
%%%===================================================================

%% @doc Stress test with sustained load
stress_sustained_load(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    ct:pal("Running sustained load stress test..."),
    
    Duration = 60000, % 1 minute
    NumWorkers = 8,
    Parent = self(),
    
    ct:pal("Running ~p workers for ~p seconds...", [NumWorkers, Duration div 1000]),
    
    % Spawn stress workers
    lists:foreach(fun(WorkerId) ->
        spawn(fun() ->
            stress_worker(Db, WorkerId, Duration, Parent)
        end)
    end, lists:seq(1, NumWorkers)),
    
    % Monitor and collect results
    Results = monitor_stress_test(NumWorkers, Duration, []),
    
    TotalOps = lists:sum([Ops || {_Worker, Ops, _Errors} <- Results]),
    TotalErrors = lists:sum([Errors || {_Worker, _Ops, Errors} <- Results]),
    
    ct:pal("Sustained load results:"),
    ct:pal("  Total operations: ~p", [TotalOps]),
    ct:pal("  Total errors: ~p", [TotalErrors]),
    ct:pal("  Error rate: ~.4f%%", [(TotalErrors / (TotalOps + TotalErrors)) * 100]),
    ct:pal("  Average throughput: ~.2f ops/sec", [TotalOps / (Duration / 1000)]),
    
    ok = bobsled:close(Db),
    
    % Verify low error rate
    ErrorRate = TotalErrors / (TotalOps + TotalErrors),
    ?assert(ErrorRate < 0.01), % Less than 1% error rate
    
    {comment, io_lib:format("~p ops, ~.4f%% errors", [TotalOps, ErrorRate * 100])}.

%% @doc Stress test rapid open/close cycles
stress_rapid_opens_closes(Config) ->
    DbPath = ?config(db_path, Config),
    
    ct:pal("Running rapid open/close stress test..."),
    
    NumCycles = 1000,
    
    StartTime = erlang:monotonic_time(microsecond),
    
    lists:foreach(fun(N) ->
        {ok, Db} = bobsled:open(DbPath, []),
        
        % Perform a few operations
        Key = generate_key(N, 16),
        Value = generate_value(N, 64),
        ok = bobsled:put(Db, Key, Value),
        {ok, Value} = bobsled:get(Db, Key),
        
        ok = bobsled:close(Db),
        
        if N rem 100 =:= 0 ->
            ct:pal("  Completed ~p cycles", [N]);
        true ->
            ok
        end
    end, lists:seq(1, NumCycles)),
    
    EndTime = erlang:monotonic_time(microsecond),
    Duration = (EndTime - StartTime) / 1000000,
    CyclesPerSecond = NumCycles / Duration,
    
    ct:pal("Rapid open/close: ~p cycles in ~.2f seconds (~.2f cycles/sec)", 
           [NumCycles, Duration, CyclesPerSecond]),
    
    {comment, io_lib:format("~.2f cycles/sec", [CyclesPerSecond])}.

%% @doc Stress test memory pressure
stress_memory_pressure(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    ct:pal("Running memory pressure stress test..."),
    
    % Create large temporary data to pressure GC
    InitialMemory = get_process_memory(),
    
    NumOps = 100000,
    LargeValueSize = 10240, % 10KB values
    
    StartTime = erlang:monotonic_time(microsecond),
    
    lists:foreach(fun(N) ->
        Key = generate_key(N, 32),
        Value = generate_value(N, LargeValueSize),
        
        % Create additional garbage
        _Garbage = binary:copy(Value, 2),
        _MoreGarbage = [Value || _ <- lists:seq(1, 10)],
        
        ok = bobsled:put(Db, Key, Value),
        
        % Occasionally force GC
        if N rem 1000 =:= 0 ->
            erlang:garbage_collect(),
            CurrentMemory = get_process_memory(),
            MemoryGrowth = CurrentMemory - InitialMemory,
            ct:pal("  ~p ops, memory growth: ~p KB", [N, MemoryGrowth div 1024]);
        true ->
            ok
        end
    end, lists:seq(1, NumOps)),
    
    EndTime = erlang:monotonic_time(microsecond),
    Duration = (EndTime - StartTime) / 1000000,
    Throughput = NumOps / Duration,
    
    FinalMemory = get_process_memory(),
    TotalMemoryGrowth = FinalMemory - InitialMemory,
    
    ct:pal("Memory pressure test completed:"),
    ct:pal("  Operations: ~p in ~.2f seconds (~.2f ops/sec)", 
           [NumOps, Duration, Throughput]),
    ct:pal("  Memory growth: ~p KB", [TotalMemoryGrowth div 1024]),
    
    ok = bobsled:close(Db),
    
    {comment, io_lib:format("~.2f ops/sec, ~p KB growth", 
                           [Throughput, TotalMemoryGrowth div 1024])}.

%% @doc Stress test mixed workload
stress_mixed_workload(Config) ->
    DbPath = ?config(db_path, Config),
    {ok, Db} = bobsled:open(DbPath, []),
    
    ct:pal("Running mixed workload stress test..."),
    
    % Setup initial data
    InitialKeys = 10000,
    lists:foreach(fun(N) ->
        Key = generate_key(N, 32),
        Value = generate_value(N, 256),
        ok = bobsled:put(Db, Key, Value)
    end, lists:seq(1, InitialKeys)),
    
    Duration = 30000, % 30 seconds
    WorkerTypes = [
        {read_heavy, 4},      % 4 read-heavy workers
        {write_heavy, 2},     % 2 write-heavy workers
        {mixed, 2},           % 2 mixed workers
        {batch, 1},           % 1 batch worker
        {scan, 1}             % 1 scan worker
    ],
    
    Parent = self(),
    TotalWorkers = lists:sum([Count || {_Type, Count} <- WorkerTypes]),
    
    % Spawn different types of workers
    lists:foreach(fun({WorkerType, Count}) ->
        lists:foreach(fun(N) ->
            spawn(fun() ->
                specialized_stress_worker(Db, WorkerType, N, InitialKeys, Duration, Parent)
            end)
        end, lists:seq(1, Count))
    end, WorkerTypes),
    
    % Collect results
    Results = collect_specialized_results(TotalWorkers, []),
    
    % Analyze results by worker type
    analyze_mixed_workload_results(Results),
    
    ok = bobsled:close(Db),
    
    {comment, "Mixed workload stress test completed"}.

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
            TestFiles = [F || F <- Files, string:prefix(F, "bobsled_perf_test_") =/= nomatch],
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
generate_key(N, Size) ->
    Prefix = iolist_to_binary(["key_", integer_to_list(N), "_"]),
    PrefixSize = byte_size(Prefix),
    if PrefixSize >= Size ->
        binary:part(Prefix, 0, Size);
    true ->
        Padding = binary:copy(<<"x">>, Size - PrefixSize),
        <<Prefix/binary, Padding/binary>>
    end.

%% @private
generate_value(N, Size) ->
    Pattern = iolist_to_binary(["value_", integer_to_list(N), "_"]),
    PatternSize = byte_size(Pattern),
    if PatternSize >= Size ->
        binary:part(Pattern, 0, Size);
    true ->
        Repeats = (Size div PatternSize) + 1,
        LargePattern = binary:copy(Pattern, Repeats),
        binary:part(LargePattern, 0, Size)
    end.

%% @private
increment_operation_count() ->
    increment_operation_count(1).

%% @private
increment_operation_count(Count) ->
    OldCount = get(operation_count),
    put(operation_count, OldCount + Count).

%% @private
store_metric(Name, Value) ->
    case ets:info(perf_metrics) of
        undefined -> ok;
        _ -> ets:insert(perf_metrics, {Name, Value})
    end.

%% @private
warmup_writes(Db, Count) ->
    lists:foreach(fun(N) ->
        Key = generate_key(N, 16),
        Value = generate_value(N, 64),
        ok = bobsled:put(Db, Key, Value)
    end, lists:seq(1, Count)).

%% @private
warmup_reads(Db, Count, KeySize) ->
    lists:foreach(fun(N) ->
        Key = generate_key(N, KeySize),
        _ = bobsled:get(Db, Key)
    end, lists:seq(1, Count)).

%% @private
calculate_latency_stats(Latencies) ->
    Sorted = lists:sort(Latencies),
    Len = length(Sorted),
    Mean = lists:sum(Sorted) / Len,
    P50 = lists:nth((Len div 2) + 1, Sorted),
    P95 = lists:nth(round(Len * 0.95), Sorted),
    P99 = lists:nth(round(Len * 0.99), Sorted),
    #{mean => Mean, p50 => P50, p95 => P95, p99 => P99}.

%% @private
read_worker(Db, WorkerId, NumKeys, KeySize, Duration, Parent) ->
    EndTime = erlang:monotonic_time(millisecond) + Duration,
    Ops = read_worker_loop(Db, WorkerId, NumKeys, KeySize, EndTime, 0),
    Parent ! {worker_result, WorkerId, Ops}.

%% @private
read_worker_loop(Db, WorkerId, NumKeys, KeySize, EndTime, Ops) ->
    case erlang:monotonic_time(millisecond) of
        Now when Now >= EndTime ->
            Ops;
        _ ->
            N = rand:uniform(NumKeys),
            Key = generate_key(N, KeySize),
            _ = bobsled:get(Db, Key),
            read_worker_loop(Db, WorkerId, NumKeys, KeySize, EndTime, Ops + 1)
    end.

%% @private
write_worker(Db, WorkerId, KeySize, ValueSize, Duration, Parent) ->
    EndTime = erlang:monotonic_time(millisecond) + Duration,
    Ops = write_worker_loop(Db, WorkerId, KeySize, ValueSize, EndTime, 0),
    Parent ! {worker_result, WorkerId, Ops}.

%% @private
write_worker_loop(Db, WorkerId, KeySize, ValueSize, EndTime, Ops) ->
    case erlang:monotonic_time(millisecond) of
        Now when Now >= EndTime ->
            Ops;
        _ ->
            Key = generate_key(WorkerId * 1000000 + Ops, KeySize),
            Value = generate_value(WorkerId * 1000000 + Ops, ValueSize),
            ok = bobsled:put(Db, Key, Value),
            write_worker_loop(Db, WorkerId, KeySize, ValueSize, EndTime, Ops + 1)
    end.

%% @private
transaction_worker(Db, WorkerId, NumCounters, Duration, Parent) ->
    EndTime = erlang:monotonic_time(millisecond) + Duration,
    {Successes, Retries} = transaction_worker_loop(Db, WorkerId, NumCounters, EndTime, {0, 0}),
    Parent ! {transaction_result, WorkerId, {Successes, Retries}}.

%% @private
transaction_worker_loop(Db, WorkerId, NumCounters, EndTime, {Successes, Retries}) ->
    case erlang:monotonic_time(millisecond) of
        Now when Now >= EndTime ->
            {Successes, Retries};
        _ ->
            CounterNum = rand:uniform(NumCounters),
            Key = iolist_to_binary(["counter_", integer_to_list(CounterNum)]),
            
            TxnFun = fun(TxnHandle) ->
                case bobsled:get(TxnHandle, Key) of
                    {ok, CounterBin} ->
                        Counter = binary_to_integer(CounterBin),
                        NewCounter = Counter + 1,
                        ok = bobsled:put(TxnHandle, Key, integer_to_binary(NewCounter)),
                        {ok, success};
                    Error ->
                        Error
                end
            end,
            
            case bobsled:transaction(Db, TxnFun) of
                {ok, success} ->
                    transaction_worker_loop(Db, WorkerId, NumCounters, EndTime, {Successes + 1, Retries});
                _Error ->
                    transaction_worker_loop(Db, WorkerId, NumCounters, EndTime, {Successes, Retries + 1})
            end
    end.

%% @private
collect_worker_results(0, Acc) -> Acc;
collect_worker_results(N, Acc) ->
    receive
        {worker_result, _WorkerId, Ops} ->
            collect_worker_results(N - 1, Acc + Ops)
    after 30000 ->
        error({timeout_collecting_worker_results, N})
    end.

%% @private
collect_transaction_results(0, Acc) -> Acc;
collect_transaction_results(N, {AccSuccesses, AccRetries}) ->
    receive
        {transaction_result, _WorkerId, {Successes, Retries}} ->
            collect_transaction_results(N - 1, {AccSuccesses + Successes, AccRetries + Retries})
    after 30000 ->
        error({timeout_collecting_transaction_results, N})
    end.

%% @private
get_process_memory() ->
    case erlang:process_info(self(), memory) of
        {memory, Memory} -> Memory;
        undefined -> 0
    end.

%% @private
get_gc_stats() ->
    case erlang:statistics(garbage_collection) of
        {NumGCs, WordsReclaimed, 0} ->
            {NumGCs, WordsReclaimed * erlang:system_info(wordsize)};
        {NumGCs, WordsReclaimed, GCTime} ->
            {NumGCs, GCTime}
    end.

%% @private
create_hierarchical_data(Db, 0, _BranchingFactor, LeafKeysPerBranch, PathPrefix) ->
    % At leaf level, create keys
    lists:foreach(fun(N) ->
        Key = iolist_to_binary([PathPrefix, "leaf_", integer_to_list(N)]),
        Value = generate_value(N, 64),
        ok = bobsled:put(Db, Key, Value)
    end, lists:seq(1, LeafKeysPerBranch)),
    LeafKeysPerBranch;

create_hierarchical_data(Db, Level, BranchingFactor, LeafKeysPerBranch, PathPrefix) ->
    lists:foldl(fun(Branch, Acc) ->
        NewPrefix = [PathPrefix, "level", integer_to_list(Level), "_branch", integer_to_list(Branch), "_"],
        KeyCount = create_hierarchical_data(Db, Level - 1, BranchingFactor, LeafKeysPerBranch, NewPrefix),
        Acc + KeyCount
    end, 0, lists:seq(1, BranchingFactor)).

%% @private
create_level_prefix(Level, BranchingFactor) ->
    Branch = rand:uniform(BranchingFactor),
    iolist_to_binary(["level", integer_to_list(Level), "_branch", integer_to_list(Branch), "_"]).

%% @private
stress_worker(Db, WorkerId, Duration, Parent) ->
    EndTime = erlang:monotonic_time(millisecond) + Duration,
    {Ops, Errors} = stress_worker_loop(Db, WorkerId, EndTime, {0, 0}),
    Parent ! {stress_result, WorkerId, {Ops, Errors}}.

%% @private
stress_worker_loop(Db, WorkerId, EndTime, {Ops, Errors}) ->
    case erlang:monotonic_time(millisecond) of
        Now when Now >= EndTime ->
            {Ops, Errors};
        _ ->
            OpType = rand:uniform(10),
            try
                case OpType of
                    N when N =< 6 -> % 60% reads
                        Key = generate_key(rand:uniform(10000), 32),
                        _ = bobsled:get(Db, Key);
                    N when N =< 8 -> % 20% writes  
                        Key = generate_key(WorkerId * 1000000 + Ops, 32),
                        Value = generate_value(WorkerId * 1000000 + Ops, 256),
                        ok = bobsled:put(Db, Key, Value);
                    N when N =< 9 -> % 10% deletes
                        Key = generate_key(rand:uniform(10000), 32),
                        ok = bobsled:delete(Db, Key);
                    _ -> % 10% list operations
                        Prefix = iolist_to_binary(["key_", integer_to_list(rand:uniform(100)), "_"]),
                        {ok, _Keys} = bobsled:list(Db, Prefix)
                end,
                stress_worker_loop(Db, WorkerId, EndTime, {Ops + 1, Errors})
            catch
                _:_ ->
                    stress_worker_loop(Db, WorkerId, EndTime, {Ops, Errors + 1})
            end
    end.

%% @private
monitor_stress_test(0, _Duration, Acc) -> Acc;
monitor_stress_test(N, Duration, Acc) ->
    receive
        {stress_result, WorkerId, {Ops, Errors}} ->
            monitor_stress_test(N - 1, Duration, [{WorkerId, Ops, Errors} | Acc])
    after Duration + 5000 -> % Allow extra time for cleanup
        error({timeout_monitoring_stress_test, N})
    end.

%% @private
specialized_stress_worker(Db, WorkerType, WorkerId, InitialKeys, Duration, Parent) ->
    EndTime = erlang:monotonic_time(millisecond) + Duration,
    Result = specialized_worker_loop(Db, WorkerType, WorkerId, InitialKeys, EndTime, #{ops => 0, errors => 0}),
    Parent ! {specialized_result, WorkerType, WorkerId, Result}.

%% @private
specialized_worker_loop(Db, WorkerType, WorkerId, InitialKeys, EndTime, Stats = #{ops := Ops, errors := Errors}) ->
    case erlang:monotonic_time(millisecond) of
        Now when Now >= EndTime ->
            Stats;
        _ ->
            try
                case WorkerType of
                    read_heavy ->
                        % 95% reads, 5% writes
                        case rand:uniform(100) of
                            N when N =< 95 ->
                                Key = generate_key(rand:uniform(InitialKeys), 32),
                                _ = bobsled:get(Db, Key);
                            _ ->
                                Key = generate_key(WorkerId * 1000000 + Ops, 32),
                                Value = generate_value(WorkerId * 1000000 + Ops, 256),
                                ok = bobsled:put(Db, Key, Value)
                        end;
                    
                    write_heavy ->
                        % 20% reads, 80% writes
                        case rand:uniform(100) of
                            N when N =< 20 ->
                                Key = generate_key(rand:uniform(InitialKeys), 32),
                                _ = bobsled:get(Db, Key);
                            _ ->
                                Key = generate_key(WorkerId * 1000000 + Ops, 32),
                                Value = generate_value(WorkerId * 1000000 + Ops, 256),
                                ok = bobsled:put(Db, Key, Value)
                        end;
                    
                    mixed ->
                        % 40% reads, 40% writes, 10% deletes, 10% CAS
                        case rand:uniform(100) of
                            N when N =< 40 ->
                                Key = generate_key(rand:uniform(InitialKeys), 32),
                                _ = bobsled:get(Db, Key);
                            N when N =< 80 ->
                                Key = generate_key(WorkerId * 1000000 + Ops, 32),
                                Value = generate_value(WorkerId * 1000000 + Ops, 256),
                                ok = bobsled:put(Db, Key, Value);
                            N when N =< 90 ->
                                Key = generate_key(rand:uniform(InitialKeys), 32),
                                ok = bobsled:delete(Db, Key);
                            _ ->
                                Key = generate_key(rand:uniform(InitialKeys), 32),
                                OldValue = generate_value(1, 256),
                                NewValue = generate_value(2, 256),
                                _ = bobsled:compare_and_swap(Db, Key, OldValue, NewValue)
                        end;
                    
                    batch ->
                        % Batch operations
                        KVPairs = lists:map(fun(N) ->
                            Key = generate_key(WorkerId * 1000000 + Ops * 100 + N, 32),
                            Value = generate_value(WorkerId * 1000000 + Ops * 100 + N, 256),
                            {Key, Value}
                        end, lists:seq(1, 10)),
                        ok = bobsled:batch_put(Db, KVPairs);
                    
                    scan ->
                        % Scan operations
                        Prefix = iolist_to_binary(["key_", integer_to_list(rand:uniform(1000)), "_"]),
                        {ok, _Keys} = bobsled:list(Db, Prefix)
                end,
                specialized_worker_loop(Db, WorkerType, WorkerId, InitialKeys, EndTime, Stats#{ops => Ops + 1})
            catch
                _:_ ->
                    specialized_worker_loop(Db, WorkerType, WorkerId, InitialKeys, EndTime, Stats#{errors => Errors + 1})
            end
    end.

%% @private
collect_specialized_results(0, Acc) -> Acc;
collect_specialized_results(N, Acc) ->
    receive
        {specialized_result, WorkerType, WorkerId, Result} ->
            collect_specialized_results(N - 1, [{WorkerType, WorkerId, Result} | Acc])
    after 35000 ->
        error({timeout_collecting_specialized_results, N})
    end.

%% @private
analyze_mixed_workload_results(Results) ->
    % Group by worker type
    Grouped = lists:foldl(fun({WorkerType, WorkerId, Result}, Acc) ->
        TypeResults = maps:get(WorkerType, Acc, []),
        Acc#{WorkerType => [{WorkerId, Result} | TypeResults]}
    end, #{}, Results),
    
    ct:pal("Mixed workload analysis:"),
    maps:foreach(fun(WorkerType, TypeResults) ->
        TotalOps = lists:sum([maps:get(ops, Result) || {_WorkerId, Result} <- TypeResults]),
        TotalErrors = lists:sum([maps:get(errors, Result) || {_WorkerId, Result} <- TypeResults]),
        WorkerCount = length(TypeResults),
        
        ct:pal("  ~p (~p workers): ~p ops, ~p errors", 
               [WorkerType, WorkerCount, TotalOps, TotalErrors])
    end, Grouped).