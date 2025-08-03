%%%-------------------------------------------------------------------
%%% @doc Bobsled Performance Benchmarks
%%%
%%% Comprehensive benchmarks for measuring bobsled NIF performance
%%% including sequential and concurrent read/write operations.
%%%
%%% @author Bobsled Team
%%% @copyright 2025 Bobsled Project
%%% @end
%%%-------------------------------------------------------------------
-module(bobsled_bench).

-export([
    run_all/0,
    run_all/1,
    sequential_writes/1,
    sequential_reads/1,
    concurrent_writes/1,
    concurrent_reads/1,
    mixed_workload/1,
    batch_writes/1,
    compare_and_swap_bench/1
]).

-define(DEFAULT_COUNT, 100000).
-define(BATCH_SIZE, 1000).

%%%===================================================================
%%% API Functions
%%%===================================================================

%% @doc Run all benchmarks with default settings
run_all() ->
    run_all(#{}).

%% @doc Run all benchmarks with custom options
run_all(Options) ->
    Count = maps:get(count, Options, ?DEFAULT_COUNT),
    DbPath = maps:get(path, Options, <<"/tmp/bobsled_bench">>),
    
    io:format("~nBobsled Performance Benchmarks~n"),
    io:format("==============================~n"),
    io:format("Database path: ~s~n", [DbPath]),
    io:format("Operation count: ~p~n~n", [Count]),
    
    % Clean up any existing database
    os:cmd("rm -rf " ++ binary_to_list(DbPath)),
    
    % Run benchmarks
    Results = [
        {"Sequential Writes", sequential_writes(Count)},
        {"Sequential Reads", sequential_reads(Count)},
        {"Batch Writes (1000 per batch)", batch_writes(Count)},
        {"Compare-and-Swap", compare_and_swap_bench(Count div 10)},
        {"Concurrent Writes (10 processes)", concurrent_writes(Count)},
        {"Concurrent Reads (10 processes)", concurrent_reads(Count)},
        {"Mixed Workload (50/50 R/W)", mixed_workload(Count)}
    ],
    
    % Print summary
    io:format("~nBenchmark Summary~n"),
    io:format("=================~n"),
    lists:foreach(fun({Name, {ops_per_sec, OpsPerSec, Duration}}) ->
        io:format("~-35s: ~10.2f ops/sec (~.3f seconds)~n", 
                  [Name, OpsPerSec, Duration / 1000000])
    end, Results),
    
    % Clean up
    os:cmd("rm -rf " ++ binary_to_list(DbPath)),
    Results.

%%%===================================================================
%%% Benchmark Functions
%%%===================================================================

%% @doc Benchmark sequential write operations
sequential_writes(Count) ->
    {Db, DbPath} = setup_db(),
    
    io:format("Running sequential write benchmark (~p operations)...~n", [Count]),
    
    % Generate keys and values
    KVPairs = generate_kv_pairs(Count),
    
    % Run benchmark
    StartTime = erlang:monotonic_time(microsecond),
    
    lists:foreach(fun({Key, Value}) ->
        ok = bobsled:put(Db, Key, Value)
    end, KVPairs),
    
    % Ensure all data is written
    ok = bobsled:flush(Db),
    
    EndTime = erlang:monotonic_time(microsecond),
    Duration = EndTime - StartTime,
    OpsPerSec = (Count * 1000000) / Duration,
    
    io:format("Sequential writes: ~.2f ops/sec (~.3f seconds)~n", 
              [OpsPerSec, Duration / 1000000]),
    
    % Verify size
    {ok, Size} = bobsled:size_on_disk(Db),
    io:format("Database size: ~.2f MB~n", [Size / (1024 * 1024)]),
    
    cleanup_db(Db, DbPath),
    {ops_per_sec, OpsPerSec, Duration}.

%% @doc Benchmark sequential read operations
sequential_reads(Count) ->
    {Db, DbPath} = setup_db(),
    
    % First, populate the database
    io:format("Populating database for read benchmark...~n"),
    KVPairs = generate_kv_pairs(Count),
    populate_db(Db, KVPairs),
    
    io:format("Running sequential read benchmark (~p operations)...~n", [Count]),
    
    % Extract just the keys
    Keys = [Key || {Key, _} <- KVPairs],
    
    % Run benchmark
    StartTime = erlang:monotonic_time(microsecond),
    
    lists:foreach(fun(Key) ->
        {ok, _Value} = bobsled:get(Db, Key)
    end, Keys),
    
    EndTime = erlang:monotonic_time(microsecond),
    Duration = EndTime - StartTime,
    OpsPerSec = (Count * 1000000) / Duration,
    
    io:format("Sequential reads: ~.2f ops/sec (~.3f seconds)~n", 
              [OpsPerSec, Duration / 1000000]),
    
    cleanup_db(Db, DbPath),
    {ops_per_sec, OpsPerSec, Duration}.

%% @doc Benchmark batch write operations
batch_writes(Count) ->
    {Db, DbPath} = setup_db(),
    
    BatchCount = Count div ?BATCH_SIZE,
    ActualCount = BatchCount * ?BATCH_SIZE,
    
    io:format("Running batch write benchmark (~p operations in ~p batches)...~n", 
              [ActualCount, BatchCount]),
    
    % Generate all KV pairs
    AllKVPairs = generate_kv_pairs(ActualCount),
    
    % Split into batches
    Batches = split_into_batches(AllKVPairs, ?BATCH_SIZE),
    
    % Run benchmark
    StartTime = erlang:monotonic_time(microsecond),
    
    lists:foreach(fun(Batch) ->
        ok = bobsled:batch_put(Db, Batch)
    end, Batches),
    
    % Ensure all data is written
    ok = bobsled:flush(Db),
    
    EndTime = erlang:monotonic_time(microsecond),
    Duration = EndTime - StartTime,
    OpsPerSec = (ActualCount * 1000000) / Duration,
    
    io:format("Batch writes: ~.2f ops/sec (~.3f seconds)~n", 
              [OpsPerSec, Duration / 1000000]),
    io:format("Average batch time: ~.3f ms~n", 
              [(Duration / BatchCount) / 1000]),
    
    cleanup_db(Db, DbPath),
    {ops_per_sec, OpsPerSec, Duration}.

%% @doc Benchmark compare-and-swap operations
compare_and_swap_bench(Count) ->
    {Db, DbPath} = setup_db(),
    
    io:format("Running compare-and-swap benchmark (~p operations)...~n", [Count]),
    
    % Initialize counters
    lists:foreach(fun(I) ->
        Key = <<"cas_key_", (integer_to_binary(I))/binary>>,
        ok = bobsled:put(Db, Key, <<"0">>)
    end, lists:seq(1, 1000)),
    
    % Run benchmark - each operation increments a counter
    StartTime = erlang:monotonic_time(microsecond),
    
    lists:foreach(fun(I) ->
        Key = <<"cas_key_", (integer_to_binary(I rem 1000))/binary>>,
        do_cas_increment(Db, Key)
    end, lists:seq(1, Count)),
    
    EndTime = erlang:monotonic_time(microsecond),
    Duration = EndTime - StartTime,
    OpsPerSec = (Count * 1000000) / Duration,
    
    io:format("Compare-and-swap: ~.2f ops/sec (~.3f seconds)~n", 
              [OpsPerSec, Duration / 1000000]),
    
    cleanup_db(Db, DbPath),
    {ops_per_sec, OpsPerSec, Duration}.

%% @doc Benchmark concurrent write operations
concurrent_writes(Count) ->
    {Db, DbPath} = setup_db(),
    
    ProcessCount = 10,
    OpsPerProcess = Count div ProcessCount,
    ActualCount = OpsPerProcess * ProcessCount,
    
    io:format("Running concurrent write benchmark (~p operations across ~p processes)...~n", 
              [ActualCount, ProcessCount]),
    
    Parent = self(),
    
    % Start benchmark
    StartTime = erlang:monotonic_time(microsecond),
    
    % Spawn worker processes
    Pids = lists:map(fun(ProcessId) ->
        spawn_link(fun() ->
            process_flag(trap_exit, true),
            % Each process writes its own key range
            lists:foreach(fun(I) ->
                Key = <<"key_p", (integer_to_binary(ProcessId))/binary, 
                        "_", (integer_to_binary(I))/binary>>,
                Value = <<"value_", (integer_to_binary(I))/binary>>,
                ok = bobsled:put(Db, Key, Value)
            end, lists:seq(1, OpsPerProcess)),
            Parent ! {done, ProcessId}
        end)
    end, lists:seq(1, ProcessCount)),
    
    % Wait for all processes to complete
    lists:foreach(fun(ProcessId) ->
        receive
            {done, ProcessId} -> ok
        after 60000 ->
            error({timeout, ProcessId})
        end
    end, lists:seq(1, ProcessCount)),
    
    % Ensure all data is written
    ok = bobsled:flush(Db),
    
    EndTime = erlang:monotonic_time(microsecond),
    Duration = EndTime - StartTime,
    OpsPerSec = (ActualCount * 1000000) / Duration,
    
    io:format("Concurrent writes: ~.2f ops/sec (~.3f seconds)~n", 
              [OpsPerSec, Duration / 1000000]),
    io:format("Per-process rate: ~.2f ops/sec~n", 
              [OpsPerSec / ProcessCount]),
    
    % Clean up workers
    lists:foreach(fun(Pid) -> unlink(Pid) end, Pids),
    
    cleanup_db(Db, DbPath),
    {ops_per_sec, OpsPerSec, Duration}.

%% @doc Benchmark concurrent read operations
concurrent_reads(Count) ->
    {Db, DbPath} = setup_db(),
    
    ProcessCount = 10,
    OpsPerProcess = Count div ProcessCount,
    ActualCount = OpsPerProcess * ProcessCount,
    
    % First, populate the database
    io:format("Populating database for concurrent read benchmark...~n"),
    KVPairs = generate_kv_pairs(ActualCount),
    populate_db(Db, KVPairs),
    
    io:format("Running concurrent read benchmark (~p operations across ~p processes)...~n", 
              [ActualCount, ProcessCount]),
    
    Parent = self(),
    
    % Partition keys among processes
    Keys = [Key || {Key, _} <- KVPairs],
    KeyChunks = split_into_batches(Keys, OpsPerProcess),
    
    % Start benchmark
    StartTime = erlang:monotonic_time(microsecond),
    
    % Spawn worker processes
    Pids = lists:map(fun({ProcessId, KeyChunk}) ->
        spawn_link(fun() ->
            process_flag(trap_exit, true),
            lists:foreach(fun(Key) ->
                {ok, _Value} = bobsled:get(Db, Key)
            end, KeyChunk),
            Parent ! {done, ProcessId}
        end)
    end, lists:zip(lists:seq(1, ProcessCount), KeyChunks)),
    
    % Wait for all processes to complete
    lists:foreach(fun(ProcessId) ->
        receive
            {done, ProcessId} -> ok
        after 60000 ->
            error({timeout, ProcessId})
        end
    end, lists:seq(1, ProcessCount)),
    
    EndTime = erlang:monotonic_time(microsecond),
    Duration = EndTime - StartTime,
    OpsPerSec = (ActualCount * 1000000) / Duration,
    
    io:format("Concurrent reads: ~.2f ops/sec (~.3f seconds)~n", 
              [OpsPerSec, Duration / 1000000]),
    io:format("Per-process rate: ~.2f ops/sec~n", 
              [OpsPerSec / ProcessCount]),
    
    % Clean up workers
    lists:foreach(fun(Pid) -> unlink(Pid) end, Pids),
    
    cleanup_db(Db, DbPath),
    {ops_per_sec, OpsPerSec, Duration}.

%% @doc Benchmark mixed read/write workload
mixed_workload(Count) ->
    {Db, DbPath} = setup_db(),
    
    ProcessCount = 10,
    OpsPerProcess = Count div ProcessCount,
    ActualCount = OpsPerProcess * ProcessCount,
    
    io:format("Running mixed workload benchmark (50/50 R/W, ~p operations)...~n", 
              [ActualCount]),
    
    Parent = self(),
    
    % Pre-populate some data for reads
    InitialKVPairs = generate_kv_pairs(10000),
    populate_db(Db, InitialKVPairs),
    InitialKeys = [Key || {Key, _} <- InitialKVPairs],
    
    % Start benchmark
    StartTime = erlang:monotonic_time(microsecond),
    
    % Spawn worker processes
    Pids = lists:map(fun(ProcessId) ->
        spawn_link(fun() ->
            process_flag(trap_exit, true),
            random:seed(erlang:phash2([self()]), 
                       erlang:monotonic_time(), 
                       erlang:unique_integer()),
            
            lists:foreach(fun(I) ->
                case random:uniform(2) of
                    1 ->
                        % Write operation
                        Key = <<"mixed_key_p", (integer_to_binary(ProcessId))/binary, 
                                "_", (integer_to_binary(I))/binary>>,
                        Value = <<"value_", (integer_to_binary(I))/binary>>,
                        ok = bobsled:put(Db, Key, Value);
                    2 ->
                        % Read operation
                        Key = lists:nth(random:uniform(length(InitialKeys)), InitialKeys),
                        {ok, _Value} = bobsled:get(Db, Key)
                end
            end, lists:seq(1, OpsPerProcess)),
            
            Parent ! {done, ProcessId}
        end)
    end, lists:seq(1, ProcessCount)),
    
    % Wait for all processes to complete
    lists:foreach(fun(ProcessId) ->
        receive
            {done, ProcessId} -> ok
        after 60000 ->
            error({timeout, ProcessId})
        end
    end, lists:seq(1, ProcessCount)),
    
    EndTime = erlang:monotonic_time(microsecond),
    Duration = EndTime - StartTime,
    OpsPerSec = (ActualCount * 1000000) / Duration,
    
    io:format("Mixed workload: ~.2f ops/sec (~.3f seconds)~n", 
              [OpsPerSec, Duration / 1000000]),
    
    % Clean up workers
    lists:foreach(fun(Pid) -> unlink(Pid) end, Pids),
    
    cleanup_db(Db, DbPath),
    {ops_per_sec, OpsPerSec, Duration}.

%%%===================================================================
%%% Helper Functions
%%%===================================================================

%% @private
setup_db() ->
    DbPath = <<"/tmp/bobsled_bench_", (integer_to_binary(erlang:system_time()))/binary>>,
    {ok, Db} = bobsled:open(DbPath, [
        {mode, fast},
        {cache_capacity, 1024 * 1024 * 1024}, % 1GB cache
        {compression_factor, 0}, % Disable compression for benchmarks
        {flush_every_ms, 10000}  % Flush every 10 seconds
    ]),
    {Db, DbPath}.

%% @private
cleanup_db(Db, DbPath) ->
    ok = bobsled:close(Db),
    os:cmd("rm -rf " ++ binary_to_list(DbPath)).

%% @private
generate_kv_pairs(Count) ->
    [{<<"bench_key_", (integer_to_binary(I))/binary>>,
      <<"benchmark_value_", (integer_to_binary(I))/binary, "_",
        (crypto:strong_rand_bytes(50))/binary>>} 
     || I <- lists:seq(1, Count)].

%% @private
populate_db(Db, KVPairs) ->
    % Use batch operations for faster population
    Batches = split_into_batches(KVPairs, ?BATCH_SIZE),
    lists:foreach(fun(Batch) ->
        ok = bobsled:batch_put(Db, Batch)
    end, Batches),
    ok = bobsled:flush(Db).

%% @private
split_into_batches(List, BatchSize) ->
    split_into_batches(List, BatchSize, []).

split_into_batches([], _BatchSize, Acc) ->
    lists:reverse(Acc);
split_into_batches(List, BatchSize, Acc) ->
    {Batch, Rest} = lists:split(min(BatchSize, length(List)), List),
    split_into_batches(Rest, BatchSize, [Batch | Acc]).

%% @private
do_cas_increment(Db, Key) ->
    case bobsled:get(Db, Key) of
        {ok, OldValue} ->
            OldInt = binary_to_integer(OldValue),
            NewValue = integer_to_binary(OldInt + 1),
            case bobsled:compare_and_swap(Db, Key, OldValue, NewValue) of
                ok -> ok;
                {error, cas_failed} ->
                    % Retry on CAS failure
                    do_cas_increment(Db, Key)
            end;
        not_found ->
            bobsled:put(Db, Key, <<"1">>)
    end.