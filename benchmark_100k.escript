#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa erl_src -pa priv

%% Simple benchmark script for 100,000 writes and reads

main(_) ->
    % Setup
    code:add_path("erl_src"),
    code:add_path("priv"),
    
    % Create priv directory and copy NIF
    file:make_dir("priv"),
    case os:type() of
        {unix, darwin} ->
            os:cmd("cp -f target/release/libbobsled.dylib priv/bobsled.so 2>/dev/null || cp -f target/debug/libbobsled.dylib priv/bobsled.so");
        {unix, _} ->
            os:cmd("cp -f target/release/libbobsled.so priv/bobsled.so 2>/dev/null || cp -f target/debug/libbobsled.so priv/bobsled.so");
        _ ->
            ok
    end,
    
    % Compile and load
    {ok, _} = compile:file("erl_src/bobsled.erl", [debug_info, {outdir, "erl_src"}]),
    code:load_file(bobsled),
    
    % Configuration
    Count = 100000,
    DbPath = <<"/tmp/bobsled_100k_bench">>,
    
    % Clean up any existing database
    os:cmd("rm -rf " ++ binary_to_list(DbPath)),
    
    % Open database with performance settings
    {ok, Db} = bobsled:open(DbPath, [
        {mode, fast},
        {cache_capacity, 512 * 1024 * 1024}, % 512MB cache
        {compression_factor, 0}, % No compression for max speed
        {flush_every_ms, 30000}  % Flush every 30 seconds
    ]),
    
    io:format("~n╔════════════════════════════════════════════╗~n"),
    io:format("║       Bobsled 100K Operations Benchmark    ║~n"),
    io:format("╚════════════════════════════════════════════╝~n~n"),
    
    %% Write Benchmark
    io:format("📝 WRITE BENCHMARK (100,000 operations)~n"),
    io:format("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━~n"),
    
    % Generate test data
    WriteData = [{<<"key_", (integer_to_binary(I))/binary>>,
                  <<"value_", (integer_to_binary(I))/binary, "_data_",
                    (crypto:strong_rand_bytes(64))/binary>>} 
                 || I <- lists:seq(1, Count)],
    
    % Warm up
    io:format("Warming up...~n"),
    lists:foreach(fun({K, V}) ->
        bobsled:put(Db, <<K/binary, "_warmup">>, V)
    end, lists:sublist(WriteData, 1000)),
    
    % Run write benchmark
    io:format("Running write benchmark...~n"),
    WriteStart = erlang:monotonic_time(microsecond),
    
    lists:foreach(fun({Key, Value}) ->
        ok = bobsled:put(Db, Key, Value)
    end, WriteData),
    
    WriteEnd = erlang:monotonic_time(microsecond),
    WriteDuration = WriteEnd - WriteStart,
    WriteOpsPerSec = (Count * 1000000) / WriteDuration,
    
    io:format("~n✅ Write Results:~n"),
    io:format("   Operations:     ~B~n", [Count]),
    io:format("   Duration:       ~.3f seconds~n", [WriteDuration / 1000000]),
    io:format("   Throughput:     ~.2f writes/second~n", [WriteOpsPerSec]),
    io:format("   Latency:        ~.2f μs/write~n", [WriteDuration / Count]),
    
    % Ensure data is written
    bobsled:flush(Db),
    
    %% Read Benchmark
    io:format("~n📖 READ BENCHMARK (100,000 operations)~n"),
    io:format("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━~n"),
    
    % Extract keys for reading
    Keys = [Key || {Key, _} <- WriteData],
    
    % Warm up CPU cache
    io:format("Warming up...~n"),
    lists:foreach(fun(Key) ->
        {ok, _} = bobsled:get(Db, Key)
    end, lists:sublist(Keys, 1000)),
    
    % Run read benchmark
    io:format("Running read benchmark...~n"),
    ReadStart = erlang:monotonic_time(microsecond),
    
    lists:foreach(fun(Key) ->
        {ok, _Value} = bobsled:get(Db, Key)
    end, Keys),
    
    ReadEnd = erlang:monotonic_time(microsecond),
    ReadDuration = ReadEnd - ReadStart,
    ReadOpsPerSec = (Count * 1000000) / ReadDuration,
    
    io:format("~n✅ Read Results:~n"),
    io:format("   Operations:     ~B~n", [Count]),
    io:format("   Duration:       ~.3f seconds~n", [ReadDuration / 1000000]),
    io:format("   Throughput:     ~.2f reads/second~n", [ReadOpsPerSec]),
    io:format("   Latency:        ~.2f μs/read~n", [ReadDuration / Count]),
    
    %% Summary
    io:format("~n📊 SUMMARY~n"),
    io:format("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━~n"),
    io:format("   Write throughput: ~.2f ops/sec~n", [WriteOpsPerSec]),
    io:format("   Read throughput:  ~.2f ops/sec~n", [ReadOpsPerSec]),
    io:format("   Read/Write ratio: ~.2fx~n", [ReadOpsPerSec / WriteOpsPerSec]),
    
    % Check database size
    {ok, DbSize} = bobsled:size_on_disk(Db),
    io:format("   Database size:    ~.2f MB~n", [DbSize / (1024 * 1024)]),
    io:format("   Bytes per entry:  ~.2f~n", [DbSize / Count]),
    
    % Cleanup
    ok = bobsled:close(Db),
    os:cmd("rm -rf " ++ binary_to_list(DbPath)),
    
    io:format("~n✨ Benchmark complete!~n~n").