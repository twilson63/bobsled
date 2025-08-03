#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa erl_src -pa benchmarks -pa priv +P 1000000

main(Args) ->
    % Add paths
    code:add_path("erl_src"),
    code:add_path("benchmarks"),
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
    
    % Compile modules
    io:format("Compiling modules...~n"),
    case compile:file("erl_src/bobsled.erl", [debug_info, {outdir, "erl_src"}]) of
        {ok, _} -> io:format("✓ Compiled bobsled.erl~n");
        {error, Errors, _} -> 
            io:format("✗ Failed to compile bobsled.erl:~n~p~n", [Errors]),
            halt(1)
    end,
    
    case compile:file("benchmarks/bobsled_bench.erl", [debug_info, {outdir, "benchmarks"}]) of
        {ok, _} -> io:format("✓ Compiled bobsled_bench.erl~n");
        {error, Errors2, _} -> 
            io:format("✗ Failed to compile bobsled_bench.erl:~n~p~n", [Errors2]),
            halt(1)
    end,
    
    % Load modules
    code:load_file(bobsled),
    code:load_file(bobsled_bench),
    
    % Parse arguments
    case Args of
        [] ->
            % Run all benchmarks with default settings
            bobsled_bench:run_all();
        ["--count", CountStr] ->
            Count = list_to_integer(CountStr),
            bobsled_bench:run_all(#{count => Count});
        ["--test", Test] ->
            % Run specific test
            Count = 100000,
            run_specific_test(Test, Count);
        ["--test", Test, "--count", CountStr] ->
            Count = list_to_integer(CountStr),
            run_specific_test(Test, Count);
        ["--help"] ->
            print_help();
        _ ->
            io:format("Invalid arguments. Use --help for usage information.~n"),
            halt(1)
    end.

run_specific_test(Test, Count) ->
    io:format("~nRunning ~s benchmark with ~p operations...~n~n", [Test, Count]),
    
    Result = case Test of
        "writes" -> bobsled_bench:sequential_writes(Count);
        "reads" -> bobsled_bench:sequential_reads(Count);
        "batch" -> bobsled_bench:batch_writes(Count);
        "cas" -> bobsled_bench:compare_and_swap_bench(Count);
        "concurrent-writes" -> bobsled_bench:concurrent_writes(Count);
        "concurrent-reads" -> bobsled_bench:concurrent_reads(Count);
        "mixed" -> bobsled_bench:mixed_workload(Count);
        _ ->
            io:format("Unknown test: ~s~n", [Test]),
            io:format("Available tests: writes, reads, batch, cas, concurrent-writes, concurrent-reads, mixed~n"),
            halt(1)
    end,
    
    case Result of
        {ops_per_sec, OpsPerSec, Duration} ->
            io:format("~nResult: ~.2f operations per second~n", [OpsPerSec]),
            io:format("Total duration: ~.3f seconds~n", [Duration / 1000000]);
        _ ->
            io:format("~nResult: ~p~n", [Result])
    end.

print_help() ->
    io:format("Bobsled Benchmark Runner~n"),
    io:format("~nUsage:~n"),
    io:format("  ./run_benchmarks.escript                    Run all benchmarks~n"),
    io:format("  ./run_benchmarks.escript --count N         Run all benchmarks with N operations~n"),
    io:format("  ./run_benchmarks.escript --test TEST       Run specific test~n"),
    io:format("  ./run_benchmarks.escript --test TEST --count N   Run specific test with N operations~n"),
    io:format("~nAvailable tests:~n"),
    io:format("  writes            Sequential write operations~n"),
    io:format("  reads             Sequential read operations~n"),
    io:format("  batch             Batch write operations~n"),
    io:format("  cas               Compare-and-swap operations~n"),
    io:format("  concurrent-writes Concurrent write operations (10 processes)~n"),
    io:format("  concurrent-reads  Concurrent read operations (10 processes)~n"),
    io:format("  mixed             Mixed read/write workload (50/50)~n"),
    io:format("~nExamples:~n"),
    io:format("  ./run_benchmarks.escript~n"),
    io:format("  ./run_benchmarks.escript --count 1000000~n"),
    io:format("  ./run_benchmarks.escript --test writes~n"),
    io:format("  ./run_benchmarks.escript --test reads --count 500000~n").