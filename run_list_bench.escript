#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa erl_src -pa benchmarks -pa priv +P 1000000

%% List operation benchmark runner

main(Args) ->
    % Setup
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
    {ok, _} = compile:file("erl_src/bobsled.erl", [debug_info, {outdir, "erl_src"}]),
    {ok, _} = compile:file("benchmarks/bobsled_list_bench.erl", [debug_info, {outdir, "benchmarks"}]),
    
    % Load modules
    code:load_file(bobsled),
    code:load_file(bobsled_list_bench),
    
    % Parse arguments and run
    case Args of
        [] ->
            bobsled_list_bench:run_all();
        ["--nodes", NodesStr] ->
            Nodes = list_to_integer(NodesStr),
            bobsled_list_bench:run_all(#{nodes => Nodes});
        ["--demo"] ->
            bobsled_list_bench:demo_file_system();
        ["--scaling"] ->
            bobsled_list_bench:benchmark_list_scaling();
        ["--help"] ->
            print_help();
        _ ->
            io:format("Invalid arguments. Use --help for usage.~n"),
            halt(1)
    end.

print_help() ->
    io:format("Bobsled List Operation Benchmark~n"),
    io:format("~nUsage:~n"),
    io:format("  ./run_list_bench.escript              Run all benchmarks (default: 10000 nodes)~n"),
    io:format("  ./run_list_bench.escript --nodes N    Run with N total nodes~n"),
    io:format("  ./run_list_bench.escript --demo       Run file system demo only~n"),
    io:format("  ./run_list_bench.escript --scaling    Run scaling analysis only~n"),
    io:format("  ./run_list_bench.escript --help       Show this help~n"),
    io:format("~nExamples:~n"),
    io:format("  ./run_list_bench.escript~n"),
    io:format("  ./run_list_bench.escript --nodes 50000~n"),
    io:format("  ./run_list_bench.escript --demo~n").