#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa erl_src -pa priv

main(_) ->
    % Add the erl_src directory to the code path
    code:add_path("erl_src"),
    code:add_path("priv"),
    
    % Set environment variable for NIF loading
    case os:type() of
        {unix, darwin} ->
            os:putenv("DYLD_LIBRARY_PATH", "target/release:target/debug");
        {unix, _} ->
            os:putenv("LD_LIBRARY_PATH", "target/release:target/debug");
        _ ->
            ok
    end,
    
    % Check if the NIF library exists
    NifPath = case os:type() of
        {unix, darwin} -> "target/release/libbobsled.dylib";
        {unix, _} -> "target/release/libbobsled.so";
        {win32, _} -> "target/release/bobsled.dll"
    end,
    
    case filelib:is_file(NifPath) of
        true ->
            io:format("Found NIF library at: ~s~n", [NifPath]);
        false ->
            io:format("Warning: NIF library not found at: ~s~n", [NifPath]),
            io:format("Trying debug build...~n"),
            DebugPath = case os:type() of
                {unix, darwin} -> "target/debug/libbobsled.dylib";
                {unix, _} -> "target/debug/libbobsled.so";
                {win32, _} -> "target/debug/bobsled.dll"
            end,
            case filelib:is_file(DebugPath) of
                true -> io:format("Found debug NIF at: ~s~n", [DebugPath]);
                false -> io:format("Error: No NIF library found!~n"), halt(1)
            end
    end,
    
    % Create priv directory and copy NIF if needed
    file:make_dir("priv"),
    case os:type() of
        {unix, darwin} ->
            os:cmd("cp -f target/release/libbobsled.dylib priv/bobsled.so 2>/dev/null || cp -f target/debug/libbobsled.dylib priv/bobsled.so");
        {unix, _} ->
            os:cmd("cp -f target/release/libbobsled.so priv/bobsled.so 2>/dev/null || cp -f target/debug/libbobsled.so priv/bobsled.so");
        _ ->
            ok
    end,
    
    % Compile the modules
    io:format("~nCompiling Erlang modules...~n"),
    case compile:file("erl_src/bobsled.erl", [debug_info, {outdir, "erl_src"}]) of
        {ok, _} -> io:format("✓ Compiled bobsled.erl~n");
        {error, Errors, _} -> 
            io:format("✗ Failed to compile bobsled.erl:~n~p~n", [Errors]),
            halt(1)
    end,
    
    case compile:file("erl_src/bobsled_tests.erl", [debug_info, {outdir, "erl_src"}, {d, 'TEST'}]) of
        {ok, _} -> io:format("✓ Compiled bobsled_tests.erl~n");
        {error, Errors2, _} -> 
            io:format("✗ Failed to compile bobsled_tests.erl:~n~p~n", [Errors2]),
            halt(1)
    end,
    
    % Load the modules
    io:format("~nLoading modules...~n"),
    case code:load_file(bobsled) of
        {module, bobsled} -> io:format("✓ Loaded bobsled module~n");
        {error, Reason} -> 
            io:format("✗ Failed to load bobsled module: ~p~n", [Reason]),
            halt(1)
    end,
    
    case code:load_file(bobsled_tests) of
        {module, bobsled_tests} -> io:format("✓ Loaded bobsled_tests module~n");
        {error, Reason2} -> 
            io:format("✗ Failed to load bobsled_tests module: ~p~n", [Reason2]),
            halt(1)
    end,
    
    % Run the tests
    io:format("~nRunning EUnit tests...~n~n"),
    case eunit:test(bobsled_tests, [verbose]) of
        ok ->
            io:format("~n✅ All tests passed!~n"),
            halt(0);
        error ->
            io:format("~n❌ Some tests failed!~n"),
            halt(1)
    end.