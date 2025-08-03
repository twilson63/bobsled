#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa erl_src -pa priv

main(_) ->
    % Add paths
    code:add_path("erl_src"),
    code:add_path("priv"),
    
    % Create priv directory and copy NIF
    file:make_dir("priv"),
    os:cmd("cp -f target/release/libbobsled.dylib priv/bobsled.so 2>/dev/null || cp -f target/debug/libbobsled.dylib priv/bobsled.so"),
    
    % Compile and load
    compile:file("erl_src/bobsled.erl", [debug_info, {outdir, "erl_src"}]),
    code:load_file(bobsled),
    
    % Test CAS
    TestDir = <<"/tmp/bobsled_cas_test">>,
    os:cmd("rm -rf " ++ binary_to_list(TestDir)),
    
    {ok, Db} = bobsled:open(TestDir, [{mode, fast}]),
    
    % Test CAS on non-existent key
    io:format("~nTest 1: CAS on non-existent key~n"),
    Result1 = bobsled:compare_and_swap(Db, <<"counter">>, not_found, <<"1">>),
    io:format("Result: ~p (expected: ok)~n", [Result1]),
    
    % Verify value
    GetResult1 = bobsled:get(Db, <<"counter">>),
    io:format("Get result: ~p (expected: {ok, <<\"1\">>})~n", [GetResult1]),
    
    % Test successful CAS
    io:format("~nTest 2: Successful CAS~n"),
    Result2 = bobsled:compare_and_swap(Db, <<"counter">>, <<"1">>, <<"2">>),
    io:format("Result: ~p (expected: ok)~n", [Result2]),
    
    % Verify value
    GetResult2 = bobsled:get(Db, <<"counter">>),
    io:format("Get result: ~p (expected: {ok, <<\"2\">>})~n", [GetResult2]),
    
    % Test failed CAS
    io:format("~nTest 3: Failed CAS (wrong old value)~n"),
    Result3 = bobsled:compare_and_swap(Db, <<"counter">>, <<"1">>, <<"3">>),
    io:format("Result: ~p (expected: {error, cas_failed})~n", [Result3]),
    
    % Verify value unchanged
    GetResult3 = bobsled:get(Db, <<"counter">>),
    io:format("Get result: ~p (expected: {ok, <<\"2\">>})~n", [GetResult3]),
    
    ok = bobsled:close(Db),
    os:cmd("rm -rf " ++ binary_to_list(TestDir)).