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
    
    % Test database size
    TestDir = <<"/tmp/bobsled_size_test">>,
    os:cmd("rm -rf " ++ binary_to_list(TestDir)),
    
    {ok, Db} = bobsled:open(TestDir, [{mode, fast}]),
    
    % Check initial size
    {ok, Size0} = bobsled:size_on_disk(Db),
    io:format("Initial size: ~p bytes~n", [Size0]),
    
    % Add a 10KB value
    LargeValue = binary:copy(<<"x">>, 10000),
    ok = bobsled:put(Db, <<"large_key">>, LargeValue),
    ok = bobsled:flush(Db),
    
    {ok, Size1} = bobsled:size_on_disk(Db),
    io:format("After 10KB value: ~p bytes (delta: ~p)~n", [Size1, Size1 - Size0]),
    
    % Add 50 x 1KB values
    LargeBatch = [{iolist_to_binary([<<"large_">>, integer_to_binary(N)]), 
                  binary:copy(<<"y">>, 1000)} || N <- lists:seq(1, 50)],
    ok = bobsled:batch_put(Db, LargeBatch),
    ok = bobsled:flush(Db),
    
    {ok, Size2} = bobsled:size_on_disk(Db),
    io:format("After 50x1KB batch: ~p bytes (delta: ~p)~n", [Size2, Size2 - Size1]),
    io:format("Total size: ~p bytes~n", [Size2]),
    
    ok = bobsled:close(Db),
    os:cmd("rm -rf " ++ binary_to_list(TestDir)).