#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa erl_src -pa priv

% Define NIF stubs
open_db(_Path, _Options) -> erlang:nif_error(nif_not_loaded).
close_db(_Handle) -> erlang:nif_error(nif_not_loaded).
put(_Handle, _Key, _Value) -> erlang:nif_error(nif_not_loaded).
get(_Handle, _Key) -> erlang:nif_error(nif_not_loaded).
delete(_Handle, _Key) -> erlang:nif_error(nif_not_loaded).
flush(_Handle) -> erlang:nif_error(nif_not_loaded).
size_on_disk(_Handle) -> erlang:nif_error(nif_not_loaded).
compare_and_swap(_Handle, _Key, _Old, _New) -> erlang:nif_error(nif_not_loaded).
list(_Handle, _Prefix) -> erlang:nif_error(nif_not_loaded).
fold(_Handle, _Prefix) -> erlang:nif_error(nif_not_loaded).
batch_put(_Handle, _KVPairs) -> erlang:nif_error(nif_not_loaded).
transaction(_Handle) -> erlang:nif_error(nif_not_loaded).

main(_) ->
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
    
    % Try to load the NIF directly
    io:format("Attempting to load NIF from priv/bobsled...~n"),
    case erlang:load_nif("priv/bobsled", 0) of
        ok -> 
            io:format("✓ NIF loaded successfully!~n"),
            test_basic_operations();
        {error, {reload, _}} ->
            io:format("✓ NIF already loaded~n"),
            test_basic_operations();
        {error, Reason} ->
            io:format("✗ Failed to load NIF: ~p~n", [Reason]),
            
            % List available functions
            io:format("~nChecking what functions are exported by the NIF...~n"),
            os:cmd("nm -gU priv/bobsled.so | grep -E 'T _?(open_db|put|get|delete|flush|close_db|compare_and_swap|list|fold|batch_put|transaction|size_on_disk)' || echo 'No matching symbols found'") ++ io_lib:nl(),
            halt(1)
    end.

test_basic_operations() ->
    io:format("~nTesting basic NIF operations...~n"),
    
    % Test opening a database
    try
        TestDir = <<"/tmp/bobsled_nif_test">>,
        os:cmd("rm -rf " ++ binary_to_list(TestDir)),
        
        % Call open_db directly
        case open_db(binary_to_list(TestDir), []) of
            {ok, DbHandle} ->
                io:format("✓ open_db succeeded, got handle: ~p~n", [DbHandle]),
                
                % Test put
                case put(DbHandle, <<"test_key">>, <<"test_value">>) of
                    ok ->
                        io:format("✓ put succeeded~n");
                    Error ->
                        io:format("✗ put failed: ~p~n", [Error])
                end,
                
                % Test get
                case get(DbHandle, <<"test_key">>) of
                    {ok, <<"test_value">>} ->
                        io:format("✓ get succeeded~n");
                    not_found ->
                        io:format("✗ get returned not_found~n");
                    Error2 ->
                        io:format("✗ get failed: ~p~n", [Error2])
                end,
                
                % Test close
                case close_db(DbHandle) of
                    ok ->
                        io:format("✓ close_db succeeded~n");
                    Error3 ->
                        io:format("✗ close_db failed: ~p~n", [Error3])
                end;
            Error ->
                io:format("✗ open_db failed: ~p~n", [Error])
        end
    catch
        error:undef ->
            io:format("✗ NIF functions are not available (undef error)~n"),
            io:format("This suggests the NIF loaded but function names don't match~n")
    end.