%%%-------------------------------------------------------------------
%%% @doc Bobsled Usage Examples
%%%
%%% This module demonstrates how to use the bobsled NIF wrapper for
%%% various database operations including basic CRUD, transactions,
%%% batch operations, and iteration patterns.
%%%
%%% @author Bobsled Team
%%% @copyright 2025 Bobsled Project
%%% @end
%%%-------------------------------------------------------------------
-module(bobsled_example).

-export([
    basic_operations/0,
    transaction_example/0,
    batch_operations/0,
    iteration_examples/0,
    config_store_example/0,
    user_session_example/0
]).

%%%===================================================================
%%% Basic Operations Example
%%%===================================================================

%% @doc Demonstrates basic put, get, and delete operations.
-spec basic_operations() -> ok.
basic_operations() ->
    % Open database
    {ok, Db} = bobsled:open(<<"/tmp/bobsled_basic">>, [
        {mode, fast},
        {cache_capacity, 1024 * 1024 * 100}  % 100MB cache
    ]),
    
    % Put some data
    ok = bobsled:put(Db, <<"user:alice">>, <<"Alice Smith">>),
    ok = bobsled:put(Db, <<"user:bob">>, <<"Bob Jones">>),
    ok = bobsled:put(Db, <<"user:charlie">>, <<"Charlie Brown">>),
    
    % Get data back
    {ok, <<"Alice Smith">>} = bobsled:get(Db, <<"user:alice">>),
    {ok, <<"Bob Jones">>} = bobsled:get(Db, <<"user:bob">>),
    not_found = bobsled:get(Db, <<"user:nonexistent">>),
    
    % Delete a user
    ok = bobsled:delete(Db, <<"user:bob">>),
    not_found = bobsled:get(Db, <<"user:bob">>),
    
    % Close database
    ok = bobsled:close(Db),
    
    io:format("Basic operations completed successfully~n").

%%%===================================================================
%%% Transaction Example
%%%===================================================================

%% @doc Demonstrates atomic transactions for account transfers.
-spec transaction_example() -> ok.
transaction_example() ->
    % Open database
    {ok, Db} = bobsled:open(<<"/tmp/bobsled_transactions">>, [
        {mode, safe}  % Use safe mode for financial data
    ]),
    
    % Initialize accounts
    ok = bobsled:put(Db, <<"account:alice">>, <<"1000">>),
    ok = bobsled:put(Db, <<"account:bob">>, <<"500">>),
    
    % Transfer 100 from alice to bob atomically
    TransferFun = fun(TxnHandle) ->
        case bobsled:get(TxnHandle, <<"account:alice">>) of
            {ok, AliceBal} ->
                case bobsled:get(TxnHandle, <<"account:bob">>) of
                    {ok, BobBal} ->
                        AliceAmount = binary_to_integer(AliceBal),
                        BobAmount = binary_to_integer(BobBal),
                        
                        if AliceAmount >= 100 ->
                            NewAlice = AliceAmount - 100,
                            NewBob = BobAmount + 100,
                            
                            ok = bobsled:put(TxnHandle, <<"account:alice">>, 
                                           integer_to_binary(NewAlice)),
                            ok = bobsled:put(TxnHandle, <<"account:bob">>, 
                                           integer_to_binary(NewBob)),
                            
                            {ok, {transfer_complete, NewAlice, NewBob}};
                        true ->
                            {error, insufficient_funds}
                        end;
                    not_found ->
                        {error, bob_account_not_found}
                end;
            not_found ->
                {error, alice_account_not_found}
        end
    end,
    
    % Execute transaction
    case bobsled:transaction(Db, TransferFun) of
        {ok, {transfer_complete, AliceFinal, BobFinal}} ->
            io:format("Transfer successful! Alice: ~p, Bob: ~p~n", 
                     [AliceFinal, BobFinal]);
        {error, Reason} ->
            io:format("Transfer failed: ~p~n", [Reason])
    end,
    
    % Verify final balances
    {ok, AliceBalance} = bobsled:get(Db, <<"account:alice">>),
    {ok, BobBalance} = bobsled:get(Db, <<"account:bob">>),
    io:format("Final balances - Alice: ~s, Bob: ~s~n", 
              [AliceBalance, BobBalance]),
    
    ok = bobsled:close(Db),
    io:format("Transaction example completed successfully~n").

%%%===================================================================
%%% Batch Operations Example
%%%===================================================================

%% @doc Demonstrates batch put operations for bulk data loading.
-spec batch_operations() -> ok.
batch_operations() ->
    % Open database
    {ok, Db} = bobsled:open(<<"/tmp/bobsled_batch">>, []),
    
    % Prepare a large batch of user data
    Users = [
        {<<"user:001">>, <<"John Doe">>},
        {<<"user:002">>, <<"Jane Smith">>},
        {<<"user:003">>, <<"Mike Johnson">>},
        {<<"user:004">>, <<"Sarah Wilson">>},
        {<<"user:005">>, <<"Tom Anderson">>}
    ],
    
    % Insert all users atomically
    ok = bobsled:batch_put(Db, Users),
    
    % Verify all users were inserted
    lists:foreach(fun({Key, ExpectedValue}) ->
        {ok, ExpectedValue} = bobsled:get(Db, Key)
    end, Users),
    
    % Get database size
    {ok, Size} = bobsled:size_on_disk(Db),
    io:format("Database size after batch insert: ~p bytes~n", [Size]),
    
    ok = bobsled:close(Db),
    io:format("Batch operations completed successfully~n").

%%%===================================================================
%%% Iteration Examples
%%%===================================================================

%% @doc Demonstrates list and fold operations for data iteration.
-spec iteration_examples() -> ok.
iteration_examples() ->
    % Open database
    {ok, Db} = bobsled:open(<<"/tmp/bobsled_iteration">>, []),
    
    % Insert hierarchical data
    ConfigData = [
        {<<"config/database/host">>, <<"localhost">>},
        {<<"config/database/port">>, <<"5432">>},
        {<<"config/database/name">>, <<"myapp">>},
        {<<"config/server/port">>, <<"8080">>},
        {<<"config/server/workers">>, <<"10">>},
        {<<"config/logging/level">>, <<"info">>},
        {<<"config/logging/file">>, <<"/var/log/app.log">>}
    ],
    
    ok = bobsled:batch_put(Db, ConfigData),
    
    % List all config keys
    {ok, AllConfigKeys} = bobsled:list(Db, <<"config/">>),
    io:format("All config keys: ~p~n", [AllConfigKeys]),
    
    % List only database config keys
    {ok, DbKeys} = bobsled:list(Db, <<"config/database/">>),
    io:format("Database config keys: ~p~n", [DbKeys]),
    
    % Fold to collect all database configuration
    CollectDbConfig = fun(Key, Value, Acc) ->
        case binary:match(Key, <<"config/database/">>) of
            {0, _} ->
                % Remove prefix to get just the setting name
                SettingName = binary:part(Key, 16, byte_size(Key) - 16),
                [{SettingName, Value} | Acc];
            nomatch ->
                Acc
        end
    end,
    
    {ok, DbConfig} = bobsled:fold(Db, CollectDbConfig, [], <<"config/database/">>),
    io:format("Database configuration: ~p~n", [DbConfig]),
    
    % Count total configuration entries
    CountFun = fun(_Key, _Value, Count) -> Count + 1 end,
    {ok, TotalConfigs} = bobsled:fold(Db, CountFun, 0, <<"config/">>),
    io:format("Total configuration entries: ~p~n", [TotalConfigs]),
    
    ok = bobsled:close(Db),
    io:format("Iteration examples completed successfully~n").

%%%===================================================================
%%% Config Store Example
%%%===================================================================

%% @doc Example of using bobsled as a configuration store.
-spec config_store_example() -> ok.
config_store_example() ->
    % Open database
    {ok, Db} = bobsled:open(<<"/tmp/bobsled_config">>, [
        {mode, safe},  % Ensure config changes are persisted
        {compression_factor, 6}  % Compress config data
    ]),
    
    % Store application configuration
    AppConfig = [
        {<<"app/name">>, <<"MyWebApp">>},
        {<<"app/version">>, <<"1.2.3">>},
        {<<"app/environment">>, <<"production">>},
        {<<"database/url">>, <<"postgresql://localhost:5432/myapp">>},
        {<<"database/pool_size">>, <<"10">>},
        {<<"cache/redis_url">>, <<"redis://localhost:6379">>},
        {<<"cache/ttl_seconds">>, <<"3600">>},
        {<<"auth/jwt_secret">>, <<"super-secret-key">>},
        {<<"auth/token_expiry">>, <<"86400">>}
    ],
    
    ok = bobsled:batch_put(Db, AppConfig),
    
    % Implement a config reader function
    GetConfig = fun(Section) ->
        Prefix = <<Section/binary, "/">>,
        {ok, SectionConfig} = bobsled:fold(Db, 
            fun(Key, Value, Acc) ->
                % Remove section prefix from key
                Setting = binary:part(Key, byte_size(Prefix), 
                                    byte_size(Key) - byte_size(Prefix)),
                [{Setting, Value} | Acc]
            end, [], Prefix),
        lists:reverse(SectionConfig)
    end,
    
    % Read different configuration sections
    DatabaseConfig = GetConfig(<<"database">>),
    CacheConfig = GetConfig(<<"cache">>),
    AuthConfig = GetConfig(<<"auth">>),
    
    io:format("Database config: ~p~n", [DatabaseConfig]),
    io:format("Cache config: ~p~n", [CacheConfig]),
    io:format("Auth config: ~p~n", [AuthConfig]),
    
    % Update a single config value using compare-and-swap
    OldPoolSize = <<"10">>,
    NewPoolSize = <<"15">>,
    case bobsled:compare_and_swap(Db, <<"database/pool_size">>, 
                                 OldPoolSize, NewPoolSize) of
        ok ->
            io:format("Pool size updated from ~s to ~s~n", 
                     [OldPoolSize, NewPoolSize]);
        {error, cas_failed} ->
            io:format("Pool size was modified by another process~n")
    end,
    
    % Flush to ensure config is persisted
    ok = bobsled:flush(Db),
    
    ok = bobsled:close(Db),
    io:format("Config store example completed successfully~n").

%%%===================================================================
%%% User Session Example
%%%===================================================================

%% @doc Example of using bobsled for user session management.
-spec user_session_example() -> ok.
user_session_example() ->
    % Open database
    {ok, Db} = bobsled:open(<<"/tmp/bobsled_sessions">>, [
        {mode, fast},  % Fast mode for session data
        {flush_every_ms, 1000}  % Flush every second
    ]),
    
    % Create user sessions
    Sessions = [
        {<<"session:abc123">>, term_to_binary(#{
            user_id => <<"user:alice">>,
            login_time => erlang:system_time(second),
            ip_address => <<"192.168.1.100">>,
            user_agent => <<"Mozilla/5.0...">>
        })},
        {<<"session:def456">>, term_to_binary(#{
            user_id => <<"user:bob">>,
            login_time => erlang:system_time(second),
            ip_address => <<"10.0.0.5">>,
            user_agent => <<"Chrome/91.0...">>
        })}
    ],
    
    ok = bobsled:batch_put(Db, Sessions),
    
    % Retrieve and deserialize a session
    {ok, SessionData} = bobsled:get(Db, <<"session:abc123">>),
    Session = binary_to_term(SessionData),
    io:format("Session data: ~p~n", [Session]),
    
    % Count active sessions
    CountSessions = fun(<<"session:", _/binary>>, _Value, Count) ->
        Count + 1;
    (_, _, Count) ->
        Count
    end,
    
    {ok, ActiveSessions} = bobsled:fold(Db, CountSessions, 0, <<"session:">>),
    io:format("Active sessions: ~p~n", [ActiveSessions]),
    
    % Clean up expired sessions (demo - normally done by a background process)
    CurrentTime = erlang:system_time(second),
    ExpiryTime = 3600,  % 1 hour
    
    CleanupExpired = fun(Key, Value, Deleted) ->
        SessionMap = binary_to_term(Value),
        LoginTime = maps:get(login_time, SessionMap),
        case CurrentTime - LoginTime > ExpiryTime of
            true ->
                ok = bobsled:delete(Db, Key),
                [Key | Deleted];
            false ->
                Deleted
        end
    end,
    
    {ok, DeletedSessions} = bobsled:fold(Db, CleanupExpired, [], <<"session:">>),
    io:format("Deleted expired sessions: ~p~n", [DeletedSessions]),
    
    ok = bobsled:close(Db),
    io:format("User session example completed successfully~n").

%%%===================================================================
%%% Private Helper Functions
%%%===================================================================

%% Additional helper functions would go here...