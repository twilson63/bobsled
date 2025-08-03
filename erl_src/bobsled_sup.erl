%%%-------------------------------------------------------------------
%%% @doc Bobsled Supervisor
%%%
%%% Example supervisor for managing a bobsled database instance within
%%% an OTP application. Demonstrates proper resource management and
%%% supervision strategies for NIF-based database systems.
%%%
%%% @author Bobsled Team
%%% @copyright 2025 Bobsled Project
%%% @end
%%%-------------------------------------------------------------------
-module(bobsled_sup).
-behaviour(supervisor).

%% API
-export([start_link/2, stop/0]).

%% Supervisor callbacks
-export([init/1]).

%% Supervisor name
-define(SERVER, ?MODULE).

%%%===================================================================
%%% API Functions
%%%===================================================================

%% @doc Start the supervisor with database path and options.
%%
%% == Example ==
%% ```
%% Options = [
%%     {cache_capacity, 1024 * 1024 * 1024},  % 1GB
%%     {mode, safe},
%%     {flush_every_ms, 1000}
%% ],
%% {ok, Pid} = bobsled_sup:start_link(<<"/var/db/myapp">>, Options).
%% '''
%%
%% @param DbPath Path to the database directory
%% @param DbOptions Database configuration options
%% @returns `{ok, Pid}' on success, `{error, Reason}' on failure
-spec start_link(DbPath :: binary(), DbOptions :: [bobsled:open_option()]) ->
    {ok, pid()} | {error, term()}.
start_link(DbPath, DbOptions) ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, {DbPath, DbOptions}).

%% @doc Stop the supervisor and close the database.
-spec stop() -> ok.
stop() ->
    case whereis(?SERVER) of
        undefined ->
            ok;
        Pid ->
            exit(Pid, shutdown),
            ok
    end.

%%%===================================================================
%%% Supervisor Callbacks
%%%===================================================================

%% @doc Initialize the supervisor.
%%
%% Creates a supervision tree with a bobsled_server worker that manages
%% the database connection. Uses one_for_one strategy to restart the
%% server if it crashes while keeping the database handle valid.
%%
%% @private
-spec init({binary(), [bobsled:open_option()]}) ->
    {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init({DbPath, DbOptions}) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 5,        % Maximum 5 restarts
        period => 10           % Within 10 seconds
    },
    
    % Child specification for the database server
    ChildSpecs = [
        #{
            id => bobsled_server,
            start => {bobsled_server, start_link, [DbPath, DbOptions]},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [bobsled_server]
        }
    ],
    
    {ok, {SupFlags, ChildSpecs}}.