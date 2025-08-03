#!/usr/bin/env escript
%%%-------------------------------------------------------------------
%%% @doc Comprehensive test runner for bobsled test suites
%%%
%%% This script provides convenient ways to run different test suites:
%%% - Full test suite (all tests)
%%% - Individual test suites
%%% - Property-based tests only
%%% - Performance benchmarks
%%% - Integration tests
%%% - Crash/recovery tests
%%% - Test coverage analysis
%%%
%%% Usage:
%%%   ./test/run_tests.erl [options]
%%%
%%% Options:
%%%   all              - Run all test suites (default)
%%%   unit             - Run unit tests only
%%%   prop             - Run property-based tests only
%%%   perf             - Run performance tests only
%%%   integration      - Run integration tests only
%%%   crash            - Run crash/recovery tests only
%%%   coverage         - Run tests with coverage analysis
%%%   quick            - Run quick smoke tests
%%%   help             - Show this help
%%%
%%% @author Bobsled Test Team
%%% @copyright 2025 Bobsled Project
%%% @end
%%%-------------------------------------------------------------------

-mode(compile).

main([]) ->
    main(["all"]);
main(["help"]) ->
    show_help();
main(Args) ->
    try
        setup_environment(),
        run_tests(Args),
        cleanup_environment(),
        halt(0)
    catch
        error:Reason ->
            io:format("Test run failed: ~p~n", [Reason]),
            halt(1);
        _:Error ->
            io:format("Unexpected error: ~p~n", [Error]),
            halt(1)
    end.

%% @doc Show help information
show_help() ->
    io:format("~s~n", [
        "Bobsled Test Runner\n"
        "\n"
        "Usage: ./test/run_tests.erl [options]\n"
        "\n"
        "Options:\n"
        "  all              - Run all test suites (default)\n"
        "  unit             - Run unit tests only (bobsled_SUITE)\n"
        "  prop             - Run property-based tests only\n"
        "  perf             - Run performance benchmarks\n"
        "  integration      - Run integration tests\n"
        "  crash            - Run crash/recovery tests\n"
        "  coverage         - Run all tests with coverage analysis\n"
        "  quick            - Run quick smoke tests\n"
        "  help             - Show this help\n"
        "\n"
        "Examples:\n"
        "  ./test/run_tests.erl                 # Run all tests\n"
        "  ./test/run_tests.erl unit           # Run unit tests only\n"
        "  ./test/run_tests.erl prop perf      # Run property and performance tests\n"
        "  ./test/run_tests.erl coverage       # Run all tests with coverage\n"
        "\n"
        "Output:\n"
        "  Test results are written to logs/ directory\n"
        "  Coverage reports are written to _build/test/cover/\n"
    ]).

%% @doc Setup test environment
setup_environment() ->
    io:format("Setting up test environment...~n"),
    
    % Ensure log directories exist
    ok = filelib:ensure_dir("logs/"),
    ok = filelib:ensure_dir("logs/perf/"),
    ok = filelib:ensure_dir("logs/integration/"),
    ok = filelib:ensure_dir("logs/crash/"),
    
    % Clean up old test files
    cleanup_test_files(),
    
    io:format("Environment setup complete.~n").

%% @doc Cleanup test environment
cleanup_environment() ->
    io:format("Cleaning up test environment...~n"),
    cleanup_test_files(),
    io:format("Cleanup complete.~n").

%% @doc Main test runner logic
run_tests(Args) ->
    io:format("Starting test run with options: ~p~n", [Args]),
    
    StartTime = erlang:monotonic_time(millisecond),
    Results = execute_test_suites(Args),
    EndTime = erlang:monotonic_time(millisecond),
    
    Duration = (EndTime - StartTime) / 1000,
    
    io:format("~n=== TEST RESULTS SUMMARY ===~n"),
    io:format("Total duration: ~.2f seconds~n", [Duration]),
    
    report_results(Results),
    
    % Check if all tests passed
    case all_tests_passed(Results) of
        true ->
            io:format("~n✓ ALL TESTS PASSED~n"),
            ok;
        false ->
            io:format("~n✗ SOME TESTS FAILED~n"),
            error(tests_failed)
    end.

%% @doc Execute test suites based on arguments
execute_test_suites(Args) ->
    TestPlan = create_test_plan(Args),
    execute_test_plan(TestPlan).

%% @doc Create test execution plan
create_test_plan(Args) ->
    case Args of
        ["all"] -> 
            [unit_tests, property_tests, perf_tests, integration_tests, crash_tests];
        ["unit"] -> 
            [unit_tests];
        ["prop"] -> 
            [property_tests];
        ["perf"] -> 
            [perf_tests];
        ["integration"] -> 
            [integration_tests];
        ["crash"] -> 
            [crash_tests];
        ["coverage"] -> 
            [coverage_all];
        ["quick"] -> 
            [quick_tests];
        Multiple when is_list(Multiple) ->
            lists:foldl(fun(Arg, Acc) ->
                case Arg of
                    "unit" -> [unit_tests | Acc];
                    "prop" -> [property_tests | Acc];
                    "perf" -> [perf_tests | Acc];
                    "integration" -> [integration_tests | Acc];
                    "crash" -> [crash_tests | Acc];
                    _ -> 
                        io:format("Warning: Unknown test type '~s'~n", [Arg]),
                        Acc
                end
            end, [], Multiple);
        _ ->
            [unit_tests]
    end.

%% @doc Execute test plan
execute_test_plan(TestPlan) ->
    lists:map(fun(TestType) ->
        execute_test_type(TestType)
    end, TestPlan).

%% @doc Execute specific test type
execute_test_type(unit_tests) ->
    io:format("~n--- Running Unit Tests ---~n"),
    Result = run_ct_suite("test/bobsled_SUITE"),
    {unit_tests, Result};

execute_test_type(property_tests) ->
    io:format("~n--- Running Property-Based Tests ---~n"),
    Result = run_property_tests(),
    {property_tests, Result};

execute_test_type(perf_tests) ->
    io:format("~n--- Running Performance Tests ---~n"),
    Result = run_ct_suite_with_profile("test/bobsled_perf_SUITE", "perf"),
    {perf_tests, Result};

execute_test_type(integration_tests) ->
    io:format("~n--- Running Integration Tests ---~n"),
    Result = run_ct_suite_with_profile("test/bobsled_integration_SUITE", "integration"),
    {integration_tests, Result};

execute_test_type(crash_tests) ->
    io:format("~n--- Running Crash/Recovery Tests ---~n"),
    Result = run_ct_suite_with_profile("test/bobsled_crash_SUITE", "crash"),
    {crash_tests, Result};

execute_test_type(coverage_all) ->
    io:format("~n--- Running All Tests with Coverage ---~n"),
    Result = run_tests_with_coverage(),
    {coverage_all, Result};

execute_test_type(quick_tests) ->
    io:format("~n--- Running Quick Tests ---~n"),
    Result = run_quick_tests(),
    {quick_tests, Result}.

%% @doc Run Common Test suite
run_ct_suite(Suite) ->
    Cmd = io_lib:format("rebar3 ct --suite ~s", [Suite]),
    run_command(Cmd).

%% @doc Run Common Test suite with specific profile
run_ct_suite_with_profile(Suite, Profile) ->
    Cmd = io_lib:format("rebar3 as ~s ct --suite ~s", [Profile, Suite]),
    run_command(Cmd).

%% @doc Run property-based tests
run_property_tests() ->
    % First compile the property test module
    case run_command("rebar3 compile") of
        {ok, _} ->
            % Then run the property tests
            run_erl_command("bobsled_prop:test_all()");
        Error ->
            Error
    end.

%% @doc Run tests with coverage
run_tests_with_coverage() ->
    Cmd = "rebar3 as test ct --cover",
    case run_command(Cmd) of
        {ok, Output} ->
            % Generate coverage report
            run_command("rebar3 as test cover"),
            {ok, Output};
        Error ->
            Error
    end.

%% @doc Run quick smoke tests
run_quick_tests() ->
    % Run a subset of unit tests for quick feedback
    Cmd = "rebar3 ct --suite test/bobsled_SUITE --group basic_operations",
    run_command(Cmd).

%% @doc Run Erlang command
run_erl_command(ErlCmd) ->
    FullCmd = io_lib:format("rebar3 shell --eval \"~s, halt().\"", [ErlCmd]),
    run_command(FullCmd).

%% @doc Run system command
run_command(Cmd) ->
    io:format("Executing: ~s~n", [Cmd]),
    Port = open_port({spawn, lists:flatten(Cmd)}, [exit_status, {line, 1000}]),
    collect_output(Port, []).

%% @doc Collect command output
collect_output(Port, Acc) ->
    receive
        {Port, {data, {eol, Line}}} ->
            io:format("~s~n", [Line]),
            collect_output(Port, [Line | Acc]);
        {Port, {data, {noeol, Line}}} ->
            io:format("~s", [Line]),
            collect_output(Port, [Line | Acc]);
        {Port, {exit_status, 0}} ->
            {ok, lists:reverse(Acc)};
        {Port, {exit_status, Status}} ->
            {error, {exit_status, Status, lists:reverse(Acc)}}
    after 300000 -> % 5 minute timeout
        port_close(Port),
        {error, timeout}
    end.

%% @doc Report test results
report_results(Results) ->
    lists:foreach(fun({TestType, Result}) ->
        case Result of
            {ok, _Output} ->
                io:format("✓ ~s: PASSED~n", [format_test_type(TestType)]);
            {error, {exit_status, Status, _Output}} ->
                io:format("✗ ~s: FAILED (exit status ~p)~n", [format_test_type(TestType), Status]);
            {error, timeout} ->
                io:format("✗ ~s: TIMEOUT~n", [format_test_type(TestType)]);
            {error, Reason} ->
                io:format("✗ ~s: ERROR (~p)~n", [format_test_type(TestType), Reason])
        end
    end, Results).

%% @doc Format test type for display
format_test_type(unit_tests) -> "Unit Tests";
format_test_type(property_tests) -> "Property Tests";
format_test_type(perf_tests) -> "Performance Tests";
format_test_type(integration_tests) -> "Integration Tests";
format_test_type(crash_tests) -> "Crash/Recovery Tests";
format_test_type(coverage_all) -> "Coverage Analysis";
format_test_type(quick_tests) -> "Quick Tests".

%% @doc Check if all tests passed
all_tests_passed(Results) ->
    lists:all(fun({_TestType, Result}) ->
        case Result of
            {ok, _} -> true;
            _ -> false
        end
    end, Results).

%% @doc Clean up test files
cleanup_test_files() ->
    % Clean up temporary test databases
    case file:list_dir("/tmp") of
        {ok, Files} ->
            TestFiles = [F || F <- Files, 
                            lists:any(fun(Pattern) ->
                                string:prefix(F, Pattern) =/= nomatch
                            end, ["bobsled_test_", "bobsled_perf_test_", 
                                  "bobsled_integration_test_", "bobsled_crash_test_",
                                  "bobsled_prop_test_"])],
            lists:foreach(fun(File) ->
                FullPath = "/tmp/" ++ File,
                case filelib:is_dir(FullPath) of
                    true -> 
                        os:cmd("rm -rf " ++ FullPath);
                    false -> 
                        file:delete(FullPath)
                end
            end, TestFiles);
        _ -> 
            ok
    end.