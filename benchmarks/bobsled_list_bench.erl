%%%-------------------------------------------------------------------
%%% @doc Bobsled List Operation Benchmarks
%%%
%%% Benchmarks for hierarchical key structures and list operations,
%%% testing the performance of path-based key organization and
%%% directory-like listing capabilities.
%%%
%%% @author Bobsled Team
%%% @copyright 2025 Bobsled Project
%%% @end
%%%-------------------------------------------------------------------
-module(bobsled_list_bench).

-export([
    run_all/0,
    run_all/1,
    benchmark_shallow_hierarchy/1,
    benchmark_deep_hierarchy/1,
    benchmark_wide_hierarchy/1,
    benchmark_mixed_hierarchy/1,
    benchmark_list_scaling/0,
    demo_file_system/0
]).

-define(DEFAULT_NODES, 10000).

%%%===================================================================
%%% API Functions
%%%===================================================================

%% @doc Run all list benchmarks with default settings
run_all() ->
    run_all(#{}).

%% @doc Run all list benchmarks with custom options
run_all(Options) ->
    NodeCount = maps:get(nodes, Options, ?DEFAULT_NODES),
    DbPath = maps:get(path, Options, <<"/tmp/bobsled_list_bench">>),
    
    io:format("~n╔════════════════════════════════════════════════════╗~n"),
    io:format("║     Bobsled Hierarchical List Benchmarks          ║~n"),
    io:format("╚════════════════════════════════════════════════════╝~n"),
    io:format("Database path: ~s~n", [DbPath]),
    io:format("Total nodes: ~p~n~n", [NodeCount]),
    
    % Clean up any existing database
    os:cmd("rm -rf " ++ binary_to_list(DbPath)),
    
    % Run benchmarks
    Results = [
        {"Shallow Hierarchy (3 levels)", benchmark_shallow_hierarchy(NodeCount)},
        {"Deep Hierarchy (10 levels)", benchmark_deep_hierarchy(NodeCount)},
        {"Wide Hierarchy (1000 children)", benchmark_wide_hierarchy(NodeCount)},
        {"Mixed Hierarchy", benchmark_mixed_hierarchy(NodeCount)}
    ],
    
    % Print summary
    io:format("~n📊 BENCHMARK SUMMARY~n"),
    io:format("═══════════════════════════════════════════════════~n"),
    lists:foreach(fun({Name, {write_ops_sec, WriteOps, list_ops_sec, ListOps, avg_children, AvgChildren}}) ->
        io:format("~s:~n", [Name]),
        io:format("  Write throughput: ~10.2f ops/sec~n", [WriteOps]),
        io:format("  List throughput:  ~10.2f ops/sec~n", [ListOps]),
        io:format("  Avg children:     ~10.2f per list~n~n", [AvgChildren])
    end, Results),
    
    % Run scaling benchmark
    io:format("Running list operation scaling benchmark...~n"),
    benchmark_list_scaling(),
    
    % Demo file system simulation
    io:format("~nRunning file system simulation demo...~n"),
    demo_file_system(),
    
    % Clean up
    os:cmd("rm -rf " ++ binary_to_list(DbPath)),
    Results.

%%%===================================================================
%%% Benchmark Functions
%%%===================================================================

%% @doc Benchmark shallow hierarchy (few levels, many siblings)
benchmark_shallow_hierarchy(NodeCount) ->
    {Db, DbPath} = setup_db(),
    
    io:format("🌳 SHALLOW HIERARCHY BENCHMARK (~p nodes, 3 levels)~n", [NodeCount]),
    io:format("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━~n"),
    
    % Create structure: /category/subcategory/item
    % 10 categories, 100 subcategories each, remaining as items
    Categories = 10,
    SubcategoriesPerCat = 100,
    ItemsPerSubcat = NodeCount div (Categories * SubcategoriesPerCat),
    
    % Write benchmark
    io:format("Writing hierarchical data...~n"),
    WriteStart = erlang:monotonic_time(microsecond),
    
    TotalWrites = lists:foldl(fun(Cat, AccOuter) ->
        CatPath = <<"data/cat", (integer_to_binary(Cat))/binary>>,
        ok = bobsled:put(Db, CatPath, <<"category_metadata">>),
        
        lists:foldl(fun(SubCat, AccInner) ->
            SubCatPath = <<CatPath/binary, "/subcat", (integer_to_binary(SubCat))/binary>>,
            ok = bobsled:put(Db, SubCatPath, <<"subcategory_metadata">>),
            
            lists:foreach(fun(Item) ->
                ItemPath = <<SubCatPath/binary, "/item", (integer_to_binary(Item))/binary>>,
                ok = bobsled:put(Db, ItemPath, generate_item_data(Item))
            end, lists:seq(1, ItemsPerSubcat)),
            
            AccInner + ItemsPerSubcat + 1
        end, AccOuter + 1, lists:seq(1, SubcategoriesPerCat))
    end, 0, lists:seq(1, Categories)),
    
    WriteEnd = erlang:monotonic_time(microsecond),
    WriteDuration = WriteEnd - WriteStart,
    WriteOpsPerSec = (TotalWrites * 1000000) / WriteDuration,
    
    io:format("✓ Wrote ~p nodes in ~.3f seconds (~.2f ops/sec)~n", 
              [TotalWrites, WriteDuration / 1000000, WriteOpsPerSec]),
    
    % List benchmark
    io:format("~nBenchmarking list operations...~n"),
    
    % Test different levels
    ListResults = [
        {"Root level", time_list_operation(Db, <<"data/">>)},
        {"Category level", time_list_operation(Db, <<"data/cat5/">>)},
        {"Subcategory level", time_list_operation(Db, <<"data/cat5/subcat50/">>)}
    ],
    
    TotalListOps = 0,
    TotalListTime = 0,
    TotalChildren = 0,
    
    lists:foreach(fun({Level, {Count, Duration, Children}}) ->
        OpsPerSec = (Count * 1000000) / Duration,
        io:format("  ~-20s: ~6.2f ops/sec (~p children, ~.2f μs/op)~n", 
                  [Level, OpsPerSec, Children, Duration / Count])
    end, ListResults),
    
    % Calculate aggregate list performance
    {ListOps, ListTime, ChildCount} = lists:foldl(
        fun({_, {Ops, Time, Children}}, {AccOps, AccTime, AccChildren}) ->
            {AccOps + Ops, AccTime + Time, AccChildren + Children}
        end, {0, 0, 0}, ListResults),
    
    ListOpsPerSec = (ListOps * 1000000) / ListTime,
    AvgChildren = ChildCount / length(ListResults),
    
    cleanup_db(Db, DbPath),
    {write_ops_sec, WriteOpsPerSec, list_ops_sec, ListOpsPerSec, avg_children, AvgChildren}.

%% @doc Benchmark deep hierarchy (many levels, few siblings)
benchmark_deep_hierarchy(NodeCount) ->
    {Db, DbPath} = setup_db(),
    
    io:format("~n🌲 DEEP HIERARCHY BENCHMARK (~p nodes, 10 levels)~n", [NodeCount]),
    io:format("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━~n"),
    
    % Create deep paths with branching factor of 3
    BranchingFactor = 3,
    MaxDepth = 10,
    
    % Write benchmark
    io:format("Writing deep hierarchical data...~n"),
    WriteStart = erlang:monotonic_time(microsecond),
    
    TotalWrites = write_deep_tree(Db, <<"deep">>, 0, MaxDepth, BranchingFactor, NodeCount),
    
    WriteEnd = erlang:monotonic_time(microsecond),
    WriteDuration = WriteEnd - WriteStart,
    WriteOpsPerSec = (TotalWrites * 1000000) / WriteDuration,
    
    io:format("✓ Wrote ~p nodes in ~.3f seconds (~.2f ops/sec)~n", 
              [TotalWrites, WriteDuration / 1000000, WriteOpsPerSec]),
    
    % List benchmark at different depths
    io:format("~nBenchmarking list operations at different depths...~n"),
    
    ListResults = lists:map(fun(Depth) ->
        Path = build_deep_path(<<"deep">>, Depth),
        {Count, Duration, Children} = time_list_operation(Db, Path),
        OpsPerSec = (Count * 1000000) / Duration,
        io:format("  Depth ~2b: ~8.2f ops/sec (~p children)~n", 
                  [Depth, OpsPerSec, Children]),
        {Count, Duration, Children}
    end, [0, 2, 4, 6, 8]),
    
    % Calculate aggregate performance
    {ListOps, ListTime, ChildCount} = lists:foldl(
        fun({Ops, Time, Children}, {AccOps, AccTime, AccChildren}) ->
            {AccOps + Ops, AccTime + Time, AccChildren + Children}
        end, {0, 0, 0}, ListResults),
    
    ListOpsPerSec = (ListOps * 1000000) / ListTime,
    AvgChildren = ChildCount / length(ListResults),
    
    cleanup_db(Db, DbPath),
    {write_ops_sec, WriteOpsPerSec, list_ops_sec, ListOpsPerSec, avg_children, AvgChildren}.

%% @doc Benchmark wide hierarchy (single level, many siblings)
benchmark_wide_hierarchy(NodeCount) ->
    {Db, DbPath} = setup_db(),
    
    io:format("~n🌿 WIDE HIERARCHY BENCHMARK (~p nodes, 1000+ children per parent)~n", [NodeCount]),
    io:format("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━~n"),
    
    % Create structure with very wide directories
    NumDirs = 10,
    ItemsPerDir = NodeCount div NumDirs,
    
    % Write benchmark
    io:format("Writing wide hierarchical data...~n"),
    WriteStart = erlang:monotonic_time(microsecond),
    
    TotalWrites = lists:foldl(fun(Dir, Acc) ->
        DirPath = <<"wide/dir", (integer_to_binary(Dir))/binary>>,
        ok = bobsled:put(Db, DirPath, <<"directory_metadata">>),
        
        % Write many items in this directory
        lists:foreach(fun(Item) ->
            ItemPath = <<DirPath/binary, "/file", (integer_to_binary(Item))/binary>>,
            ok = bobsled:put(Db, ItemPath, generate_item_data(Item))
        end, lists:seq(1, ItemsPerDir)),
        
        Acc + ItemsPerDir + 1
    end, 0, lists:seq(1, NumDirs)),
    
    WriteEnd = erlang:monotonic_time(microsecond),
    WriteDuration = WriteEnd - WriteStart,
    WriteOpsPerSec = (TotalWrites * 1000000) / WriteDuration,
    
    io:format("✓ Wrote ~p nodes in ~.3f seconds (~.2f ops/sec)~n", 
              [TotalWrites, WriteDuration / 1000000, WriteOpsPerSec]),
    
    % List benchmark - test listing directories with many children
    io:format("~nBenchmarking list operations on wide directories...~n"),
    
    % Test listing different directories
    ListResults = lists:map(fun(Dir) ->
        DirPath = <<"wide/dir", (integer_to_binary(Dir))/binary, "/">>,
        {Count, Duration, Children} = time_list_operation(Db, DirPath),
        OpsPerSec = (Count * 1000000) / Duration,
        io:format("  Directory ~p: ~8.2f ops/sec (~p children, ~.2f ms/op)~n", 
                  [Dir, OpsPerSec, Children, Duration / 1000]),
        {Count, Duration, Children}
    end, lists:seq(1, min(5, NumDirs))),
    
    % Calculate aggregate performance
    {ListOps, ListTime, ChildCount} = lists:foldl(
        fun({Ops, Time, Children}, {AccOps, AccTime, AccChildren}) ->
            {AccOps + Ops, AccTime + Time, AccChildren + Children}
        end, {0, 0, 0}, ListResults),
    
    ListOpsPerSec = (ListOps * 1000000) / ListTime,
    AvgChildren = ChildCount / length(ListResults),
    
    cleanup_db(Db, DbPath),
    {write_ops_sec, WriteOpsPerSec, list_ops_sec, ListOpsPerSec, avg_children, AvgChildren}.

%% @doc Benchmark mixed hierarchy (realistic file system simulation)
benchmark_mixed_hierarchy(NodeCount) ->
    {Db, DbPath} = setup_db(),
    
    io:format("~n🌴 MIXED HIERARCHY BENCHMARK (~p nodes, realistic structure)~n", [NodeCount]),
    io:format("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━~n"),
    
    % Create a realistic file system structure
    % /home/users/*/documents, downloads, pictures, etc.
    
    Users = 100,
    Folders = ["documents", "downloads", "pictures", "music", "videos"],
    FilesPerFolder = (NodeCount - Users * length(Folders)) div (Users * length(Folders)),
    
    % Write benchmark
    io:format("Writing mixed hierarchical data...~n"),
    WriteStart = erlang:monotonic_time(microsecond),
    
    TotalWrites = lists:foldl(fun(User, AccUser) ->
        UserPath = <<"home/user", (integer_to_binary(User))/binary>>,
        ok = bobsled:put(Db, UserPath, <<"user_profile">>),
        
        lists:foldl(fun(Folder, AccFolder) ->
            FolderPath = <<UserPath/binary, "/", (list_to_binary(Folder))/binary>>,
            ok = bobsled:put(Db, FolderPath, <<"folder_metadata">>),
            
            % Add files to folder
            lists:foreach(fun(File) ->
                FilePath = <<FolderPath/binary, "/file", (integer_to_binary(File))/binary, ".dat">>,
                ok = bobsled:put(Db, FilePath, generate_item_data(File))
            end, lists:seq(1, FilesPerFolder)),
            
            AccFolder + FilesPerFolder + 1
        end, AccUser + 1, Folders)
    end, 0, lists:seq(1, Users)),
    
    WriteEnd = erlang:monotonic_time(microsecond),
    WriteDuration = WriteEnd - WriteStart,
    WriteOpsPerSec = (TotalWrites * 1000000) / WriteDuration,
    
    io:format("✓ Wrote ~p nodes in ~.3f seconds (~.2f ops/sec)~n", 
              [TotalWrites, WriteDuration / 1000000, WriteOpsPerSec]),
    
    % List benchmark - test various list operations
    io:format("~nBenchmarking realistic list operations...~n"),
    
    ListTests = [
        {"List all users", <<"home/">>},
        {"List user folders", <<"home/user50/">>},
        {"List documents", <<"home/user50/documents/">>},
        {"List downloads", <<"home/user50/downloads/">>}
    ],
    
    ListResults = lists:map(fun({TestName, Path}) ->
        {Count, Duration, Children} = time_list_operation(Db, Path),
        OpsPerSec = (Count * 1000000) / Duration,
        io:format("  ~-20s: ~8.2f ops/sec (~p children)~n", 
                  [TestName, OpsPerSec, Children]),
        {Count, Duration, Children}
    end, ListTests),
    
    % Calculate aggregate performance
    {ListOps, ListTime, ChildCount} = lists:foldl(
        fun({Ops, Time, Children}, {AccOps, AccTime, AccChildren}) ->
            {AccOps + Ops, AccTime + Time, AccChildren + Children}
        end, {0, 0, 0}, ListResults),
    
    ListOpsPerSec = (ListOps * 1000000) / ListTime,
    AvgChildren = ChildCount / length(ListResults),
    
    cleanup_db(Db, DbPath),
    {write_ops_sec, WriteOpsPerSec, list_ops_sec, ListOpsPerSec, avg_children, AvgChildren}.

%% @doc Benchmark list operation scaling with different child counts
benchmark_list_scaling() ->
    {Db, DbPath} = setup_db(),
    
    io:format("~n📈 LIST OPERATION SCALING BENCHMARK~n"),
    io:format("════════════════════════════════════════════════════~n"),
    io:format("Testing list performance with varying child counts...~n~n"),
    
    ChildCounts = [10, 50, 100, 500, 1000, 5000, 10000],
    
    Results = lists:map(fun(ChildCount) ->
        Path = <<"scale/test", (integer_to_binary(ChildCount))/binary>>,
        
        % Create directory with N children
        lists:foreach(fun(I) ->
            ChildPath = <<Path/binary, "/child", (integer_to_binary(I))/binary>>,
            ok = bobsled:put(Db, ChildPath, <<"child_data">>)
        end, lists:seq(1, ChildCount)),
        
        % Benchmark list operation
        {Iterations, Duration, _} = time_list_operation(Db, <<Path/binary, "/">>, 100),
        OpsPerSec = (Iterations * 1000000) / Duration,
        AvgDuration = Duration / Iterations,
        
        io:format("  ~6b children: ~10.2f ops/sec (~6.2f μs/op)~n", 
                  [ChildCount, OpsPerSec, AvgDuration]),
        
        {ChildCount, OpsPerSec, AvgDuration}
    end, ChildCounts),
    
    % Analyze scaling
    io:format("~nScaling Analysis:~n"),
    [{C1, O1, _} | Rest] = Results,
    lists:foldl(fun({C2, O2, _}, {PrevC, PrevO}) ->
        Ratio = O1 / O2,
        Factor = C2 / C1,
        io:format("  ~6bx more children: ~5.2fx slower (from ~b to ~b children)~n",
                  [round(C2/PrevC), PrevO/O2, PrevC, C2]),
        {C2, O2}
    end, {C1, O1}, Rest),
    
    cleanup_db(Db, DbPath),
    Results.

%% @doc Demo file system simulation with common operations
demo_file_system() ->
    {Db, DbPath} = setup_db(),
    
    io:format("~n🖥️  FILE SYSTEM SIMULATION DEMO~n"),
    io:format("════════════════════════════════════════════════════~n"),
    
    % Create a mini file system
    create_file_system_structure(Db),
    
    % Demonstrate various list operations
    io:format("~nNavigating file system structure:~n"),
    
    % List root
    {ok, RootItems} = bobsled:list(Db, <<"fs/">>),
    io:format("~n/ (root)~n"),
    lists:foreach(fun(Item) ->
        io:format("  ├── ~s~n", [binary:part(Item, byte_size(<<"fs/">>) , byte_size(Item) - byte_size(<<"fs/">>))])
    end, lists:sort(RootItems)),
    
    % List a user's home
    {ok, UserItems} = bobsled:list(Db, <<"fs/home/alice/">>),
    io:format("~n/home/alice/~n"),
    lists:foreach(fun(Item) ->
        RelPath = binary:part(Item, byte_size(<<"fs/home/alice/">>), 
                             byte_size(Item) - byte_size(<<"fs/home/alice/">>)),
        io:format("  ├── ~s~n", [RelPath])
    end, lists:sort(UserItems)),
    
    % List project files
    {ok, ProjectItems} = bobsled:list(Db, <<"fs/home/alice/projects/webapp/">>),
    io:format("~n/home/alice/projects/webapp/~n"),
    lists:foreach(fun(Item) ->
        RelPath = binary:part(Item, byte_size(<<"fs/home/alice/projects/webapp/">>), 
                             byte_size(Item) - byte_size(<<"fs/home/alice/projects/webapp/">>)),
        io:format("  ├── ~s~n", [RelPath])
    end, lists:sort(ProjectItems)),
    
    % Performance test on realistic operations
    io:format("~nCommon file system operations benchmark:~n"),
    
    % Benchmark common operations
    Operations = [
        {"List user home", fun() -> bobsled:list(Db, <<"fs/home/alice/">>) end},
        {"List projects", fun() -> bobsled:list(Db, <<"fs/home/alice/projects/">>) end},
        {"List source files", fun() -> bobsled:list(Db, <<"fs/home/alice/projects/webapp/src/">>) end},
        {"Check file exists", fun() -> bobsled:get(Db, <<"fs/home/alice/projects/webapp/src/main.erl">>) end},
        {"Read config", fun() -> bobsled:get(Db, <<"fs/home/alice/.config/app.conf">>) end}
    ],
    
    lists:foreach(fun({OpName, Op}) ->
        {Time, _Result} = timer:tc(fun() ->
            lists:foreach(fun(_) -> Op() end, lists:seq(1, 1000))
        end),
        OpsPerSec = (1000 * 1000000) / Time,
        io:format("  ~-20s: ~10.2f ops/sec~n", [OpName, OpsPerSec])
    end, Operations),
    
    cleanup_db(Db, DbPath),
    ok.

%%%===================================================================
%%% Helper Functions
%%%===================================================================

%% @private
setup_db() ->
    DbPath = <<"/tmp/bobsled_list_bench_", (integer_to_binary(erlang:system_time()))/binary>>,
    {ok, Db} = bobsled:open(DbPath, [
        {mode, fast},
        {cache_capacity, 512 * 1024 * 1024}, % 512MB cache
        {compression_factor, 3}, % Light compression
        {flush_every_ms, 30000}
    ]),
    {Db, DbPath}.

%% @private
cleanup_db(Db, DbPath) ->
    ok = bobsled:close(Db),
    os:cmd("rm -rf " ++ binary_to_list(DbPath)).

%% @private
generate_item_data(Index) ->
    <<"item_data_", (integer_to_binary(Index))/binary, "_", 
      (crypto:strong_rand_bytes(32))/binary>>.

%% @private
time_list_operation(Db, Prefix) ->
    time_list_operation(Db, Prefix, 1000).

time_list_operation(Db, Prefix, Iterations) ->
    Start = erlang:monotonic_time(microsecond),
    
    ChildCounts = lists:map(fun(_) ->
        {ok, Children} = bobsled:list(Db, Prefix),
        length(Children)
    end, lists:seq(1, Iterations)),
    
    End = erlang:monotonic_time(microsecond),
    Duration = End - Start,
    AvgChildren = lists:sum(ChildCounts) / length(ChildCounts),
    
    {Iterations, Duration, round(AvgChildren)}.

%% @private
write_deep_tree(Db, Path, CurrentDepth, MaxDepth, BranchingFactor, RemainingNodes) when RemainingNodes > 0 ->
    ok = bobsled:put(Db, Path, <<"node_data">>),
    
    if
        CurrentDepth < MaxDepth ->
            {Written, Remaining} = lists:foldl(fun(Child, {AccWritten, AccRemaining}) ->
                if
                    AccRemaining > 0 ->
                        ChildPath = <<Path/binary, "/node", (integer_to_binary(Child))/binary>>,
                        ChildWritten = write_deep_tree(Db, ChildPath, CurrentDepth + 1, 
                                                       MaxDepth, BranchingFactor, 
                                                       AccRemaining - 1),
                        {AccWritten + ChildWritten, AccRemaining - ChildWritten};
                    true ->
                        {AccWritten, 0}
                end
            end, {1, RemainingNodes - 1}, lists:seq(1, BranchingFactor)),
            Written;
        true ->
            1
    end;
write_deep_tree(_, _, _, _, _, _) ->
    0.

%% @private
build_deep_path(Base, 0) ->
    <<Base/binary, "/">>;
build_deep_path(Base, Depth) ->
    Path = lists:foldl(fun(_, Acc) ->
        <<Acc/binary, "/node1">>
    end, Base, lists:seq(1, Depth)),
    <<Path/binary, "/">>.

%% @private
create_file_system_structure(Db) ->
    % Create a realistic file system structure
    Structure = [
        % System directories
        <<"fs/bin">>,
        <<"fs/etc">>,
        <<"fs/var">>,
        <<"fs/tmp">>,
        
        % User directories
        <<"fs/home">>,
        <<"fs/home/alice">>,
        <<"fs/home/alice/.config">>,
        <<"fs/home/alice/.config/app.conf">>,
        <<"fs/home/alice/documents">>,
        <<"fs/home/alice/documents/report.pdf">>,
        <<"fs/home/alice/documents/notes.txt">>,
        <<"fs/home/alice/downloads">>,
        <<"fs/home/alice/downloads/package.zip">>,
        <<"fs/home/alice/pictures">>,
        <<"fs/home/alice/pictures/vacation.jpg">>,
        <<"fs/home/alice/projects">>,
        <<"fs/home/alice/projects/webapp">>,
        <<"fs/home/alice/projects/webapp/README.md">>,
        <<"fs/home/alice/projects/webapp/src">>,
        <<"fs/home/alice/projects/webapp/src/main.erl">>,
        <<"fs/home/alice/projects/webapp/src/utils.erl">>,
        <<"fs/home/alice/projects/webapp/src/config.erl">>,
        <<"fs/home/alice/projects/webapp/test">>,
        <<"fs/home/alice/projects/webapp/test/main_test.erl">>,
        
        <<"fs/home/bob">>,
        <<"fs/home/bob/documents">>,
        <<"fs/home/bob/projects">>
    ],
    
    lists:foreach(fun(Path) ->
        Value = case binary:match(Path, <<".">>) of
            nomatch -> <<"directory">>;
            _ -> <<"file_content_", (crypto:strong_rand_bytes(20))/binary>>
        end,
        ok = bobsled:put(Db, Path, Value)
    end, Structure).