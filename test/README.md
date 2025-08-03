# Bobsled Test Suite

This directory contains a comprehensive test suite for the bobsled NIF, covering all aspects of functionality, performance, and reliability.

## Test Structure

### Test Suites

1. **`bobsled_SUITE.erl`** - Comprehensive Common Test suite
   - Basic operations (open, close, put, get, delete)
   - Error handling and edge cases
   - Advanced operations (CAS, batch, transactions)
   - Iteration operations (list, fold with prefixes)
   - Concurrent access patterns
   - Resource management

2. **`bobsled_prop.erl`** - Property-based tests using PropEr
   - Stateful and stateless property tests
   - Database operation invariants
   - Concurrent operation properties
   - Transaction isolation properties
   - Data consistency guarantees

3. **`bobsled_perf_SUITE.erl`** - Performance benchmark tests
   - Throughput benchmarks (reads/writes)
   - Latency measurements
   - Concurrent operation scalability
   - Memory usage patterns
   - Large dataset handling
   - Stress testing under load

4. **`bobsled_integration_SUITE.erl`** - Integration tests
   - OTP application lifecycle integration
   - Supervisor restart scenarios
   - Multi-process coordination
   - Resource cleanup verification
   - Hot code upgrade scenarios

5. **`bobsled_crash_SUITE.erl`** - Crash and recovery tests
   - Database recovery after crashes
   - NIF panic handling verification
   - Resource leak detection
   - Corruption detection and handling
   - System-level failure scenarios

### Test Configuration

- **`test.config`** - Test-specific configuration
- **`run_tests.erl`** - Comprehensive test runner script

## Running Tests

### Quick Start

```bash
# Run all tests
./test/run_tests.erl

# Run specific test suites
./test/run_tests.erl unit           # Unit tests only
./test/run_tests.erl prop           # Property-based tests
./test/run_tests.erl perf           # Performance tests
./test/run_tests.erl integration    # Integration tests
./test/run_tests.erl crash          # Crash/recovery tests

# Run with coverage analysis
./test/run_tests.erl coverage

# Quick smoke tests
./test/run_tests.erl quick
```

### Using Rebar3 Directly

```bash
# Basic unit tests
rebar3 ct --suite test/bobsled_SUITE

# Performance tests
rebar3 as perf ct --suite test/bobsled_perf_SUITE

# Integration tests
rebar3 as integration ct --suite test/bobsled_integration_SUITE

# Crash tests
rebar3 as crash ct --suite test/bobsled_crash_SUITE

# All tests with coverage
rebar3 as test ct --cover

# Property-based tests
rebar3 shell
> bobsled_prop:test_all().
```

### Test Profiles

The following rebar3 profiles are configured:

- **`test`** - Standard test profile with coverage
- **`perf`** - Performance testing profile
- **`integration`** - Integration testing profile  
- **`crash`** - Crash testing profile

## Test Categories

### Unit Tests (`bobsled_SUITE.erl`)

#### Basic Operations
- `test_open_close/1` - Database open and close
- `test_put_get/1` - Basic put/get operations
- `test_delete/1` - Delete operations
- `test_put_get_delete_cycle/1` - Complete operation cycles
- `test_multiple_databases/1` - Multiple database instances

#### Error Handling
- `test_invalid_arguments/1` - Invalid argument rejection
- `test_closed_database_operations/1` - Operations on closed databases
- `test_invalid_options/1` - Invalid configuration options
- `test_file_permissions/1` - File permission errors
- `test_nif_error_handling/1` - NIF-specific error handling

#### Advanced Operations
- `test_compare_and_swap_*` - Atomic compare-and-swap operations
- `test_batch_put/1` - Batch write operations
- `test_transactions_*` - Transaction operations and rollback
- `test_concurrent_*` - Concurrent operation handling

#### Iteration Operations
- `test_list_*` - Prefix-based key listing
- `test_fold_*` - Fold operations with accumulators

#### Edge Cases
- `test_empty_keys_values/1` - Empty keys and values
- `test_large_keys_values/1` - Large keys and values
- `test_unicode_keys_values/1` - Unicode handling
- `test_binary_keys_values/1` - Binary data handling
- `test_many_operations/1` - High-volume operations

### Property-Based Tests (`bobsled_prop.erl`)

#### Stateless Properties
- `prop_put_get_roundtrip/0` - Put followed by get returns same value
- `prop_get_nonexistent/0` - Get non-existent key returns not_found
- `prop_delete_removes_key/0` - Delete removes key-value pairs
- `prop_cas_*` - Compare-and-swap properties
- `prop_batch_put_equivalence/0` - Batch operations equivalent to individual
- `prop_list_prefix_matching/0` - List operations return correct prefixes
- `prop_fold_processes_all_entries/0` - Fold processes all entries
- `prop_persistence_across_sessions/0` - Data persists across sessions

#### Stateful Properties
- `prop_stateful_operations/0` - Stateful model-based testing
- Database state model with operation sequences
- Postcondition verification against model

#### Concurrent Properties
- `prop_concurrent_reads_consistent/0` - Concurrent reads return consistent results
- `prop_concurrent_writes_succeed/0` - Concurrent writes all succeed
- `prop_transaction_isolation/0` - Transaction isolation properties

### Performance Tests (`bobsled_perf_SUITE.erl`)

#### Throughput Benchmarks
- `bench_sequential_writes/1` - Sequential write throughput
- `bench_sequential_reads/1` - Sequential read throughput
- `bench_mixed_operations/1` - Mixed read/write operations
- `bench_batch_operations/1` - Batch operation throughput
- `bench_iteration_operations/1` - List/fold operation throughput

#### Latency Benchmarks
- `bench_single_operation_latency/1` - Individual operation latency
- `bench_transaction_latency/1` - Transaction latency
- `bench_cas_latency/1` - Compare-and-swap latency
- `bench_prefix_scan_latency/1` - Prefix scan latency

#### Concurrency Benchmarks
- `bench_concurrent_reads/1` - Concurrent read scalability
- `bench_concurrent_writes/1` - Concurrent write scalability
- `bench_concurrent_mixed/1` - Mixed concurrent operations
- `bench_reader_writer_scaling/1` - Reader/writer scaling patterns
- `bench_transaction_contention/1` - Transaction contention handling

#### Memory Benchmarks
- `bench_memory_usage_growth/1` - Memory usage patterns
- `bench_cache_efficiency/1` - Cache hit/miss patterns
- `bench_large_value_handling/1` - Large value performance
- `bench_gc_pressure/1` - Garbage collection impact

#### Large Data Benchmarks
- `bench_million_keys/1` - Million key operations
- `bench_large_values/1` - Large value handling
- `bench_deep_prefix_trees/1` - Hierarchical data performance
- `bench_database_size_growth/1` - Database growth patterns

#### Stress Tests
- `stress_sustained_load/1` - Sustained high load
- `stress_rapid_opens_closes/1` - Rapid open/close cycles
- `stress_memory_pressure/1` - Memory pressure scenarios
- `stress_mixed_workload/1` - Complex mixed workloads

### Integration Tests (`bobsled_integration_SUITE.erl`)

#### Application Lifecycle
- `test_app_start_stop/1` - Application start/stop cycles
- `test_app_restart/1` - Application restart scenarios
- `test_app_config_changes/1` - Configuration change handling

#### Supervision Tree
- `test_supervisor_child_restart/1` - Child process restart
- `test_supervisor_shutdown/1` - Graceful shutdown
- `test_supervisor_max_restarts/1` - Restart intensity limits
- `test_nested_supervision/1` - Nested supervisor hierarchies

#### Process Coordination
- `test_multiple_processes_same_db/1` - Multiple processes, same database
- `test_process_isolation/1` - Process isolation verification
- `test_shared_database_access/1` - Shared database access patterns
- `test_concurrent_process_operations/1` - Concurrent process operations
- `test_process_death_cleanup/1` - Process death cleanup

#### Resource Management
- `test_resource_cleanup_on_crash/1` - Resource cleanup after crashes
- `test_database_handle_sharing/1` - Database handle sharing
- `test_memory_leak_detection/1` - Memory leak detection
- `test_file_handle_management/1` - File handle management
- `test_ets_cleanup/1` - ETS table cleanup

#### Error Recovery
- `test_database_corruption_recovery/1` - Corruption recovery
- `test_disk_full_recovery/1` - Disk full scenarios
- `test_network_partition_simulation/1` - Network partition simulation
- `test_partial_write_recovery/1` - Partial write recovery
- `test_process_link_failures/1` - Process link failure handling

#### Hot Code Upgrade
- `test_code_upgrade_during_operations/1` - Code upgrade during operations
- `test_state_preservation_upgrade/1` - State preservation during upgrade
- `test_version_compatibility/1` - Version compatibility

### Crash and Recovery Tests (`bobsled_crash_SUITE.erl`)

#### Process Crashes
- `test_process_exit_normal/1` - Normal process exit
- `test_process_exit_abnormal/1` - Abnormal process exit
- `test_process_killed/1` - Process killed with signals
- `test_linked_process_crash/1` - Linked process crash propagation
- `test_monitor_process_crash/1` - Monitored process crash detection

#### System Crashes
- `test_vm_restart_simulation/1` - VM restart simulation
- `test_power_failure_simulation/1` - Power failure simulation
- `test_disk_failure_simulation/1` - Disk failure simulation
- `test_memory_exhaustion/1` - Memory exhaustion scenarios
- `test_file_system_errors/1` - File system error handling

#### NIF Panics
- `test_invalid_handle_usage/1` - Invalid handle usage protection
- `test_concurrent_handle_access/1` - Concurrent handle access
- `test_double_close_protection/1` - Double close protection
- `test_use_after_close/1` - Use after close protection
- `test_memory_corruption_detection/1` - Memory corruption detection

#### Corruption Scenarios
- `test_partial_write_corruption/1` - Partial write corruption
- `test_file_truncation/1` - File truncation recovery
- `test_random_byte_corruption/1` - Random byte corruption
- `test_header_corruption/1` - Header corruption recovery
- `test_index_corruption/1` - Index corruption recovery

#### Resource Exhaustion
- `test_out_of_memory/1` - Out of memory scenarios
- `test_file_descriptor_exhaustion/1` - File descriptor exhaustion
- `test_disk_space_exhaustion/1` - Disk space exhaustion
- `test_large_transaction_limits/1` - Large transaction limits
- `test_concurrent_connection_limits/1` - Connection limits

#### Recovery Verification
- `test_data_integrity_after_crash/1` - Data integrity after crash
- `test_transaction_atomicity/1` - Transaction atomicity verification
- `test_consistency_after_recovery/1` - Consistency after recovery
- `test_performance_after_recovery/1` - Performance after recovery
- `test_incremental_recovery/1` - Incremental recovery patterns

## Test Outputs

### Logs
Test logs are written to the `logs/` directory:
- `logs/test.log` - Main test log
- `logs/sasl.log` - SASL error log
- `logs/perf/` - Performance test logs
- `logs/integration/` - Integration test logs
- `logs/crash/` - Crash test logs

### Coverage Reports
Coverage reports are generated in `_build/test/cover/`:
- HTML coverage reports
- Coverage summary statistics
- Per-module coverage analysis

### Performance Reports
Performance test results include:
- Throughput measurements (ops/sec)
- Latency percentiles (P50, P95, P99)
- Memory usage patterns
- Scalability metrics
- Stress test results

## Test Guidelines

### Writing New Tests

1. **Follow Naming Conventions**
   - Test functions should start with `test_` for unit tests
   - Property functions should start with `prop_` for property tests
   - Benchmark functions should start with `bench_` for performance tests

2. **Use Appropriate Test Framework**
   - Common Test for functional testing
   - PropEr for property-based testing
   - Custom benchmarking for performance testing

3. **Include Proper Documentation**
   - Document test purpose and expected behavior
   - Include examples of failure scenarios
   - Document any special setup requirements

4. **Resource Cleanup**
   - Always clean up test databases
   - Use unique paths for each test
   - Ensure no resource leaks

5. **Error Handling**
   - Test both success and failure cases
   - Verify error messages and codes
   - Test edge cases and boundary conditions

### Test Data Management

- Use temporary directories for test databases
- Generate deterministic test data where possible
- Clean up test artifacts after each test
- Use appropriate data sizes for test objectives

### Performance Testing Guidelines

- Run performance tests in dedicated environment
- Use consistent hardware and system configuration
- Account for system load and other processes
- Include warm-up periods for accurate measurements
- Report confidence intervals and variability

### Debugging Failed Tests

1. **Check Test Logs**
   - Review detailed logs in `logs/` directory
   - Look for error patterns and stack traces
   - Check resource usage and timing issues

2. **Reproduce Failures**
   - Run individual test cases in isolation
   - Use debugger or additional logging
   - Verify test environment consistency

3. **Common Issues**
   - File permission problems
   - Resource cleanup failures
   - Timing-dependent test failures
   - Memory or disk space issues

## Continuous Integration

The test suite is designed to run in CI environments:

- All tests should be deterministic
- Tests clean up their own resources
- Reasonable timeouts for CI systems
- Clear pass/fail criteria
- Comprehensive error reporting

### CI Commands

```bash
# Basic CI test run
rebar3 ct

# Full CI test run with coverage
rebar3 as test ct --cover

# Performance regression testing
rebar3 as perf ct --suite test/bobsled_perf_SUITE

# Integration testing
rebar3 as integration ct --suite test/bobsled_integration_SUITE
```

## Contributing

When adding new tests:

1. Follow existing patterns and conventions
2. Add tests to appropriate test suite
3. Update this documentation
4. Ensure tests pass in clean environment
5. Add appropriate assertions and error handling

For questions about the test suite, please refer to the individual test files for detailed implementation examples.