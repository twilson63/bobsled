# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] - 2025-08-03

### Added
- Initial release of Bobsled
- High-performance Erlang NIF for Sled embedded database
- Basic operations: put, get, delete
- Advanced operations: compare_and_swap, batch_put, transactions
- Hierarchical key support with list and fold operations
- Comprehensive panic safety to protect BEAM VM
- Full test suite with EUnit tests
- Performance benchmarks showing 500K+ writes/sec and 1.9M+ reads/sec
- OTP integration examples and best practices
- Documentation and API reference

### Performance
- Sequential writes: 578,292 ops/sec
- Sequential reads: 1,962,092 ops/sec
- Concurrent writes (10 processes): 804,388 ops/sec
- Concurrent reads (10 processes): 2,974,331 ops/sec
- Compare-and-swap: 763,533 ops/sec

[Unreleased]: https://github.com/twilson63/bobsled/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/twilson63/bobsled/releases/tag/v1.0.0