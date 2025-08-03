# Contributing to Bobsled

Thank you for your interest in contributing to Bobsled! We welcome contributions from everyone.

## Code of Conduct

By participating in this project, you agree to abide by our Code of Conduct:
- Be respectful and inclusive
- Welcome newcomers and help them get started
- Focus on what is best for the community
- Show empathy towards other community members

## How to Contribute

### Reporting Issues

- Check if the issue already exists in the [issue tracker](https://github.com/twilson63/bobsled/issues)
- Create a new issue with a clear title and description
- Include steps to reproduce the problem
- Specify your environment (OS, Erlang/OTP version, Rust version)

### Suggesting Features

- Open an issue with the "enhancement" label
- Clearly describe the feature and its use case
- Discuss the feature with maintainers before implementing

### Pull Requests

1. **Fork the repository** and create your branch from `main`
2. **Write your code** following our style guidelines
3. **Add tests** for new functionality
4. **Update documentation** as needed
5. **Run tests** to ensure everything passes
6. **Submit a pull request** with a clear description

## Development Setup

### Prerequisites

- Erlang/OTP 24+
- Rust 1.70+
- Make

### Getting Started

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/bobsled.git
cd bobsled

# Add upstream remote
git remote add upstream https://github.com/twilson63/bobsled.git

# Build the project
make compile

# Run tests
./run_eunit_tests.escript
```

## Style Guidelines

### Rust Code

- Follow standard Rust conventions
- Use `rustfmt` for formatting
- Run `cargo clippy` and fix warnings
- Keep unsafe code to a minimum

```bash
cargo fmt
cargo clippy
```

### Erlang Code

- Follow OTP design principles
- Use meaningful variable and function names
- Add type specs for public functions
- Document modules and functions

### Commit Messages

- Use clear, descriptive commit messages
- Start with a verb in present tense ("Add", "Fix", "Update")
- Reference issues when applicable (#123)

Example:
```
Add batch_put operation for atomic bulk writes

- Implement batch_put/2 in Rust NIF
- Add Erlang wrapper with proper type specs
- Include comprehensive tests
- Update documentation

Fixes #42
```

## Testing

### Running Tests

```bash
# Run all tests
make test

# Run specific test suite
./run_eunit_tests.escript

# Run benchmarks
./run_benchmarks.escript
```

### Writing Tests

- Add unit tests for new functions
- Include edge cases and error conditions
- Test concurrent scenarios when applicable
- Ensure tests are deterministic

Example test:
```erlang
new_feature_test() ->
    {Db, _} = setup_test_db(),
    
    % Test normal case
    ?assertEqual(ok, bobsled:new_feature(Db, <<"test">>)),
    
    % Test error case
    ?assertMatch({error, _}, bobsled:new_feature(Db, invalid_input)),
    
    cleanup_test_db(Db).
```

## Documentation

- Update README.md for user-facing changes
- Add inline documentation for new functions
- Include examples in documentation
- Update API reference section

## Release Process

1. Update version in `Cargo.toml`
2. Update CHANGELOG.md
3. Run full test suite
4. Create a pull request
5. After merge, tag the release

## Getting Help

- Join our [Discord server](https://discord.gg/bobsled) (if applicable)
- Ask questions in GitHub issues
- Check existing documentation

## Recognition

Contributors will be recognized in:
- The project README
- Release notes
- Special thanks section

Thank you for contributing to Bobsled! 🛷