# Contributing to zeroio

Thank you for your interest in contributing to zeroio! We welcome all forms of contributions and appreciate your help in making this project better.

## Code Ownership and Licensing

**Important**: By contributing code to this project, you agree that your contributions become part of the project and can be used by anyone according to the project's license terms. Your code will be available under the same license as the rest of the project.

## Ways to Contribute

We welcome contributions in many forms:

- **Code contributions**: Bug fixes, new features, performance improvements
- **Documentation**: README improvements, code comments, examples, tutorials
- **Testing**: Writing tests, reporting bugs, testing on different platforms
- **Design**: Protocol improvements, API design suggestions
- **Community**: Helping others in discussions, answering questions

## Getting Started

### Development Setup

1. **Fork and clone the repository**
   ```bash
   git clone https://github.com/SithraBot/zeroio.git
   cd zeroio
   ```

2. **Install Rust** (if not already installed)

3. **Build the project**
   ```bash
   cargo build
   ```

4. **Run tests**
   ```bash
   cargo test
   ```

### Making Changes

1. **Create a feature branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes**
   - Follow the existing code style
   - Add tests for new functionality
   - Update documentation as needed

3. **Test your changes**
   ```bash
   cargo test
   cargo clippy
   cargo fmt --check
   ```

   Note: The project uses custom configuration files to maintain consistent code style and enforce best practices.

## Code Guidelines

### Configuration Setup

The project uses a two-tier configuration system:

1. **Workspace-level lints** (in root `Cargo.toml`): Defines which clippy lints to enable/disable
2. **Tool-specific configuration**:
   - `clippy.toml`: Sets thresholds and behavior for clippy lints
   - `rustfmt.toml`: Controls code formatting style

### Running Code Quality Tools

```bash
# Check formatting (don't auto-fix)
cargo fmt --check

# Auto-format code
cargo fmt

# Run clippy on entire workspace
cargo clippy --workspace

# Run clippy and auto-fix simple issues
cargo clippy --workspace --fix
```

### Rust Standards
- Follow project formatting standards (`cargo fmt`)
- Address all Clippy warnings (`cargo clippy --workspace`)
- Write comprehensive tests for new features
- Use meaningful variable and function names
- Add documentation comments for public APIs

The project maintains custom rules optimized for communication library development:
- **Clippy lints**: Focus on performance, memory safety, error handling, and API design
- **Formatting**: 100-character lines, struct field alignment, import grouping
- **Thresholds**: Conservative limits on complexity, function parameters, and type sizes

### Protocol Changes
- Protocol modifications require careful consideration
- Maintain backward compatibility when possible
- Update the protocol specification in `draft.md`
- Consider cross-language implications

### Performance
- Profile performance-critical changes
- Consider memory allocation patterns
- Benchmark against existing implementations

## Submitting Changes

### Pull Request Process

1. **Update documentation** if you've made API changes
2. **Add tests** that cover your changes
3. **Ensure all tests pass** locally
4. **Write a clear commit message** describing your changes
5. **Submit a pull request** with:
   - Clear description of what you've changed
   - Why the change is needed
   - Any breaking changes
   - Test results

### Pull Request Guidelines

- Keep PRs focused on a single feature or fix
- Write clear, descriptive commit messages
- Reference relevant issues in your PR description
- Be responsive to feedback and review comments

## Reporting Issues

### Bug Reports
When reporting bugs, please include:
- Operating system and version
- Rust version (`rustc --version`)
- Steps to reproduce the issue
- Expected vs actual behavior
- Relevant logs or error messages

### Feature Requests
For new features, please describe:
- The use case for the feature
- How it would work
- Any alternatives you've considered
- Whether you're willing to implement it

## Communication

- **GitHub Issues**: For bugs, features, and general discussion
- **Pull Requests**: For code review and discussion
- **Discussions**: For broader questions and community chat

## Code of Conduct

We expect all contributors to:
- Be respectful and inclusive
- Provide constructive feedback
- Focus on the technical merits of contributions
- Help maintain a welcoming community

## Recognition

Contributors will be recognized in:
- Git commit history
- Release notes for significant contributions
- Project documentation where appropriate

## Questions?

If you have questions about contributing, feel free to:
- Open a GitHub issue with the "question" label
- Start a discussion in the GitHub Discussions section

Thank you for contributing to zeroio!