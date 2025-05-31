# Contributing to zeroio

We welcome all contributions to make this project better.

## Code Ownership and Licensing

**Important**: By contributing code to this project, you agree that your contributions become part of the project and can be used by anyone according to the project's license terms. Your code will be available under the same license as the rest of the project.

## Ways to Contribute

- Code: Bug fixes, features, performance improvements
- Documentation: READMEs, comments, examples
- Testing: Writing tests, reporting bugs
- Design: Protocol and API improvements

## Getting Started

### Development Setup

```bash
git clone https://github.com/SithraBot/zeroio.git
cd zeroio
cargo build
cargo test --workspace
```

### Making Changes

```bash
git checkout -b feature/your-feature-name
# Make changes, add tests, update docs
cargo test --workspace
cargo clippy --workspace
cargo fmt --check
```

## Code Guidelines

### Code Standards

- Follow `cargo fmt` formatting
- Address all `cargo clippy --workspace` warnings
- Write tests for new features
- Document public APIs
- Use meaningful names

The project uses custom clippy and rustfmt configurations for consistency.

### Protocol Changes
- Maintain backward compatibility
- Update `draft.md` specification
- Consider cross-language implications

## Submitting Changes

### Pull Requests

- Update docs for API changes
- Add tests for your changes
- Ensure all tests pass locally
- Write clear commit messages
- Keep PRs focused on single features
- Describe what changed and why

## Reporting Issues

### Bug Reports
Include:
- OS and Rust version (`rustc --version`)
- Steps to reproduce
- Expected vs actual behavior

### Feature Requests
Describe:
- Use case and how it works
- Alternatives considered
- Whether you'll implement it

## Communication

Use GitHub Issues for bugs and features, Pull Requests for code review.

## Code of Conduct

Be respectful, provide constructive feedback, focus on technical merits.