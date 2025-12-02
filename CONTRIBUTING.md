# Contributing to The Pipeline Framework

Thank you for considering contributing to The Pipeline Framework! This document outlines the guidelines for contributing to this project.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [How Can I Contribute?](#how-can-i-contribute)
- [Development Setup](#development-setup)
- [Coding Standards](#coding-standards)
- [Testing Guidelines](#testing-guidelines)
- [Pull Request Process](#pull-request-process)
- [Style Guidelines](#style-guidelines)
- [Release Process](#release-process)

## Code of Conduct

This project and everyone participating in it is governed by our [Code of Conduct](CODE_OF_CONDUCT.md). By 
participating, you are expected to uphold this code. Please report unacceptable behavior to [team@pipelineframework.org](mailto:team@pipelineframework.org).

## How Can I Contribute?

### Reporting Bugs
- Use the GitHub issue tracker to report bugs
- Provide detailed information about the environment, steps to reproduce, and expected vs. actual behavior
- Check existing issues before creating a new one

### Suggesting Enhancements
- Use the GitHub issue tracker for enhancement requests
- Clearly explain the enhancement and its use cases
- Consider the impact on the existing codebase and users

### Code Contributions
- Fix bugs or implement new features
- Improve documentation
- Add tests or improve test coverage

## Development Setup

### Prerequisites
- Java 21 or higher
- Maven 3.8.6 or higher
- Git

### Getting Started
1. Fork the repository
2. Clone your fork:
   ```bash
   git clone https://github.com/your-username/pipelineframework.git
   cd pipelineframework
   ```
3. Set up the upstream remote:
   ```bash
   git remote add upstream https://github.com/mbarcia/pipelineframework.git
   ```
4. Build the project:
   ```bash
   ./mvnw clean install
   ```

### Project Structure
- `framework/` - Main framework implementation (runtime and deployment modules)
- `examples/` - Example applications showing framework usage
- `docs/` - Documentation files
- `.github/workflows/` - GitHub Actions workflows

## Coding Standards

### General Guidelines
- Follow the existing code style in the project
- Use meaningful variable and method names
- Write clear, concise comments where necessary
- Follow Java best practices and conventions
- Use Java 21 features where appropriate

### Code Formatting
- Use Google Java Format with AOSP style
- Run `./mvnw spotless:apply` to format your code before committing
- All formatting is checked during the build process

### Dependencies
- Only add dependencies that are truly necessary
- Prefer existing dependencies when possible
- All POM files must be 100% declarative (no conditional logic)

## Testing Guidelines

Please refer to our [TESTING.md](TESTING.md) document for detailed testing guidelines. Here's a summary:

- **Unit Tests**: Name ends with `Test`, runs with Surefire, covered by Jacoco
- **Quarkus Tests**: Use `@QuarkusTest`, name ends with `Test`, runs with Surefire, covered by Jacoco
- **Integration Tests**: Name ends with `IT`, runs with Failsafe, not covered by Jacoco
- **Quarkus Integration Tests**: Use `@QuarkusIntegrationTest`, name ends with `IT`, runs with Failsafe, not covered by Jacoco

### Test Decision Tree
1. Pure business logic? → Unit test (name ends with `Test`)
2. Needs Quarkus runtime, DI, config, serialization? → `@QuarkusTest` (name ends with `Test`)
3. Requires containers, external services, or full bootstrap? → `@QuarkusIntegrationTest` (name ends with `IT`)

## Pull Request Process

1. Ensure any install or build dependencies are removed before the end of the layer when doing a build
2. Update the README.md with details of changes to the interface, if applicable
3. Add tests for any new functionality
4. Ensure all tests pass
5. Update documentation as needed
6. Follow the testing guidelines in [TESTING.md](TESTING.md)
7. Submit your pull request against the `main` branch

### Before Submitting
- Run `./mvnw clean verify` to ensure all tests pass
- Run `./mvnw spotless:check` to ensure code formatting is correct
- Verify your changes work as expected

### Pull Request Checklist
- [ ] Code follows project standards
- [ ] Appropriate unit tests added/updated
- [ ] Documentation updated if necessary
- [ ] All CI checks pass

## Style Guidelines

### Documentation
- Use clear, concise language
- Follow the existing documentation style
- Include examples where helpful
- Update related documentation when making API changes

### Git Commit Messages
- Use the present tense ("Add feature" not "Added feature")
- Use the imperative mood ("Move cursor to..." not "Moves cursor to...")
- Limit the first line to 72 characters or fewer
- Reference issues and pull requests after a blank line, if applicable

## Release Process

The Pipeline Framework uses an automated release process with GitHub Actions. Please see our [PUBLISHING.md](docs/PUBLISHING.md) document for details about the release process.

### For Maintainers
When your pull request is merged to main, the maintainers will handle the release process using the Maven Release Plugin.

## Questions?

If you have questions not covered in this document or the other documentation files, feel free to open an issue with the "question" label.

---

Thank you again for your contribution! 🎉