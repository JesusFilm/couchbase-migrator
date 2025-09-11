# Commit Message Guidelines

This project uses [Conventional Commits](https://www.conventionalcommits.org/) to ensure consistent and meaningful commit messages.

## Format

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

## Types

- **feat**: A new feature
- **fix**: A bug fix
- **docs**: Documentation only changes
- **style**: Changes that do not affect the meaning of the code (white-space, formatting, missing semi-colons, etc)
- **refactor**: A code change that neither fixes a bug nor adds a feature
- **perf**: A code change that improves performance
- **test**: Adding missing tests or correcting existing tests
- **build**: Changes that affect the build system or external dependencies
- **ci**: Changes to our CI configuration files and scripts
- **chore**: Other changes that don't modify src or test files
- **revert**: Reverts a previous commit

## Examples

### Good commit messages:

```
feat: add user authentication
fix: resolve memory leak in connection pool
docs: update API documentation
style: format code with prettier
refactor: extract database connection logic
perf: optimize query performance
test: add unit tests for user service
build: update dependencies
ci: add automated testing
chore: update gitignore
```

### Bad commit messages:

```
update stuff
fix bug
changes
WIP
```

## Rules

- Use lowercase for type and scope
- Don't end the description with a period
- Keep the description under 100 characters
- Use the imperative mood ("add feature" not "added feature")
- Reference issues in the footer when applicable

## Enforcement

Commit messages are automatically validated using:

- **Husky**: Git hooks management
- **Commitlint**: Commit message validation (via `pnpm commitlint`)
- **Lint-staged**: Pre-commit linting and formatting (via `pnpm lint-staged`)

If your commit message doesn't follow the conventional commit format, the commit will be rejected.

**Note**: This project uses `pnpm` as the package manager, so all hooks use `pnpm` commands instead of `npx`.
