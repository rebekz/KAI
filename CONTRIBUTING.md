# Contributing to KAI

Hi there! Thank you for even being interested in contributing. As a growing open source project we are open
to contributions, whether they be in the form of new features, integrations with other tools and frameworks, better documentation, or bug fixes.

## Table of Contents

- [Development Environment Setup](#development-environment-setup)
- [Development Workflow](#development-workflow)
- [Code Quality Standards](#code-quality-standards)
- [Testing Guidelines](#testing-guidelines)
- [Pull Request Process](#pull-request-process)
- [Project Structure](#project-structure)

## Development Environment Setup

### Prerequisites

- Python 3.11 or higher
- [uv](https://docs.astral.sh/uv/) package manager
- Docker and Docker Compose (for running services)
- Git

### Initial Setup

1. **Fork and clone the repository**
   ```bash
   git clone https://github.com/yourusername/KAI.git
   cd KAI
   ```

2. **Install uv package manager**
   ```bash
   # On macOS and Linux
   curl -LsSf https://astral.sh/uv/install.sh | sh
   
   # On Windows
   powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
   ```

3. **Install dependencies**
   ```bash
   uv sync
   ```

4. **Set up environment variables**
   ```bash
   cp .env.example .env
   ```
   
   Configure the `.env` file with necessary credentials:
   ```bash
   # Generate encryption key
   uv run python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   ```
   
   Add the generated key to your `.env` file as `ENCRYPT_KEY`.

5. **Start required services**
   ```bash
   docker compose up -d typesense
   ```

### Running the Application

- **Development server**: `uv run python app/main.py`
- **Run tests**: `uv run pytest`
- **Code formatting**: `uv run black .`
- **Linting**: `uv run ruff check .`

## Development Workflow

### Branch Strategy

1. Create a feature branch from `main`:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. Make your changes and commit them:
   ```bash
   git add .
   git commit -m "feat: add new feature description"
   ```

3. Push to your fork and create a pull request:
   ```bash
   git push origin feature/your-feature-name
   ```

### Commit Message Guidelines

Follow conventional commit format:
- `feat:` for new features
- `fix:` for bug fixes
- `docs:` for documentation changes
- `test:` for test additions/modifications
- `refactor:` for code refactoring
- `chore:` for maintenance tasks

## Code Quality Standards

### Python Style Guidelines

- Use Python 3.11+ features
- Follow PEP 8 guidelines
- Use type hints for all function parameters and return values
- Maximum line length: 88 characters (Black default)

### Code Formatting

The project uses the following tools:
- **Black**: Code formatting
- **Ruff**: Linting and import sorting

Run before committing:
```bash
uv run black .
uv run ruff check . --fix
```

### Architecture Patterns

- Follow the repository pattern for data access
- Use dependency injection for services
- Implement proper error handling with custom exceptions
- Use Pydantic models for data validation

## Testing Guidelines

### Test Structure

Tests are organized in the `tests/` directory:
- `test_api/`: API endpoint tests
- `test_integration/`: Integration tests
- `test_modules/`: Unit tests for individual modules

### Running Tests

```bash
# Run all tests
uv run pytest

# Run specific test file
uv run pytest tests/test_modules/test_mysql_connection.py

# Run with coverage
uv run pytest --cov=app

# Run integration tests
uv run pytest tests/test_integration/
```

### Writing Tests

- Use descriptive test names
- Follow AAA pattern (Arrange, Act, Assert)
- Use pytest fixtures for common setup
- Mock external dependencies
- Test both success and failure scenarios

Example test structure:
```python
def test_should_create_database_connection_when_valid_data_provided():
    # Arrange
    connection_data = DatabaseConnectionCreate(...)
    
    # Act
    result = service.create_connection(connection_data)
    
    # Assert
    assert result.id is not None
    assert result.name == connection_data.name
```

## Pull Request Process

### Before Submitting

1. **Ensure all tests pass**:
   ```bash
   uv run pytest
   ```

2. **Run code quality checks**:
   ```bash
   uv run black .
   uv run ruff check .
   ```

3. **Update documentation** if needed

4. **Add tests** for new functionality

### PR Requirements

- [ ] Tests pass
- [ ] Code is properly formatted
- [ ] Documentation is updated
- [ ] PR description explains the changes
- [ ] Breaking changes are documented

### Review Process

1. Automated checks must pass
2. At least one maintainer review required
3. Address review feedback
4. Squash commits before merging

## Project Structure

```
KAI/
├── app/                    # Main application code
│   ├── api/               # API request/response models
│   ├── data/              # Database schemas and storage
│   ├── modules/           # Feature modules
│   │   ├── alias/         # Alias management
│   │   ├── business_glossary/
│   │   ├── context_store/
│   │   └── ...
│   ├── server/            # Server configuration
│   └── utils/             # Utility functions
├── tests/                 # Test files
├── docs/                  # Documentation
├── pyproject.toml         # Project configuration
└── docker-compose.yml     # Docker services
```

### Module Structure

Each module follows this pattern:
```
module_name/
├── models/
│   └── __init__.py       # Pydantic models
├── repositories/
│   └── __init__.py       # Data access layer
└── services/
    └── __init__.py       # Business logic
```

## Getting Help

- Check existing [issues](https://github.com/mta-tech/KAI/issues)
- Read the [documentation](https://mta-3.gitbook.io/kai)
- Ask questions in discussions

## Guidelines

To contribute to this project, please follow a ["fork and pull request"](https://docs.github.com/en/get-started/quickstart/contributing-to-projects) workflow.
Please do not try to push directly to this repo unless you are maintainer.