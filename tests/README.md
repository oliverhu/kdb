# Tests

This directory contains all the test files for the KDB database implementation.

## Test Files

- `test_btree.py` - Tests for B-tree functionality (headers, pager, insert, splits)
- `test_cursor.py` - Tests for cursor functionality (navigation, traversal)
- `test_record.py` - Tests for record serialization/deserialization
- `test_select.py` - Tests for SELECT functionality

## Running Tests

### Recommended: Use Pytest

From the project root, run:
```bash
pytest tests/
```
Or for more verbose output:
```bash
pytest -v tests/
```

Pytest will automatically discover and run all test files and functions.

### Run Individual Test Files
You can also run a specific test file:
```bash
pytest tests/test_btree.py
pytest tests/test_cursor.py
pytest tests/test_record.py
pytest tests/test_select.py
```

## Test Organization

Each test file follows a consistent structure:
- Individual test functions with descriptive names
- Proper setup and cleanup (creating/removing test database files)
- Clear assertions and error messages
- Can be run independently or as part of the test suite

## Adding New Tests

When adding new tests:
1. Create a new test file following the naming convention `test_*.py`
2. Include proper imports and path setup
3. Add descriptive test function names (starting with `test_`)
4. Include proper cleanup to remove test files
5. Pytest will automatically discover new tests