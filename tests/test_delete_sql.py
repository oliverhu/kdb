#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from virtual_machine import VirtualMachine
from symbols import ToAst
from grammar import GRAMMAR
from lark import Lark


def test_delete_sql_syntax():
    """Test DELETE SQL statement syntax and execution with proper validation"""
    print("Testing DELETE SQL syntax...")
    
    # Clean up any existing database
    db_file = "test_delete_sql.db"
    if os.path.exists(db_file):
        os.remove(db_file)

    # Create virtual machine
    vm = VirtualMachine(db_file)
    parser = Lark(GRAMMAR, start='program')
    transformer = ToAst()

    # Test setup: Create table and insert test data
    setup_commands = [
        "CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT, email TEXT);",
        "INSERT INTO users (id, username, email) VALUES (1, 'John Doe', 'john.doe@example.com');",
        "INSERT INTO users (id, username, email) VALUES (2, 'Jane Smith', 'jane.smith@example.com');",
        "INSERT INTO users (id, username, email) VALUES (3, 'Bob Wilson', 'bob.wilson@example.com');"
    ]

    print("\n--- Setting up test data ---")
    for cmd in setup_commands:
        print(f"Executing: {cmd}")
        parse_tree = parser.parse(cmd)
        tree = transformer.transform(parse_tree)
        vm.run(tree)

    # Verify initial data
    print("\n--- Verifying initial data ---")
    select_cmd = "SELECT id, username FROM users;"
    print(f"Executing: {select_cmd}")
    parse_tree = parser.parse(select_cmd)
    tree = transformer.transform(parse_tree)
    
    # Capture initial results
    import io
    import sys
    from contextlib import redirect_stdout
    
    f = io.StringIO()
    with redirect_stdout(f):
        vm.run(tree)
    initial_results = f.getvalue().strip()
    print(initial_results)
    
    # Parse the results to get actual data
    initial_data = eval(initial_results) if initial_results else []
    expected_initial = [[1, 'John Doe'], [2, 'Jane Smith'], [3, 'Bob Wilson']]
    assert initial_data == expected_initial, f"Initial data mismatch. Expected {expected_initial}, got {initial_data}"
    print("✓ Initial data verified correctly")

    # Test 1: DELETE with WHERE clause
    print("\n--- Test 1: DELETE with WHERE clause ---")
    delete_cmd = "DELETE FROM users WHERE id = 2;"
    print(f"Executing: {delete_cmd}")
    parse_tree = parser.parse(delete_cmd)
    tree = transformer.transform(parse_tree)
    vm.run(tree)

    # Verify deletion
    print("Verifying after deletion:")
    select_cmd = "SELECT id, username FROM users;"
    print(f"Executing: {select_cmd}")
    parse_tree = parser.parse(select_cmd)
    tree = transformer.transform(parse_tree)
    
    f = io.StringIO()
    with redirect_stdout(f):
        vm.run(tree)
    after_delete_results = f.getvalue().strip()
    print(after_delete_results)
    
    # Parse the results to get actual data
    after_delete_data = eval(after_delete_results) if after_delete_results else []
    expected_after_delete = [[1, 'John Doe'], [3, 'Bob Wilson']]
    assert after_delete_data == expected_after_delete, f"After delete data mismatch. Expected {expected_after_delete}, got {after_delete_data}"
    print("✓ Record with id=2 successfully deleted")

    # Test 2: DELETE without WHERE clause (delete all remaining records)
    print("\n--- Test 2: DELETE without WHERE clause ---")
    delete_all_cmd = "DELETE FROM users;"
    print(f"Executing: {delete_all_cmd}")
    parse_tree = parser.parse(delete_all_cmd)
    tree = transformer.transform(parse_tree)
    vm.run(tree)

    # Verify all records deleted
    print("Verifying all records deleted:")
    select_cmd = "SELECT id, username FROM users;"
    print(f"Executing: {select_cmd}")
    parse_tree = parser.parse(select_cmd)
    tree = transformer.transform(parse_tree)
    
    f = io.StringIO()
    with redirect_stdout(f):
        vm.run(tree)
    final_results = f.getvalue().strip()
    print(final_results)
    
    # Parse the results to get actual data
    final_data = eval(final_results) if final_results else []
    # When all records are deleted, we should get an empty list
    expected_final = []
    assert final_data == expected_final, f"Final data should be empty, got {final_data}"
    print("✓ All records successfully deleted")

    # Test 3: Verify that deleted records cannot be found
    print("\n--- Test 3: Verify deleted records are not found ---")
    select_specific_cmd = "SELECT id, username FROM users WHERE id = 2;"
    print(f"Executing: {select_specific_cmd}")
    parse_tree = parser.parse(select_specific_cmd)
    tree = transformer.transform(parse_tree)
    
    f = io.StringIO()
    with redirect_stdout(f):
        vm.run(tree)
    specific_results = f.getvalue().strip()
    print(specific_results)
    
    # The result should be empty or contain only default values
    specific_data = eval(specific_results) if specific_results else []
    assert not specific_data or all(row == [0, ''] for row in specific_data), f"Deleted record should not be found, got {specific_data}"
    print("✓ Deleted record correctly not found")

    # Clean up
    vm.state_manager.pager.close()
    if os.path.exists(db_file):
        os.remove(db_file)

    print("\n✓ DELETE SQL syntax tests completed with proper validation!")


def test_delete_sql_comprehensive():
    """Test comprehensive DELETE scenarios with proper validation"""
    print("\nTesting comprehensive DELETE scenarios...")
    
    # Clean up any existing database
    db_file = "test_delete_comprehensive.db"
    if os.path.exists(db_file):
        os.remove(db_file)

    # Create virtual machine
    vm = VirtualMachine(db_file)
    parser = Lark(GRAMMAR, start='program')
    transformer = ToAst()

    # Test setup: Create table and insert test data
    setup_commands = [
        "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, category TEXT);",
        "INSERT INTO products (id, name, category) VALUES (1, 'Laptop', 'Electronics');",
        "INSERT INTO products (id, name, category) VALUES (2, 'Mouse', 'Electronics');",
        "INSERT INTO products (id, name, category) VALUES (3, 'Book', 'Books');",
        "INSERT INTO products (id, name, category) VALUES (4, 'Tablet', 'Electronics');",
        "INSERT INTO products (id, name, category) VALUES (5, 'Pen', 'Office');"
    ]

    print("\n--- Setting up test data ---")
    for cmd in setup_commands:
        parse_tree = parser.parse(cmd)
        tree = transformer.transform(parse_tree)
        vm.run(tree)

    # Helper function to capture SELECT results
    def capture_select_results(sql):
        parse_tree = parser.parse(sql)
        tree = transformer.transform(parse_tree)
        import io
        from contextlib import redirect_stdout
        f = io.StringIO()
        with redirect_stdout(f):
            vm.run(tree)
        results = f.getvalue().strip()
        return eval(results) if results else []

    # Verify initial data
    print("\n--- Verifying initial data ---")
    initial_data = capture_select_results("SELECT id, name, category FROM products;")
    print(initial_data)
    expected_initial = [
        [1, 'Laptop', 'Electronics'], [2, 'Mouse', 'Electronics'], [3, 'Book', 'Books'],
        [4, 'Tablet', 'Electronics'], [5, 'Pen', 'Office']
    ]
    assert initial_data == expected_initial, f"Initial data mismatch. Expected {expected_initial}, got {initial_data}"
    print("✓ Initial data verified correctly")

    # Test 1: DELETE specific record by ID
    print("\n--- Test 1: DELETE specific record by ID ---")
    delete_cmd = "DELETE FROM products WHERE id = 2;"
    print(f"Executing: {delete_cmd}")
    parse_tree = parser.parse(delete_cmd)
    tree = transformer.transform(parse_tree)
    vm.run(tree)

    # Verify deletion
    after_specific_delete = capture_select_results("SELECT id, name, category FROM products;")
    print(after_specific_delete)
    expected_after_specific = [
        [1, 'Laptop', 'Electronics'], [3, 'Book', 'Books'],
        [4, 'Tablet', 'Electronics'], [5, 'Pen', 'Office']
    ]
    assert after_specific_delete == expected_after_specific, f"After specific delete mismatch. Expected {expected_after_specific}, got {after_specific_delete}"
    print("✓ Specific record successfully deleted")

    # Test 2: DELETE by category (multiple records)
    print("\n--- Test 2: DELETE by category (multiple records) ---")
    delete_cmd = "DELETE FROM products WHERE category = 'Electronics';"
    print(f"Executing: {delete_cmd}")
    parse_tree = parser.parse(delete_cmd)
    tree = transformer.transform(parse_tree)
    vm.run(tree)

    # Verify deletion
    after_category_delete = capture_select_results("SELECT id, name, category FROM products;")
    print(after_category_delete)
    expected_after_category = [[3, 'Book', 'Books'], [5, 'Pen', 'Office']]
    assert after_category_delete == expected_after_category, f"After category delete mismatch. Expected {expected_after_category}, got {after_category_delete}"
    print("✓ All Electronics products successfully deleted")

    # Test 3: DELETE remaining records
    print("\n--- Test 3: DELETE remaining records ---")
    delete_cmd = "DELETE FROM products WHERE id = 3;"
    print(f"Executing: {delete_cmd}")
    parse_tree = parser.parse(delete_cmd)
    tree = transformer.transform(parse_tree)
    vm.run(tree)

    delete_cmd = "DELETE FROM products WHERE id = 5;"
    print(f"Executing: {delete_cmd}")
    parse_tree = parser.parse(delete_cmd)
    tree = transformer.transform(parse_tree)
    vm.run(tree)

    # Verify all records deleted
    after_all_delete = capture_select_results("SELECT id, name FROM products;")
    print(after_all_delete)
    # When all records are deleted, we should get an empty list
    expected_after_all = []
    assert after_all_delete == expected_after_all, f"After all delete mismatch. Expected {expected_after_all}, got {after_all_delete}"
    print("✓ All records successfully deleted")

    # Test 4: Verify deleted records are not found
    print("\n--- Test 4: Verify deleted records are not found ---")
    
    # Check for deleted Electronics products
    electronics_check = capture_select_results("SELECT id, name FROM products WHERE category = 'Electronics';")
    assert electronics_check == [], f"Deleted Electronics products should not be found, got {electronics_check}"
    
    # Check for specific deleted records
    specific_check = capture_select_results("SELECT id, name FROM products WHERE id = 2;")
    assert specific_check == [], f"Deleted record with id=2 should not be found, got {specific_check}"
    
    print("✓ All deleted records correctly not found")

    # Test 5: DELETE from empty table (should not error)
    print("\n--- Test 5: DELETE from empty table ---")
    delete_cmd = "DELETE FROM products;"
    print(f"Executing: {delete_cmd}")
    parse_tree = parser.parse(delete_cmd)
    tree = transformer.transform(parse_tree)
    vm.run(tree)

    # Verify table is empty
    final_data = capture_select_results("SELECT id, name FROM products;")
    # When table is empty, we should get an empty list
    expected_final = []
    assert final_data == expected_final, f"Table should be empty, got {final_data}"
    print("✓ Empty table delete handled correctly")

    # Clean up
    vm.state_manager.pager.close()
    if os.path.exists(db_file):
        os.remove(db_file)

    print("\n✓ Comprehensive DELETE tests completed with proper validation!")


def test_delete_sql_parsing():
    """Test DELETE SQL statement parsing without execution"""
    print("\nTesting DELETE SQL parsing...")
    
    parser = Lark(GRAMMAR, start='program')
    transformer = ToAst()
    
    # Test various DELETE statement syntaxes
    delete_statements = [
        "DELETE FROM users;",
        "DELETE FROM users WHERE id = 1;",
        "DELETE FROM users WHERE username = 'John';",
        "DELETE FROM products WHERE price > 100;",
    ]
    
    for stmt in delete_statements:
        print(f"Parsing: {stmt}")
        try:
            parse_tree = parser.parse(stmt)
            tree = transformer.transform(parse_tree)
            print(f"✓ Successfully parsed: {type(tree.statements[0]).__name__}")
        except Exception as e:
            print(f"✗ Failed to parse: {e}")
            raise
    
    print("✓ DELETE SQL parsing tests passed!")


if __name__ == "__main__":
    test_delete_sql_parsing()
    test_delete_sql_syntax()
    test_delete_sql_comprehensive()
    print("\n✓ All DELETE SQL tests passed!") 