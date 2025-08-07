#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from virtual_machine import VirtualMachine
from symbols import ToAst
from grammar import GRAMMAR
from lark import Lark


def test_update_sql_parsing():
    """Test UPDATE SQL statement parsing without execution"""
    print("Testing UPDATE SQL parsing...")
    
    parser = Lark(GRAMMAR, start='program')
    transformer = ToAst()
    
    # Test various UPDATE statement syntaxes
    update_statements = [
        "UPDATE users SET email = 'new@example.com';",
        "UPDATE users SET email = 'new@example.com' WHERE id = 1;",
        "UPDATE users SET username = 'John Updated' WHERE id = 2;",
        "UPDATE products SET price = 100 WHERE category = 'Electronics';",
    ]
    
    for stmt in update_statements:
        print(f"Parsing: {stmt}")
        try:
            parse_tree = parser.parse(stmt)
            tree = transformer.transform(parse_tree)
            print(f"✓ Successfully parsed: {type(tree.statements[0]).__name__}")
        except Exception as e:
            print(f"✗ Failed to parse: {e}")
            raise
    
    print("✓ UPDATE SQL parsing tests passed!")


def test_update_sql_syntax():
    """Test UPDATE SQL statement syntax and execution with proper validation"""
    print("\nTesting UPDATE SQL syntax...")
    
    # Clean up any existing database
    db_file = "test_update_sql.db"
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
    initial_data = capture_select_results("SELECT id, username, email FROM users;")
    print(initial_data)
    expected_initial = [
        [1, 'John Doe', 'john.doe@example.com'],
        [2, 'Jane Smith', 'jane.smith@example.com'],
        [3, 'Bob Wilson', 'bob.wilson@example.com']
    ]
    assert initial_data == expected_initial, f"Initial data mismatch. Expected {expected_initial}, got {initial_data}"
    print("✓ Initial data verified correctly")

    # Test 1: UPDATE specific record with WHERE clause
    print("\n--- Test 1: UPDATE specific record with WHERE clause ---")
    update_cmd = "UPDATE users SET email = 'john.updated@example.com' WHERE id = 1;"
    print(f"Executing: {update_cmd}")
    parse_tree = parser.parse(update_cmd)
    tree = transformer.transform(parse_tree)
    vm.run(tree)

    # Verify update
    after_update = capture_select_results("SELECT id, username, email FROM users;")
    print(after_update)
    expected_after_update = [
        [1, 'John Doe', 'john.updated@example.com'],
        [2, 'Jane Smith', 'jane.smith@example.com'],
        [3, 'Bob Wilson', 'bob.wilson@example.com']
    ]
    # Sort both lists by id to handle B-tree reordering
    after_update_sorted = sorted(after_update, key=lambda x: x[0])
    expected_after_update_sorted = sorted(expected_after_update, key=lambda x: x[0])
    assert after_update_sorted == expected_after_update_sorted, f"After update mismatch. Expected {expected_after_update_sorted}, got {after_update_sorted}"
    print("✓ Specific record successfully updated")

    # Test 2: UPDATE single column
    print("\n--- Test 2: UPDATE single column ---")
    update_cmd = "UPDATE users SET username = 'Jane Updated' WHERE id = 2;"
    print(f"Executing: {update_cmd}")
    parse_tree = parser.parse(update_cmd)
    tree = transformer.transform(parse_tree)
    vm.run(tree)

    # Verify single column update
    after_single_update = capture_select_results("SELECT id, username, email FROM users;")
    print(after_single_update)
    expected_after_single = [
        [1, 'John Doe', 'john.updated@example.com'],
        [2, 'Jane Updated', 'jane.smith@example.com'],  # username updated, email unchanged
        [3, 'Bob Wilson', 'bob.wilson@example.com']
    ]
    # Sort both lists by id to handle B-tree reordering
    after_single_update_sorted = sorted(after_single_update, key=lambda x: x[0])
    expected_after_single_sorted = sorted(expected_after_single, key=lambda x: x[0])
    assert after_single_update_sorted == expected_after_single_sorted, f"After single update mismatch. Expected {expected_after_single_sorted}, got {after_single_update_sorted}"
    print("✓ Single column successfully updated")

    # Test 3: UPDATE without WHERE clause (should update all records)
    print("\n--- Test 3: UPDATE without WHERE clause ---")
    update_cmd = "UPDATE users SET email = 'all.updated@example.com';"
    print(f"Executing: {update_cmd}")
    parse_tree = parser.parse(update_cmd)
    tree = transformer.transform(parse_tree)
    vm.run(tree)

    # Verify all records updated
    after_all_update = capture_select_results("SELECT id, username, email FROM users;")
    print(after_all_update)
    expected_after_all = [
        [1, 'John Doe', 'all.updated@example.com'],
        [2, 'Jane Updated', 'all.updated@example.com'],
        [3, 'Bob Wilson', 'all.updated@example.com']
    ]
    # Sort both lists by id to handle B-tree reordering
    after_all_update_sorted = sorted(after_all_update, key=lambda x: x[0])
    expected_after_all_sorted = sorted(expected_after_all, key=lambda x: x[0])
    assert after_all_update_sorted == expected_after_all_sorted, f"After all update mismatch. Expected {expected_after_all_sorted}, got {after_all_update_sorted}"
    print("✓ All records successfully updated")

    # Test 4: UPDATE with complex WHERE condition
    print("\n--- Test 4: UPDATE with complex WHERE condition ---")
    update_cmd = "UPDATE users SET username = 'Bob Updated' WHERE id > 2;"
    print(f"Executing: {update_cmd}")
    parse_tree = parser.parse(update_cmd)
    tree = transformer.transform(parse_tree)
    vm.run(tree)

    # Verify complex condition update
    after_complex_update = capture_select_results("SELECT id, username, email FROM users;")
    print(after_complex_update)
    expected_after_complex = [
        [1, 'John Doe', 'all.updated@example.com'],
        [2, 'Jane Updated', 'all.updated@example.com'],
        [3, 'Bob Updated', 'all.updated@example.com']
    ]
    # Sort both lists by id to handle B-tree reordering
    after_complex_update_sorted = sorted(after_complex_update, key=lambda x: x[0])
    expected_after_complex_sorted = sorted(expected_after_complex, key=lambda x: x[0])
    assert after_complex_update_sorted == expected_after_complex_sorted, f"After complex update mismatch. Expected {expected_after_complex_sorted}, got {after_complex_update_sorted}"
    print("✓ Complex WHERE condition update successful")

    # Test 5: Verify that non-matching WHERE conditions don't update records
    print("\n--- Test 5: Verify non-matching WHERE conditions ---")
    update_cmd = "UPDATE users SET username = 'Should Not Change' WHERE id = 999;"
    print(f"Executing: {update_cmd}")
    parse_tree = parser.parse(update_cmd)
    tree = transformer.transform(parse_tree)
    vm.run(tree)

    # Verify no changes for non-matching condition
    after_no_match = capture_select_results("SELECT id, username, email FROM users;")
    print(after_no_match)
    # Should be the same as before since no records match id = 999
    # Sort both lists by id to handle B-tree reordering
    after_no_match_sorted = sorted(after_no_match, key=lambda x: x[0])
    expected_after_complex_sorted = sorted(expected_after_complex, key=lambda x: x[0])
    assert after_no_match_sorted == expected_after_complex_sorted, f"No match update should not change data. Expected {expected_after_complex_sorted}, got {after_no_match_sorted}"
    print("✓ Non-matching WHERE condition correctly handled")

    # Clean up
    vm.state_manager.pager.close()
    if os.path.exists(db_file):
        os.remove(db_file)

    print("\n✓ UPDATE SQL syntax tests completed with proper validation!")


def test_update_sql_comprehensive():
    """Test comprehensive UPDATE scenarios with proper validation"""
    print("\nTesting comprehensive UPDATE scenarios...")
    
    # Clean up any existing database
    db_file = "test_update_comprehensive.db"
    if os.path.exists(db_file):
        os.remove(db_file)

    # Create virtual machine
    vm = VirtualMachine(db_file)
    parser = Lark(GRAMMAR, start='program')
    transformer = ToAst()

    # Test setup: Create table and insert test data
    setup_commands = [
        "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, category TEXT, price INTEGER);",
        "INSERT INTO products (id, name, category, price) VALUES (1, 'Laptop', 'Electronics', 1200);",
        "INSERT INTO products (id, name, category, price) VALUES (2, 'Mouse', 'Electronics', 50);",
        "INSERT INTO products (id, name, category, price) VALUES (3, 'Book', 'Books', 25);",
        "INSERT INTO products (id, name, category, price) VALUES (4, 'Tablet', 'Electronics', 800);",
        "INSERT INTO products (id, name, category, price) VALUES (5, 'Pen', 'Office', 5);"
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
    initial_data = capture_select_results("SELECT id, name, category, price FROM products;")
    print(initial_data)
    expected_initial = [
        [1, 'Laptop', 'Electronics', 1200],
        [2, 'Mouse', 'Electronics', 50],
        [3, 'Book', 'Books', 25],
        [4, 'Tablet', 'Electronics', 800],
        [5, 'Pen', 'Office', 5]
    ]
    assert initial_data == expected_initial, f"Initial data mismatch. Expected {expected_initial}, got {initial_data}"
    print("✓ Initial data verified correctly")

    # Test 1: UPDATE by category (multiple records)
    print("\n--- Test 1: UPDATE by category (multiple records) ---")
    update_cmd = "UPDATE products SET price = 1000 WHERE category = 'Electronics';"
    print(f"Executing: {update_cmd}")
    parse_tree = parser.parse(update_cmd)
    tree = transformer.transform(parse_tree)
    vm.run(tree)

    # Verify category update
    after_category_update = capture_select_results("SELECT id, name, category, price FROM products;")
    print(after_category_update)
    expected_after_category = [
        [1, 'Laptop', 'Electronics', 1000],  # updated
        [2, 'Mouse', 'Electronics', 1000],   # updated
        [3, 'Book', 'Books', 25],            # unchanged
        [4, 'Tablet', 'Electronics', 1000],  # updated
        [5, 'Pen', 'Office', 5]              # unchanged
    ]
    # Sort both lists by id to handle B-tree reordering
    after_category_update_sorted = sorted(after_category_update, key=lambda x: x[0])
    expected_after_category_sorted = sorted(expected_after_category, key=lambda x: x[0])
    assert after_category_update_sorted == expected_after_category_sorted, f"After category update mismatch. Expected {expected_after_category_sorted}, got {after_category_update_sorted}"
    print("✓ Category-based update successful")

    # Test 2: UPDATE with price range condition
    print("\n--- Test 2: UPDATE with price range condition ---")
    update_cmd = "UPDATE products SET category = 'Premium' WHERE price > 1000;"
    print(f"Executing: {update_cmd}")
    parse_tree = parser.parse(update_cmd)
    tree = transformer.transform(parse_tree)
    vm.run(tree)

    # Verify price range update
    after_price_update = capture_select_results("SELECT id, name, category, price FROM products;")
    print(after_price_update)
    expected_after_price = [
        [1, 'Laptop', 'Electronics', 1000],  # price = 1000, so price > 1000 is false
        [2, 'Mouse', 'Electronics', 1000],   # unchanged
        [3, 'Book', 'Books', 25],            # unchanged
        [4, 'Tablet', 'Electronics', 1000],  # unchanged
        [5, 'Pen', 'Office', 5]              # unchanged
    ]
    # Sort both lists by id to handle B-tree reordering
    after_price_update_sorted = sorted(after_price_update, key=lambda x: x[0])
    expected_after_price_sorted = sorted(expected_after_price, key=lambda x: x[0])
    assert after_price_update_sorted == expected_after_price_sorted, f"After price update mismatch. Expected {expected_after_price_sorted}, got {after_price_update_sorted}"
    print("✓ Price range update successful")

    # Test 3: UPDATE specific record
    print("\n--- Test 3: UPDATE specific record ---")
    update_cmd = "UPDATE products SET name = 'Gaming Laptop' WHERE id = 1;"
    print(f"Executing: {update_cmd}")
    parse_tree = parser.parse(update_cmd)
    tree = transformer.transform(parse_tree)
    vm.run(tree)

    # Verify specific record update
    after_specific_update = capture_select_results("SELECT id, name, category, price FROM products;")
    print(after_specific_update)
    expected_after_specific = [
        [1, 'Gaming Laptop', 'Electronics', 1000],  # name updated
        [2, 'Mouse', 'Electronics', 1000],          # unchanged
        [3, 'Book', 'Books', 25],                   # unchanged
        [4, 'Tablet', 'Electronics', 1000],         # unchanged
        [5, 'Pen', 'Office', 5]                     # unchanged
    ]
    # Sort both lists by id to handle B-tree reordering
    after_specific_update_sorted = sorted(after_specific_update, key=lambda x: x[0])
    expected_after_specific_sorted = sorted(expected_after_specific, key=lambda x: x[0])
    assert after_specific_update_sorted == expected_after_specific_sorted, f"After specific update mismatch. Expected {expected_after_specific_sorted}, got {after_specific_update_sorted}"
    print("✓ Specific record update successful")

    # Test 4: UPDATE with multiple conditions
    print("\n--- Test 4: UPDATE with multiple conditions ---")
    update_cmd = "UPDATE products SET price = 0 WHERE category = 'Electronics' AND price < 200;"
    print(f"Executing: {update_cmd}")
    parse_tree = parser.parse(update_cmd)
    tree = transformer.transform(parse_tree)
    vm.run(tree)

    # Verify multiple condition update
    after_multi_condition = capture_select_results("SELECT id, name, category, price FROM products;")
    print(after_multi_condition)
    expected_after_multi_condition = [
        [1, 'Gaming Laptop', 'Electronics', 1000],  # unchanged (price >= 200)
        [2, 'Mouse', 'Electronics', 1000],          # unchanged (price >= 200)
        [3, 'Book', 'Books', 25],                   # unchanged
        [4, 'Tablet', 'Electronics', 1000],         # unchanged (price >= 200)
        [5, 'Pen', 'Office', 5]                     # unchanged
    ]
    # Sort both lists by id to handle B-tree reordering
    after_multi_condition_sorted = sorted(after_multi_condition, key=lambda x: x[0])
    expected_after_multi_condition_sorted = sorted(expected_after_multi_condition, key=lambda x: x[0])
    assert after_multi_condition_sorted == expected_after_multi_condition_sorted, f"After multi-condition update mismatch. Expected {expected_after_multi_condition_sorted}, got {after_multi_condition_sorted}"
    print("✓ Multiple condition update successful")

    # Test 5: UPDATE all records
    print("\n--- Test 5: UPDATE all records ---")
    update_cmd = "UPDATE products SET category = 'General';"
    print(f"Executing: {update_cmd}")
    parse_tree = parser.parse(update_cmd)
    tree = transformer.transform(parse_tree)
    vm.run(tree)

    # Verify all records update
    after_all_records = capture_select_results("SELECT id, name, category, price FROM products;")
    print(after_all_records)
    expected_after_all_records = [
        [1, 'Gaming Laptop', 'General', 1000],
        [2, 'Mouse', 'General', 1000],
        [3, 'Book', 'General', 25],
        [4, 'Tablet', 'General', 1000],
        [5, 'Pen', 'General', 5]
    ]
    # Sort both lists by id to handle B-tree reordering
    after_all_records_sorted = sorted(after_all_records, key=lambda x: x[0])
    expected_after_all_records_sorted = sorted(expected_after_all_records, key=lambda x: x[0])
    assert after_all_records_sorted == expected_after_all_records_sorted, f"After all records update mismatch. Expected {expected_after_all_records_sorted}, got {after_all_records_sorted}"
    print("✓ All records update successful")

    # Clean up
    vm.state_manager.pager.close()
    if os.path.exists(db_file):
        os.remove(db_file)

    print("\n✓ Comprehensive UPDATE tests completed with proper validation!")


if __name__ == "__main__":
    test_update_sql_parsing()
    test_update_sql_syntax()
    test_update_sql_comprehensive()
    print("\n✓ All UPDATE SQL tests passed!") 