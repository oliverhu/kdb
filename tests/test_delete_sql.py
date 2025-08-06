#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from virtual_machine import VirtualMachine
from symbols import ToAst
from grammar import GRAMMAR
from lark import Lark


def test_delete_sql_syntax():
    """Test DELETE SQL statement syntax and execution"""
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
    vm.run(tree)

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
    vm.run(tree)

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
    vm.run(tree)

    # Clean up
    vm.state_manager.pager.close()
    if os.path.exists(db_file):
        os.remove(db_file)

    print("\n✓ DELETE SQL syntax tests completed!")


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
    print("\n✓ All DELETE SQL tests passed!") 