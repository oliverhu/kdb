#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from virtual_machine import VirtualMachine
from main import ToAst

def test_select():
    # Clean up any existing database
    db_file = "test_select.db"
    if os.path.exists(db_file):
        os.remove(db_file)

    # Create virtual machine
    vm = VirtualMachine(db_file)

    # Test SQL commands
    sql_commands = [
        "CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT, email TEXT);",
        "INSERT INTO users (id, username, email) VALUES (1, 'John Doe', 'john.doe@example.com');",
        "SELECT username, email FROM users;"
    ]

    for cmd in sql_commands:
        print(f"\nExecuting: {cmd}")
        # Parse and execute the command
        from grammar import GRAMMAR
        from lark import Lark
        parser = Lark(GRAMMAR, start='program')
        parse_tree = parser.parse(cmd)
        transformer = ToAst()
        tree = transformer.transform(parse_tree)
        vm.run(tree)

    print("\nTest completed!")