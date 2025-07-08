#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cursor import Cursor
from btree import BTree, NodeType, InternalNodeHeader, LeafNodeHeader, get_node_type
from pager import Pager
from record import Record, serialize, deserialize, cell_size
from schema.basic_schema import BasicSchema, Column
from schema.datatypes import Integer, Text


def test_single_leaf_cursor():
    """Test cursor on a single leaf node"""
    print("Testing single leaf cursor...")

    # Create test database
    test_db_file = "test_single_leaf.db"
    if os.path.exists(test_db_file):
        os.remove(test_db_file)

    pager = Pager(test_db_file)
    tree = BTree.new_tree(pager)

    # Create schema and records
    schema = BasicSchema("test_table", [
        Column("id", Integer(), True),
        Column("data", Text(), False)
    ])

    # Insert test records
    for i in range(1, 4):
        record = Record(values={"id": i, "data": f"data_{i}"}, schema=schema)
        cell = serialize(record)
        tree.insert(cell)

    # Create cursor and test navigation
    cursor = Cursor(pager, tree)
    cursor.navigate_to_first_leaf_node()

    # Should be at the first leaf node
    assert cursor.page_num == 0, f"Expected page 0, got {cursor.page_num}"
    assert not cursor.end_of_table, "Should not be at end of table"

    # Test next navigation (should reach end since only one leaf)
    cursor.navigate_to_next_leaf_node()
    assert cursor.end_of_table, "Should be at end of table after single leaf"

    # Clean up
    pager.close()
    os.remove(test_db_file)

    print("✓ Single leaf cursor test passed!")


def test_cursor_with_splits():
    """Test cursor traversal on a B-tree with internal nodes (after splits)"""
    print("Testing cursor with splits...")

    # Create test database
    test_db_file = "test_cursor_splits.db"
    if os.path.exists(test_db_file):
        os.remove(test_db_file)

    pager = Pager(test_db_file)
    tree = BTree.new_tree(pager)

    # Create schema and records
    schema = BasicSchema("test_table", [
        Column("id", Integer(), True),
        Column("data", Text(), False)
    ])

    # Insert enough records to trigger splits
    records = []
    for i in range(1, 8):  # Insert 7 records to create internal nodes
        record = Record(values={"id": i, "data": f"data_{i}"}, schema=schema)
        records.append(record)
        cell = serialize(record)
        tree.insert(cell)

    # Print B-tree structure
    print("B-tree structure after inserts:")
    print(tree)

    # Print parent/children info for each leaf node
    def print_leaf_info(page_num):
        page = pager.get_page(page_num)
        if get_node_type(page) == NodeType.LEAF:
            header = LeafNodeHeader.from_header(page)
            print(f"Leaf page {page_num}: parent={header.parent_page_num}, num_cells={header.num_cells}")
            if header.parent_page_num != 0:
                parent_page = pager.get_page(header.parent_page_num)
                parent_header = InternalNodeHeader.from_header(parent_page)
                print(f"    Parent {header.parent_page_num}: children={parent_header.children}, right_child={parent_header.right_child_page_num}")
        else:
            header = InternalNodeHeader.from_header(page)
            for child in header.children:
                print_leaf_info(child)
            print_leaf_info(header.right_child_page_num)

    # Print info for all leaf nodes
    root_page = pager.get_page(tree.root_page_num)
    if get_node_type(root_page) == NodeType.INTERNAL:
        root_header = InternalNodeHeader.from_header(root_page)
        for child in root_header.children:
            print_leaf_info(child)
        print_leaf_info(root_header.right_child_page_num)
    else:
        print_leaf_info(tree.root_page_num)

    # Create cursor and retrieve all records
    cursor = Cursor(pager, tree)
    retrieved_records = []

    # Navigate to first leaf
    cursor.navigate_to_first_leaf_node()

    while not cursor.end_of_table:
        # Get all cells from current leaf
        page = pager.get_page(cursor.page_num)
        header = LeafNodeHeader.from_header(page)

        for cell_pointer in header.cell_pointers:
            cell_data = page[cell_pointer:]
            cell_size_val = cell_size(cell_data)
            cell = page[cell_pointer:cell_pointer + cell_size_val]
            record = deserialize(cell, schema)
            retrieved_records.append(record)

        # Move to next leaf
        cursor.navigate_to_next_leaf_node()

    # Verify we got all records
    assert len(retrieved_records) == 7, f"Expected 7 records, got {len(retrieved_records)}"

    # Verify records are in order
    for i, record in enumerate(retrieved_records):
        expected_id = i + 1
        assert record.values["id"] == expected_id, f"Record {i} has wrong id: {record.values['id']} != {expected_id}"

    # Clean up
    pager.close()
    os.remove(test_db_file)

    print("✓ Cursor with splits test passed!")


def test_empty_tree_cursor():
    """Test cursor on an empty tree"""
    print("Testing empty tree cursor...")

    # Create test database
    test_db_file = "test_empty_cursor.db"
    if os.path.exists(test_db_file):
        os.remove(test_db_file)

    pager = Pager(test_db_file)
    tree = BTree.new_tree(pager)

    # Create cursor on empty tree
    cursor = Cursor(pager, tree)
    cursor.navigate_to_first_leaf_node()

    # Should be at the first leaf node (even if empty)
    assert cursor.page_num == 0, f"Expected page 0, got {cursor.page_num}"

    # Test next navigation (should reach end immediately)
    cursor.navigate_to_next_leaf_node()
    assert cursor.end_of_table, "Should be at end of table for empty tree"

    # Clean up
    pager.close()
    os.remove(test_db_file)

    print("✓ Empty tree cursor test passed!")


def test_cursor_navigation():
    """Test cursor navigation between leaf nodes"""
    print("Testing cursor navigation...")

    # Create test database
    test_db_file = "test_cursor_nav.db"
    if os.path.exists(test_db_file):
        os.remove(test_db_file)

    pager = Pager(test_db_file)
    tree = BTree.new_tree(pager)

    # Create schema and records
    schema = BasicSchema("test_table", [
        Column("id", Integer(), True),
        Column("data", Text(), False)
    ])

    # Insert records to create multiple leaf nodes
    for i in range(1, 5):
        record = Record(values={"id": i, "data": f"data_{i}"}, schema=schema)
        cell = serialize(record)
        tree.insert(cell)

    # Create cursor and test navigation
    cursor = Cursor(pager, tree)
    cursor.navigate_to_first_leaf_node()

    # Should be at the first leaf node
    assert cursor.page_num == 0, f"Expected page 0, got {cursor.page_num}"

    # Navigate to next leaf
    cursor.navigate_to_next_leaf_node()
    # Should be at the next leaf node (after split)
    assert cursor.page_num != 0, "Should have moved to different page"
    assert not cursor.end_of_table, "Should not be at end of table"

    # Clean up
    pager.close()
    os.remove(test_db_file)

    print("✓ Cursor navigation test passed!")


def test_cursor_advance():
    """Test cursor advance functionality"""
    print("Testing cursor advance...")

    # Create test database
    test_db_file = "test_cursor_advance.db"
    if os.path.exists(test_db_file):
        os.remove(test_db_file)

    pager = Pager(test_db_file)
    tree = BTree.new_tree(pager)

    # Create schema and records
    schema = BasicSchema("test_table", [
        Column("id", Integer(), True),
        Column("data", Text(), False)
    ])

    # Insert test records
    for i in range(1, 4):
        record = Record(values={"id": i, "data": f"data_{i}"}, schema=schema)
        cell = serialize(record)
        tree.insert(cell)

    # Create cursor and test advance
    cursor = Cursor(pager, tree)
    cursor.navigate_to_first_leaf_node()

    # Should be at the first leaf node
    assert cursor.page_num == 0, f"Expected page 0, got {cursor.page_num}"

    # Test advance (should reach end after all records)
    # Advance through all 3 records
    for i in range(3):
        assert not cursor.end_of_table, f"Should not be at end of table at iteration {i}"
        cursor.advance()

    # After advancing through all cells, should be at end of table
    assert cursor.end_of_table, "Should be at end of table after advancing through all records"

    # Clean up
    pager.close()
    os.remove(test_db_file)

    print("✓ Cursor advance test passed!")


def test_cursor_get_cell():
    """Test cursor get_cell functionality"""
    print("Testing cursor get_cell...")

    # Create test database
    test_db_file = "test_cursor_get_cell.db"
    if os.path.exists(test_db_file):
        os.remove(test_db_file)

    pager = Pager(test_db_file)
    tree = BTree.new_tree(pager)

    # Create schema and records
    schema = BasicSchema("test_table", [
        Column("id", Integer(), True),
        Column("data", Text(), False)
    ])

    # Insert test record
    record = Record(values={"id": 1, "data": "test_data"}, schema=schema)
    cell = serialize(record)
    tree.insert(cell)

    # Create cursor and test get_cell
    cursor = Cursor(pager, tree)
    cursor.navigate_to_first_leaf_node()

    # Get cell and verify
    cell_data = cursor.get_cell()
    retrieved_record = deserialize(cell_data, schema)

    assert retrieved_record.values["id"] == record.values["id"], "Cell id mismatch"
    assert retrieved_record.values["data"] == record.values["data"], "Cell data mismatch"

    # Clean up
    pager.close()
    os.remove(test_db_file)

    print("✓ Cursor get_cell test passed!")