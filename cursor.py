import sys
from pager import Pager
from btree import BTree, InternalNodeHeader, LeafNodeHeader, NodeType, get_node_type

"""
Cursor abstrats the B-tree structure and provides a way to iterate over the cells in the tree.
Since each table is a b-tree, a cursor can be used to iterate over the cells in the table.

[Low Address] ←→ [High Address]
+-------------------------------------------------------------------+
| Header (Fixed Size)                                               |
| +- Node Type (4B)                                                 |
| +- Is Root (4B)                                                   |
| +- Parent Pointer (4B)                                            |
| +- Number of Cells (4B)                                           |
| +- Allocation Pointer (4B)                                        |
| +- Free List Head Pointer (4B)                                    |
| +- Total Free List Space (4B)                                     |
+-------------------------------------------------------------------+
| Cell Pointer Array (Variable Size)                                |
| +- Cell Pointer 0 (4B) -> points to cell location                 |
| +- Cell Pointer 1 (4B) -> points to cell location                 |
| +- ... (sorted by key)                                            |
+-------------------------------------------------------------------+
| Unallocated Space (Grows towards low addresses)                   |
+-------------------------------------------------------------------+
| Cells (Variable Size, stored at high addresses)                   |
| +- Cell 0 (key_size + data_size + key + data)                     |
| +- Cell 1 (key_size + data_size + key + data)                     |
| +- ...                                                            |
+-------------------------------------------------------------------+
"""
def num_cells(page: bytes) -> int:
    return int.from_bytes(page[16:20], sys.byteorder)

class Cursor:
    def __init__(self, pager: Pager, tree: BTree):
        self.pager = pager
        self.tree = tree
        self.page_num = tree.root_page_num
        self.end_of_table = False
        self.cell_num = 0
        self.navigate_to_first_leaf_node()

    def advance(self):
        if self.end_of_table:
            return

        page = self.pager.get_page(self.page_num)
        header = LeafNodeHeader.from_header(page)

        self.cell_num += 1
        if self.cell_num >= header.num_cells:
            self.navigate_to_next_leaf_node()
            self.cell_num = 0

    def get_cell(self):
        page = self.pager.get_page(self.page_num)
        header = LeafNodeHeader.from_header(page)

        # Check if we're at the end of the current page
        if self.cell_num >= header.num_cells:
            return b''  # Return empty bytes if no more cells

        cell_offset = header.cell_pointers[self.cell_num]
        return page[cell_offset:]

    # private methods
    def navigate_to_first_leaf_node(self, page_num: int = None):
        if page_num is None:
            page_num = self.page_num
        page = self.pager.get_page(page_num)
        while get_node_type(page) != NodeType.LEAF:
            header = InternalNodeHeader.from_header(page)
            page_num = header.children[0]
            page = self.pager.get_page(page_num)
            self.page_num = page_num

    def navigate_to_next_leaf_node(self):
        page = self.pager.get_page(self.page_num)
        node_type = get_node_type(page)
        if node_type == NodeType.LEAF:
            header = LeafNodeHeader.from_header(page)
            parent_page_num = header.parent_page_num
        else:
            header = InternalNodeHeader.from_header(page)
            parent_page_num = header.parent_page_num
        if header.is_root:
            self.end_of_table = True
            return None

        current_page_num = self.page_num
        while True:
            parent_header = InternalNodeHeader.from_header(self.pager.get_page(parent_page_num))
            if current_page_num == parent_header.right_child_page_num:
                if parent_header.is_root:
                    self.end_of_table = True
                    return None
                current_page_num = parent_page_num
                parent_page_num = parent_header.parent_page_num
            else:
                for i, child_page_num in enumerate(parent_header.children):
                    if child_page_num == current_page_num:
                        # If not the last child, go to the next sibling
                        if i + 1 < len(parent_header.children):
                            next_sibling = parent_header.children[i + 1]
                            # Skip invalid page numbers (0)
                            if next_sibling == 0:
                                # Try to find the next valid sibling
                                for j in range(i + 2, len(parent_header.children)):
                                    if parent_header.children[j] != 0:
                                        next_sibling = parent_header.children[j]
                                        break
                                else:
                                    # No valid sibling found, try right_child
                                    if parent_header.right_child_page_num != 0:
                                        next_sibling = parent_header.right_child_page_num
                                    else:
                                        self.end_of_table = True
                                        return None
                            self.page_num = next_sibling
                            self.navigate_to_first_leaf_node(self.page_num)
                            return
                        else:
                            # If last child, go to the right_child_page_num
                            next_sibling = parent_header.right_child_page_num
                            # Skip invalid page numbers (0)
                            if next_sibling == 0:
                                self.end_of_table = True
                                return None
                            self.page_num = next_sibling
                            self.navigate_to_first_leaf_node(self.page_num)
                            return
                self.end_of_table = True
                return None

def test_cursor_single_leaf():
    """Test cursor traversal on a single leaf node"""
    import os
    from record import Record, serialize, deserialize
    from schema.basic_schema import BasicSchema, Column, Integer, Text

    # Create test database
    test_db_file = "test_cursor_single.db"
    if os.path.exists(test_db_file):
        os.remove(test_db_file)

    pager = Pager(test_db_file)
    tree = BTree.new_tree(pager)

    # Create schema and records
    schema = BasicSchema("test_table", [
        Column("id", Integer(), True),
        Column("data", Text(), False)
    ])

    records = [
        Record(values={"id": 1, "data": "first"}, schema=schema),
        Record(values={"id": 2, "data": "second"}, schema=schema),
        Record(values={"id": 3, "data": "third"}, schema=schema)
    ]

    # Insert records
    for record in records:
        cell = serialize(record)
        tree.insert(cell)

    # Test cursor traversal
    cursor = Cursor(pager, tree)
    retrieved_records = []

    while not cursor.end_of_table:
        cell = cursor.get_cell()
        record = deserialize(cell, schema)
        retrieved_records.append(record)
        cursor.advance()

    # Verify all records were retrieved in order
    assert len(retrieved_records) == 3, f"Expected 3 records, got {len(retrieved_records)}"
    for i, record in enumerate(retrieved_records):
        assert record.values["id"] == i + 1, f"Expected id {i+1}, got {record.values['id']}"
        assert record.values["data"] == ["first", "second", "third"][i], f"Expected data {['first', 'second', 'third'][i]}, got {record.values['data']}"

    print("✓ Single leaf cursor test passed!")

    # Cleanup
    pager.close()
    os.remove(test_db_file)


def test_cursor_with_splits():
    """Test cursor traversal on a B-tree with internal nodes (after splits)"""
    import os
    from record import Record, serialize, deserialize
    from schema.basic_schema import BasicSchema, Column, Integer, Text
    from btree import InternalNodeHeader, LeafNodeHeader, NodeType, get_node_type

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
    # Print parent/children info for each leaf node
    def print_leaf_info(page_num):
        page = pager.get_page(page_num)
        if get_node_type(page) == NodeType.LEAF:
            header = LeafNodeHeader.from_header(page)
            if header.parent_page_num != 0:
                parent_page = pager.get_page(header.parent_page_num)
                parent_header = InternalNodeHeader.from_header(parent_page)
        else:
            header = InternalNodeHeader.from_header(page)
            for child in header.children:
                print_leaf_info(child)
            print_leaf_info(header.right_child_page_num)

    print_leaf_info(tree.root_page_num)

    # Test cursor traversal
    cursor = Cursor(pager, tree)
    retrieved_records = []

    while not cursor.end_of_table:
        cell = cursor.get_cell()
        record = deserialize(cell, schema)
        retrieved_records.append(record)
        cursor.advance()

    # Verify all records were retrieved in order
    assert len(retrieved_records) == 7, f"Expected 7 records, got {len(retrieved_records)}"
    for i, record in enumerate(retrieved_records):
        expected_id = i + 1
        assert record.values["id"] == expected_id, f"Expected id {expected_id}, got {record.values['id']}"
        assert record.values["data"] == f"data_{expected_id}", f"Expected data data_{expected_id}, got {record.values['data']}"

    print("✓ Cursor with splits test passed!")

    # Cleanup
    pager.close()
    os.remove(test_db_file)


def test_cursor_empty_tree():
    """Test cursor behavior on an empty B-tree"""
    import os

    # Create test database
    test_db_file = "test_cursor_empty.db"
    if os.path.exists(test_db_file):
        os.remove(test_db_file)

    pager = Pager(test_db_file)
    tree = BTree.new_tree(pager)

    # Test cursor on empty tree
    cursor = Cursor(pager, tree)

    # Should not be at end of table initially
    assert cursor.end_of_table == False, "Cursor should not be at end of table initially"

    # Try to get cell - should return empty data since no cells exist
    cell = cursor.get_cell()
    assert len(cell) == 0, "Cell should be empty on empty tree"

    # Advance should set end_of_table to True since there are no cells
    cursor.advance()
    assert cursor.end_of_table == True, "Cursor should be at end of table after advancing on empty tree"

    print("✓ Empty tree cursor test passed!")

    # Cleanup
    pager.close()
    os.remove(test_db_file)


def test_cursor_navigation():
    """Test cursor navigation methods"""
    import os
    from record import Record, serialize, deserialize
    from schema.basic_schema import BasicSchema, Column, Integer, Text

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

    # Insert records to create internal nodes
    for i in range(1, 6):
        record = Record(values={"id": i, "data": f"data_{i}"}, schema=schema)
        cell = serialize(record)
        tree.insert(cell)

    # Test navigation to first leaf node
    cursor = Cursor(pager, tree)

    # Verify we're at a leaf node
    page = pager.get_page(cursor.page_num)
    assert get_node_type(page) == NodeType.LEAF, "Cursor should navigate to leaf node"

    # Verify we're at the first cell
    assert cursor.cell_num == 0, "Cursor should start at cell 0"

    print("✓ Cursor navigation test passed!")

    # Cleanup
    pager.close()
    os.remove(test_db_file)


def test_cursor_advance():
    """Test cursor advance functionality"""
    import os
    from record import Record, serialize, deserialize
    from schema.basic_schema import BasicSchema, Column, Integer, Text

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

    # Insert 3 records
    for i in range(1, 4):
        record = Record(values={"id": i, "data": f"data_{i}"}, schema=schema)
        cell = serialize(record)
        tree.insert(cell)

    cursor = Cursor(pager, tree)

    # Test advancing through cells
    for i in range(3):
        assert not cursor.end_of_table, f"Should not be at end of table at iteration {i}"
        cell = cursor.get_cell()
        record = deserialize(cell, schema)
        assert record.values["id"] == i + 1, f"Expected id {i+1}, got {record.values['id']}"
        cursor.advance()

    # After advancing through all cells, should be at end of table
    assert cursor.end_of_table == True, "Should be at end of table after advancing through all cells"

    print("✓ Cursor advance test passed!")

    # Cleanup
    pager.close()
    os.remove(test_db_file)


def test_cursor_get_cell():
    """Test cursor get_cell functionality"""
    import os
    from record import Record, serialize, deserialize
    from schema.basic_schema import BasicSchema, Column, Integer, Text

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

    # Insert a record
    record = Record(values={"id": 1, "data": "test_data"}, schema=schema)
    cell = serialize(record)
    tree.insert(cell)

    cursor = Cursor(pager, tree)

    # Test getting cell
    retrieved_cell = cursor.get_cell()
    assert len(retrieved_cell) > 0, "Retrieved cell should not be empty"

    # Verify the cell can be deserialized back to the original record
    retrieved_record = deserialize(retrieved_cell, schema)
    assert retrieved_record.values["id"] == record.values["id"], "Retrieved record should match original"
    assert retrieved_record.values["data"] == record.values["data"], "Retrieved record should match original"

    print("✓ Cursor get_cell test passed!")

    # Cleanup
    pager.close()
    os.remove(test_db_file)


def run_all_cursor_tests():
    """Run all cursor tests"""
    print("Running cursor tests...")

    test_cursor_single_leaf()
    test_cursor_with_splits()
    test_cursor_empty_tree()
    test_cursor_navigation()
    test_cursor_advance()
    test_cursor_get_cell()

    print("All cursor tests passed!")


if __name__ == "__main__":
    run_all_cursor_tests()