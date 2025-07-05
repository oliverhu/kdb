"""
┌─────────────────────────────────────────────────────────────┐
│                    Table B-tree                             │
├─────────────────────────────────────────────────────────────┤
│ Root Page (Internal Node)                                   │
│ ├─ Node Type: Internal                                      │
│ ├─ Is Root: True                                            │
│ ├─ Parent Pointer: Self                                     │
│ ├─ Number of Keys: N                                        │
│ ├─ Right Child Pointer: Page X                              │
│ └─ [Child0, Key0, Child1, Key1, ..., ChildN, KeyN]          │
├─────────────────────────────────────────────────────────────┤
│ Leaf Pages (Data Storage)                                   │
│ ├─ Node Type: Leaf                                          │
│ ├─ Number of Cells: M                                       │
│ ├─ Allocation Pointer: Y                                    │
│ ├─ Cell Pointers: [ptr0, ptr1, ..., ptrM]                   │
│ └─ Cells: [cell0, cell1, ..., cellM]                        │
│     └─ Each cell: [key_size, data_size, key, data]          │
└─────────────────────────────────────────────────────────────┘
"""
from enum import Enum, auto
import os
from pager import DatabaseFileHeader, Pager
from typing import List

from record import Record, deserialize, deserialize_key, serialize
from schema.basic_schema import BasicSchema, Column
from schema.datatypes import Integer, Text

type Cell = bytes

INTERNAL_NODE_MAX_KEYS = 3
LEAF_NODE_MAX_CELLS = 3

class NodeType(Enum):
    INTERNAL = 0
    LEAF = 1


class InternalNodeHeader:
    """
    The header of an internal node in the B-tree.
    """
    def __init__(self, node_type: NodeType,
                 is_root: bool,
                 parent_page_num: int,
                 num_keys: int,
                 right_child_page_num: int,
                 keys: list[int],
                 children: list[int]):
        self.node_type = node_type
        self.is_root = is_root
        self.parent_page_num = parent_page_num
        self.num_keys = num_keys
        self.right_child_page_num = right_child_page_num
        self.keys = keys
        self.children = children

    @staticmethod
    def from_header(header: bytes):
        node_type = NodeType(Integer.deserialize(header[0:4]))
        is_root = Integer.deserialize(header[4:8]) == 1
        parent_page_num = Integer.deserialize(header[8:12])
        num_keys = Integer.deserialize(header[12:16])
        right_child_page_num = Integer.deserialize(header[16:20])
        keys = []
        for i in range(num_keys):
            keys.append(Integer.deserialize(header[20 + i * 4:24 + i * 4]))
        children = []
        children_offset = 20 + num_keys * 4
        for i in range(num_keys + 1):
            children.append(Integer.deserialize(header[children_offset + i * 4:children_offset + (i + 1) * 4]))
        return InternalNodeHeader(node_type, is_root, parent_page_num, num_keys, right_child_page_num, keys, children)

    def to_header(self):
        return Integer.serialize(self.node_type.value) + Integer.serialize(1 if self.is_root else 0) + Integer.serialize(self.parent_page_num) + Integer.serialize(self.num_keys) + Integer.serialize(self.right_child_page_num) + b"".join(Integer.serialize(key) for key in self.keys) + b"".join(Integer.serialize(child) for child in self.children)

class LeafNodeHeader:
    """
    The header of a page in the B-tree.
    """
    def __init__(self, is_root: bool, parent_page_num: int, num_cells: int, allocation_pointer: int, cell_pointers: list[int]):
        self.node_type = NodeType.LEAF
        self.is_root = is_root
        self.num_cells = num_cells
        self.allocation_pointer = allocation_pointer
        self.parent_page_num = parent_page_num
        self.cell_pointers = cell_pointers

    @staticmethod
    def from_header(header: bytes):
        is_root = Integer.deserialize(header[4:8]) == 1
        parent_page_num = Integer.deserialize(header[8:12])
        num_cells = Integer.deserialize(header[12:16])
        allocation_pointer = Integer.deserialize(header[16:20])
        cell_pointers = []
        for i in range(num_cells):
            cell_pointers.append(Integer.deserialize(header[20 + i * 4:24 + i * 4]))
        return LeafNodeHeader(is_root, parent_page_num, num_cells, allocation_pointer, cell_pointers)

    def to_header(self):
        return Integer.serialize(self.node_type.value) + Integer.serialize(1 if self.is_root else 0) + Integer.serialize(self.parent_page_num) + Integer.serialize(self.num_cells) + Integer.serialize(self.allocation_pointer) + b"".join(Integer.serialize(cell_pointer) for cell_pointer in self.cell_pointers)

    def __str__(self):
        return f"LeafNodeHeader(node_type={self.node_type}, is_root={self.is_root}, parent_page_num={self.parent_page_num}, num_cells={self.num_cells}, allocation_pointer={self.allocation_pointer}, cell_pointers={self.cell_pointers})"

"""
Each time we create a table, we create a new B-tree.
The root page points to the unused page in the file, it is either
a released page or a new page (append to the end of the file).

Inside the BTree (B+ tree to be more accurate), there are two types of nodes:
- Internal nodes:
    - Each internal node has a fixed number of keys and children.
    - The keys are used to determine the range of keys that each child node contains.
    - The children are pointers to the child nodes.
- Leaf nodes:
    - Each leaf node has a fixed number of cells.

Root node
- the initial root node is a leaf node
- after insertion beyond capacity, the root node is split
into an intenral node (as root) and two leaf nodes.

"""
class BTree:
    def __init__(self, pager: Pager, root_page_num: int):
        self.pager = pager
        self.root_page_num = root_page_num

    # public APIs
    def find(self, key: int, page_num: int = None) -> Cell:
        # recursively find the page that contains the key
        if page_num is None:
            page_num = self.root_page_num
        cell = self.pager.get_page(page_num)
        header = LeafNodeHeader.from_header(cell)
        print(f"header: {header}")
        if header.node_type == NodeType.LEAF:
            return cell
        else:
            # For internal nodes, we need to find the appropriate child
            # This is a simplified implementation - in a real B-tree,
            # we would compare the key with the keys in the internal node
            # to determine which child to traverse
            return self.find(key, header.right_child_page_num)

    @staticmethod
    def new_tree(pager: Pager):
        # create a new root node (leaf node).
        root_page_num = pager.get_free_page()
        root_page = bytearray(pager.page_size)
        root_header = LeafNodeHeader(is_root=True, parent_page_num=0, num_cells=0, allocation_pointer=0, cell_pointers=[])
        root_header.to_header()
        pager.write_page(root_page_num, bytes(root_page))
        return BTree(pager, root_page_num)

    def insert(self, record: Record, page_num: int = None):
        """
        Insert a record into the specified page.
        The record is serialized and added to the page, updating the page header accordingly.
        Returns a tuple of (position, length) where the record was stored.
        """
        if page_num is None:
            page_num = self.root_page_num

        # Get the current page
        page = bytearray(self.pager.get_page(page_num))

        # Parse the page header
        header = LeafNodeHeader.from_header(page)

        # Serialize the record
        record_bytes = serialize(record)
        record_length = len(record_bytes)

        # Calculate position for the new record (from end of page)
        record_pos = self.pager.page_size - record_length

        # If this is not the first record, adjust position to avoid overlap
        if header.num_cells > 0:
            # Find the minimum cell pointer to ensure we don't overlap
            min_cell_pointer = min(header.cell_pointers)
            record_pos = min_cell_pointer - record_length

        # Update the page header
        header.num_cells += 1
        header.cell_pointers.append(record_pos)
        header.allocation_pointer = record_pos

        # Write the updated header back to the page
        header_bytes = header.to_header()
        page[:len(header_bytes)] = header_bytes

        # Write the record data to the page
        page[record_pos:record_pos + record_length] = record_bytes

        # Write the updated page back to disk
        self.pager.write_page(page_num, bytes(page))

        return (record_pos, record_length)

    def delete(self, key: int):
        pass

    # private APIs


def test_page_header():
    # Test serialization/deserialization of page header
    original_header = LeafNodeHeader(
        is_root=True,
        parent_page_num=0,
        num_cells=3,
        allocation_pointer=100,
        cell_pointers=[200, 300, 400]
    )
    serialized = original_header.to_header()
    deserialized = LeafNodeHeader.from_header(serialized)

    assert deserialized.node_type == original_header.node_type, "Node type mismatch"
    assert deserialized.is_root == original_header.is_root, "Is root mismatch"
    assert deserialized.parent_page_num == original_header.parent_page_num, "Parent page num mismatch"
    assert deserialized.num_cells == original_header.num_cells, "Num cells mismatch"
    assert deserialized.allocation_pointer == original_header.allocation_pointer, "Allocation pointer mismatch"
    assert deserialized.cell_pointers == original_header.cell_pointers, "Cell pointers mismatch"

    # Test with different values
    header2 = LeafNodeHeader(
        is_root=False,
        parent_page_num=5,
        num_cells=0,
        allocation_pointer=50,
        cell_pointers=[]
    )
    serialized2 = header2.to_header()
    deserialized2 = LeafNodeHeader.from_header(serialized2)

    assert deserialized2.is_root == header2.is_root
    assert deserialized2.parent_page_num == header2.parent_page_num
    assert deserialized2.num_cells == header2.num_cells
    assert deserialized2.allocation_pointer == header2.allocation_pointer
    assert deserialized2.cell_pointers == header2.cell_pointers

    print("All page header tests passed!")


def test_file_header():
    # Test serialization/deserialization of file header
    original_header = DatabaseFileHeader(version="kdb000", next_free_page=10, has_free_list=False)
    serialized = original_header.to_header()
    deserialized = DatabaseFileHeader.from_header(serialized)

    assert deserialized.version == original_header.version, "Version mismatch"
    assert deserialized.next_free_page == original_header.next_free_page, "Next free page mismatch"
    assert deserialized.has_free_list == original_header.has_free_list, "Has free list mismatch"

    # Test with different values
    header2 = DatabaseFileHeader(version="kdb000", next_free_page=0, has_free_list=True)
    serialized2 = header2.to_header()
    deserialized2 = DatabaseFileHeader.from_header(serialized2)

    assert deserialized2.version == header2.version
    assert deserialized2.next_free_page == header2.next_free_page
    assert deserialized2.has_free_list == header2.has_free_list

    print("All file header tests passed!")


def test_pager():
    # Test creating a new database file
    test_db_file = "test.db"
    if os.path.exists(test_db_file):
        os.remove(test_db_file)

    # Create new pager and verify file header
    pager = Pager(test_db_file)
    tree = BTree.new_tree(pager)
    assert pager.file_header.version == "kdb000"
    assert pager.file_header.next_free_page == 0
    assert pager.file_header.has_free_list == False

    # Create a simple schema for testing
    schema = BasicSchema("test_table", [
        Column("id", Integer(), True),
        Column("data", Text(), False)
    ])

    # Create test records/cells
    record1 = Record(values={"id": 1, "data": "test data 1"}, schema=schema)
    record2 = Record(values={"id": 2, "data": "test data 2"}, schema=schema)

    # Insert records using the new insert function
    pos1, len1 = tree.insert(record1)
    pos2, len2 = tree.insert(record2)

    print(f"Inserted records at positions: {pos1} (length: {len1}), {pos2} (length: {len2})")

    # Read page and verify records can be read back
    cell = tree.find(1)
    read_header = LeafNodeHeader.from_header(cell)

    print(f"Page header: num_cells={read_header.num_cells}, cell_pointers={read_header.cell_pointers}")

    # Verify records can be read back
    read_record1 = deserialize(cell[read_header.cell_pointers[0]:read_header.cell_pointers[0]+len1], schema)
    read_record2 = deserialize(cell[read_header.cell_pointers[1]:read_header.cell_pointers[1]+len2], schema)

    assert read_record1.values["id"] == record1.values["id"]
    assert read_record1.values["data"] == record1.values["data"]
    assert read_record2.values["id"] == record2.values["id"]
    assert read_record2.values["data"] == record2.values["data"]

    print(f"Record 1 verified: {read_record1.values}")
    print(f"Record 2 verified: {read_record2.values}")

    # Test reading non-existent page returns empty page
    empty_page = pager.read_page(999)
    assert len(empty_page) == pager.page_size
    assert all(b == 0 for b in empty_page)

    # Clean up
    pager.close()
    os.remove(test_db_file)

    print("All pager tests passed!")


def test_insert():
    # Test the insert function
    test_db_file = "test_insert.db"
    if os.path.exists(test_db_file):
        os.remove(test_db_file)

    # Create new pager
    pager = Pager(test_db_file)
    tree = BTree(pager, 0)

    # Create a simple schema for testing
    schema = BasicSchema("test_table", [
        Column("id", Integer(), True),
        Column("data", Text(), False)
    ])

    # Create test records
    record1 = Record(values={"id": 1, "data": "first record"}, schema=schema)
    record2 = Record(values={"id": 2, "data": "second record"}, schema=schema)
    record3 = Record(values={"id": 3, "data": "third record"}, schema=schema)

    # Insert records into page 1
    pos1, len1 = tree.insert(record1, 1)
    pos2, len2 = tree.insert(record2, 1)
    pos3, len3 = tree.insert(record3, 1)

    print(f"Inserted records at positions: {pos1} (length: {len1}), {pos2} (length: {len2}), {pos3} (length: {len3})")

    # Read the page back and verify
    page = pager.read_page(1)
    header = LeafNodeHeader.from_header(page)

    print(f"Page header: num_cells={header.num_cells}, cell_pointers={header.cell_pointers}")

    # Verify all records can be read back
    for i, (expected_record, expected_len) in enumerate([(record1, len1), (record2, len2), (record3, len3)]):
        cell_pos = header.cell_pointers[i]
        read_record = deserialize(page[cell_pos:cell_pos + expected_len], schema)

        assert read_record.values["id"] == expected_record.values["id"], f"Record {i} id mismatch"
        assert read_record.values["data"] == expected_record.values["data"], f"Record {i} data mismatch"
        print(f"Record {i} verified: {read_record.values}")

    # Clean up
    pager.close()
    os.remove(test_db_file)

    print("All insert tests passed!")


def test_internal_node_header():
    # Test serialization/deserialization of internal node header
    original_header = InternalNodeHeader(
        node_type=NodeType.INTERNAL,
        is_root=True,
        parent_page_num=0,
        num_keys=2,
        right_child_page_num=5,
        keys=[10, 20],
        children=[1, 3, 6]
    )
    print(f"Original header: keys={original_header.keys}, children={original_header.children}")
    serialized = original_header.to_header()
    print(f"Serialized length: {len(serialized)} bytes")

    # Debug: manually check the serialized data
    print(f"Serialized bytes: {serialized[:30]}...")  # Show first 30 bytes

    deserialized = InternalNodeHeader.from_header(serialized)
    print(f"Deserialized header: keys={deserialized.keys}, children={deserialized.children}")

    assert deserialized.node_type == original_header.node_type, "Node type mismatch"
    assert deserialized.is_root == original_header.is_root, "Is root mismatch"
    assert deserialized.parent_page_num == original_header.parent_page_num, "Parent page num mismatch"
    assert deserialized.num_keys == original_header.num_keys, "Number of keys mismatch"
    assert deserialized.right_child_page_num == original_header.right_child_page_num, "Right child page num mismatch"
    assert deserialized.keys == original_header.keys, "Keys mismatch"
    assert deserialized.children == original_header.children, "Children mismatch"

    # Test with different values - empty internal node
    header2 = InternalNodeHeader(
        node_type=NodeType.INTERNAL,
        is_root=False,
        parent_page_num=2,
        num_keys=0,
        right_child_page_num=0,
        keys=[],
        children=[0]
    )
    serialized2 = header2.to_header()
    deserialized2 = InternalNodeHeader.from_header(serialized2)

    assert deserialized2.node_type == header2.node_type, "Node type mismatch"
    assert deserialized2.is_root == header2.is_root, "Is root mismatch"
    assert deserialized2.parent_page_num == header2.parent_page_num, "Parent page num mismatch"
    assert deserialized2.num_keys == header2.num_keys, "Number of keys mismatch"
    assert deserialized2.right_child_page_num == header2.right_child_page_num, "Right child page num mismatch"
    assert deserialized2.keys == header2.keys, "Keys mismatch"
    assert deserialized2.children == header2.children, "Children mismatch"

    # Test with maximum keys (INTERNAL_NODE_MAX_CELLS)
    header3 = InternalNodeHeader(
        node_type=NodeType.INTERNAL,
        is_root=False,
        parent_page_num=1,
        num_keys=INTERNAL_NODE_MAX_KEYS,
        right_child_page_num=10,
        keys=[5, 15, 25],
        children=[2, 4, 6, 8]
    )
    serialized3 = header3.to_header()
    deserialized3 = InternalNodeHeader.from_header(serialized3)

    assert deserialized3.node_type == header3.node_type, "Node type mismatch"
    assert deserialized3.is_root == header3.is_root, "Is root mismatch"
    assert deserialized3.parent_page_num == header3.parent_page_num, "Parent page num mismatch"
    assert deserialized3.num_keys == header3.num_keys, "Number of keys mismatch"
    assert deserialized3.right_child_page_num == header3.right_child_page_num, "Right child page num mismatch"
    assert deserialized3.keys == header3.keys, "Keys mismatch"
    assert deserialized3.children == header3.children, "Children mismatch"

    print("All internal node header tests passed!")


if __name__ == "__main__":
    test_file_header()
    test_page_header()
    test_internal_node_header()
    test_pager()
    test_insert()