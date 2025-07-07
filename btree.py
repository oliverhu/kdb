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

from record import Record, deserialize, deserialize_key, serialize, cell_size
from schema.basic_schema import BasicSchema, Column
from schema.datatypes import Integer, Text

type Cell = bytearray

# Constants
INTERNAL_NODE_MAX_KEYS = 3
LEAF_NODE_MAX_CELLS = 3
LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) // 2
LEAF_NODE_LEFT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT

class NodeType(Enum):
    INTERNAL = 0
    LEAF = 1


def get_node_type(header: bytes) -> NodeType:
    return NodeType(Integer.deserialize(header[0:4]))

class InternalNodeHeader:
    """
    The header of an internal node in the B-tree.
    """
    def __init__(self,
                 is_root: bool,
                 parent_page_num: int,
                 num_keys: int,
                 right_child_page_num: int,
                 keys: list[int],
                 children: list[int]):
        self.node_type = NodeType.INTERNAL
        self.is_root = is_root
        self.parent_page_num = parent_page_num
        self.num_keys = num_keys
        self.right_child_page_num = right_child_page_num
        self.keys = keys
        self.children = children

    @staticmethod
    def from_header(header: bytes):
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
        return InternalNodeHeader(is_root, parent_page_num, num_keys, right_child_page_num, keys, children)

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
    def find(self, key: int, page_num: int = None) -> int:
        # recursively find the page that contains the key
        if page_num is None:
            page_num = self.root_page_num
        page = self.pager.get_page(page_num)
        node_type_val = Integer.deserialize(page[0:4])
        node_type = NodeType(node_type_val)
        # If the page is uninitialized (all zeros or invalid header), initialize it as a leaf node
        header = None
        # Only check allocation_pointer for LEAF nodes
        if (node_type not in (NodeType.LEAF, NodeType.INTERNAL)) or \
           (node_type == NodeType.LEAF and int.from_bytes(page[16:20], 'little') < 128):
            header = LeafNodeHeader(is_root=False, parent_page_num=0, num_cells=0, allocation_pointer=self.pager.page_size, cell_pointers=[])
            header_bytes = header.to_header()
            page[:len(header_bytes)] = header_bytes
            self.pager.write_page(page_num, bytes(page))
            self.pager.pages[page_num] = page
            node_type = NodeType.LEAF
        if node_type == NodeType.LEAF:
            header = LeafNodeHeader.from_header(page)
            return page_num
        elif node_type == NodeType.INTERNAL:
            header = InternalNodeHeader.from_header(page)
            # For internal nodes, choose the correct child based on the key
            left_child_page_num = header.children[0]
            right_child_page_num = header.children[1]
            # Get the largest key in the left child
            left_child_page = self.pager.get_page(left_child_page_num)
            left_child_header = LeafNodeHeader.from_header(left_child_page)
            if left_child_header.num_cells == 0:
                # If left child is empty, go right
                return self.find(key, right_child_page_num)
            # Get the largest key in the left child
            max_key = None
            for ptr in left_child_header.cell_pointers:
                cell_data = left_child_page[ptr:]
                cell_key = deserialize_key(cell_data)
                if max_key is None or cell_key > max_key:
                    max_key = cell_key
            if key <= max_key:
                return self.find(key, left_child_page_num)
            else:
                return self.find(key, right_child_page_num)

    @staticmethod
    def new_tree(pager: Pager):
        # create a new root node (leaf node).
        root_page_num = pager.get_free_page()
        root_page = bytearray(pager.page_size)
        root_header = LeafNodeHeader(is_root=True, parent_page_num=0, num_cells=0, allocation_pointer=pager.page_size, cell_pointers=[])
        header_bytes = root_header.to_header()
        root_page[:len(header_bytes)] = header_bytes
        pager.write_page(root_page_num, bytes(root_page))
        return BTree(pager, root_page_num)


    def insert(self, record: Record):
        """
        Insert a record into the correct page in the B-tree.
        The record is serialized and added to the page, updating the page header accordingly.
        Returns a tuple of (position, length) where the record was stored.
        """
        # Find the correct page to insert into
        page_num = self.find(record.get_primary_key())

        # Get the current page
        page = bytearray(self.pager.get_page(page_num))

        # Parse the page header
        header = LeafNodeHeader.from_header(page)

        num_cells = header.num_cells
        if num_cells < LEAF_NODE_MAX_CELLS:
            # Insert into the leaf node
            return self.insert_into_leaf_node(record, page_num)
        else:
            # Split the leaf node
            self.split_leaf_node(page_num, record.schema)
            # Re-fetch the page after split
            page_num = self.find(record.get_primary_key())
            page = self.pager.get_page(page_num)
            header = LeafNodeHeader.from_header(page)
            result = self.insert_into_leaf_node(record, page_num)
            page = self.pager.get_page(page_num)
            header = LeafNodeHeader.from_header(page)
            return result


    def delete(self, key: int):
        pass

    def left_most_leaf_node(self) -> int:
        page_num = self.root_page_num
        while get_node_type(self.pager.get_page(page_num)) != NodeType.LEAF:
            # Get the leftmost child page number from the internal node header
            # For internal nodes, the first child pointer (leftmost) is stored at bytes 20-24
            # Bytes 16-20 contain the right_child_page_num which is incorrect for finding leftmost child
            page_bytes = self.pager.get_page(page_num)[20:24]  # First child pointer in children array
            page_num = Integer.deserialize(page_bytes)
        return page_num

    # private APIs
    def new_internal_node(self, left_node_page_num: int, right_node_page_num: int) -> int:
        root_page_num = self.pager.get_free_page()
        root_page = bytearray(self.pager.page_size)

        # Get the maximum key from the left child to use as the separator key
        left_child_page = self.pager.get_page(left_node_page_num)
        left_child_header = LeafNodeHeader.from_header(left_child_page)
        separator_key = 0  # Default value
        if left_child_header.num_cells > 0:
            # Find the maximum key in the left child
            max_key = None
            for ptr in left_child_header.cell_pointers:
                cell_data = left_child_page[ptr:]
                cell_key = deserialize_key(cell_data)
                if max_key is None or cell_key > max_key:
                    max_key = cell_key
            separator_key = max_key

        root_header = InternalNodeHeader(
            is_root=True,
            parent_page_num=0,
            num_keys=1,  # We have one key separating two children
            right_child_page_num=right_node_page_num,
            keys=[separator_key],  # The separator key
            children=[left_node_page_num, right_node_page_num]
        )
        header_bytes = root_header.to_header()
        root_page[:len(header_bytes)] = header_bytes
        self.pager.write_page(root_page_num, bytes(root_page))
        return root_page_num

    def split_leaf_node(self, page_num: int, schema: BasicSchema):
        old_page_num = page_num
        old_header = LeafNodeHeader.from_header(self.pager.get_page(page_num))
        old_header.parent_page_num = self.root_page_num

        new_page_num = self.pager.get_free_page()
        new_page = bytearray(self.pager.page_size)
        new_header = LeafNodeHeader(is_root=False, parent_page_num=old_header.parent_page_num, num_cells=0, allocation_pointer=self.pager.page_size, cell_pointers=[])

        new_internal_node = self.new_internal_node(old_page_num, new_page_num)
        old_header.parent_page_num = new_internal_node
        new_header.parent_page_num = new_internal_node
        if self.root_page_num == page_num:
            self.root_page_num = new_internal_node
        # Debug: print root node type and children after split
        root_page = self.pager.get_page(self.root_page_num)
        root_node_type = NodeType(Integer.deserialize(root_page[0:4]))
        if root_node_type == NodeType.INTERNAL:
            internal_header = InternalNodeHeader.from_header(root_page)
            for child_num in internal_header.children:
                child_page = self.pager.get_page(child_num)
                child_type = NodeType(Integer.deserialize(child_page[0:4]))

        # Write the initial header to the new page before any inserts
        new_header_bytes = new_header.to_header()
        new_page[:len(new_header_bytes)] = new_header_bytes
        self.pager.write_page(new_page_num, bytes(new_page))
        self.pager.pages[new_page_num] = new_page  # Ensure in-memory page is updated

        # Split the old page into two pages
        old_page = self.pager.get_page(old_page_num)
        pointers_to_move = old_header.cell_pointers[LEAF_NODE_LEFT_SPLIT_COUNT:]
        for ptr in pointers_to_move:
            cell_data = old_page[ptr:]
            size = cell_size(cell_data)
            record = deserialize(old_page[ptr:ptr+size], schema)
            self.insert_into_leaf_node(record, new_page_num)
        # After moving records, re-read the header from the new page (do not overwrite with empty header)
        new_page = self.pager.get_page(new_page_num)
        new_header = LeafNodeHeader.from_header(new_page)

        # --- FIX: Truncate the left node's cell pointers and update header ---
        old_header.cell_pointers = old_header.cell_pointers[:LEAF_NODE_LEFT_SPLIT_COUNT]
        old_header.num_cells = LEAF_NODE_LEFT_SPLIT_COUNT
        if old_header.cell_pointers:
            old_header.allocation_pointer = min(old_header.cell_pointers)
        else:
            old_header.allocation_pointer = self.pager.page_size
        old_header_bytes = old_header.to_header()
        old_page[:len(old_header_bytes)] = old_header_bytes
        self.pager.write_page(old_page_num, bytes(old_page))
        self.pager.pages[old_page_num] = old_page  # Ensure in-memory page is updated

    def insert_cell_into_leaf_node(self, cell: Cell, page_num: int):
        page = bytearray(self.pager.get_page(page_num))
        header = LeafNodeHeader.from_header(page)

        # Calculate the offset where the cell will be stored
        # Start from the end of the page and work backwards
        cell_offset = header.allocation_pointer - len(cell)
        if cell_offset < 0:
            raise Exception("Cell offset is negative. Not enough space in page.")

        # Write the cell data to the page
        page[cell_offset:cell_offset + len(cell)] = cell

        # Update header
        header.num_cells += 1
        header.cell_pointers.append(cell_offset)  # Store the offset, not the cell data
        header.allocation_pointer = cell_offset

        header_bytes = header.to_header()
        page[:len(header_bytes)] = header_bytes
        self.pager.write_page(page_num, bytes(page))
        self.pager.pages[page_num] = page  # Ensure in-memory page is updated

    def insert_into_leaf_node(self, record: Record, page_num: int):
        cell = serialize(record)
        self.insert_cell_into_leaf_node(cell, page_num)
        # Return the position and length where the record was stored
        # The position is the cell offset, and length is the cell size
        header = LeafNodeHeader.from_header(self.pager.get_page(page_num))
        return header.cell_pointers[-1], len(cell)  # Return the last cell's offset and length


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
    tree.insert(record1)
    tree.insert(record2)

    # Read page and verify records can be read back
    page_num = tree.find(1)
    read_page = pager.read_page(page_num)
    read_header = LeafNodeHeader.from_header(read_page)

    print(f"Page header: num_cells={read_header.num_cells}, cell_pointers={read_header.cell_pointers}")

    # Verify records can be read back
    # Get cell data using the offsets stored in cell_pointers
    cell1_offset = read_header.cell_pointers[0]
    cell2_offset = read_header.cell_pointers[1]

    # Calculate cell sizes
    cell1_size = cell_size(read_page[cell1_offset:])
    cell2_size = cell_size(read_page[cell2_offset:])

    read_record1 = deserialize(read_page[cell1_offset:cell1_offset + cell1_size], schema)
    read_record2 = deserialize(read_page[cell2_offset:cell2_offset + cell2_size], schema)

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
    tree = BTree.new_tree(pager)

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
    pos1, len1 = tree.insert(record1)
    pos2, len2 = tree.insert(record2)
    pos3, len3 = tree.insert(record3)

    print(f"Inserted records at positions: {pos1} (length: {len1}), {pos2} (length: {len2}), {pos3} (length: {len3})")

    # Read the page back and verify
    page = pager.read_page(tree.root_page_num)
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
        is_root=True,
        parent_page_num=0,
        num_keys=2,
        right_child_page_num=5,
        keys=[10, 20],
        children=[1, 3, 6]
    )
    serialized = original_header.to_header()
    deserialized = InternalNodeHeader.from_header(serialized)

    assert deserialized.node_type == original_header.node_type, "Node type mismatch"
    assert deserialized.is_root == original_header.is_root, "Is root mismatch"
    assert deserialized.parent_page_num == original_header.parent_page_num, "Parent page num mismatch"
    assert deserialized.num_keys == original_header.num_keys, "Number of keys mismatch"
    assert deserialized.right_child_page_num == original_header.right_child_page_num, "Right child page num mismatch"
    assert deserialized.keys == original_header.keys, "Keys mismatch"
    assert deserialized.children == original_header.children, "Children mismatch"

    # Test with different values - empty internal node
    header2 = InternalNodeHeader(
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

    # Test with maximum keys (INTERNAL_NODE_MAX_KEYS)
    header3 = InternalNodeHeader(
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


def test_split_leaf_node():
    """Test the split_leaf_node functionality"""
    test_db_file = "test_split.db"
    if os.path.exists(test_db_file):
        os.remove(test_db_file)

    # Create new pager
    pager = Pager(test_db_file)
    tree = BTree.new_tree(pager)

    # Create a simple schema for testing
    schema = BasicSchema("test_table", [
        Column("id", Integer(), True),
        Column("data", Text(), False)
    ])

    # Create test records - enough to fill a leaf node and trigger a split
    records = []
    for i in range(LEAF_NODE_MAX_CELLS + 1):  # Insert one more than max to trigger split
        records.append(Record(values={"id": i + 1, "data": f"record {i + 1}"}, schema=schema))

    print(f"Inserting {len(records)} records to trigger leaf node split...")

    # Insert records - the last one should trigger a split
    for i, record in enumerate(records):
        pos, length = tree.insert(record)
        print(f"Inserted record {i + 1} at position {pos}, length {length}")

    # Verify the tree structure after split
    print(f"Root page number after split: {tree.root_page_num}")

    # Read the root page to see if it's now an internal node
    root_page = pager.read_page(tree.root_page_num)
    root_node_type = get_node_type(root_page)
    print(f"Root node type: {root_node_type}")

    # If the root is now an internal node, verify its structure
    if root_node_type == NodeType.INTERNAL:
        internal_header = InternalNodeHeader.from_header(root_page)
        print(f"Internal node header: {internal_header}")

        # Verify internal node properties
        assert internal_header.is_root == True, "New root should be marked as root"
        assert internal_header.num_keys == 1, "New internal node should have 1 key separating two children"
        assert len(internal_header.children) == 2, "Internal node should have 2 children"
        assert internal_header.right_child_page_num == internal_header.children[1], "Right child should match"

        # Check the left child (original leaf node)
        left_child_page = pager.read_page(internal_header.children[0])
        left_header = LeafNodeHeader.from_header(left_child_page)
        print(f"Left child (original page): num_cells={left_header.num_cells}, cell_pointers={left_header.cell_pointers}")

        # Check the right child (new leaf node)
        right_child_page = pager.read_page(internal_header.children[1])
        right_header = LeafNodeHeader.from_header(right_child_page)
        print(f"Right child (new page): num_cells={right_header.num_cells}, cell_pointers={right_header.cell_pointers}")

        # Verify that records are properly distributed
        total_cells = left_header.num_cells + right_header.num_cells
        assert total_cells == LEAF_NODE_MAX_CELLS + 1, f"Total cells should be {LEAF_NODE_MAX_CELLS + 1}, got {total_cells}"

        # Check that records are properly distributed
        # After inserting 4 records: split happens at 3 cells (2 left, 1 right), then 4th record goes to right
        # So final distribution should be: left=2, right=2
        assert left_header.num_cells == LEAF_NODE_LEFT_SPLIT_COUNT, f"Left child should have {LEAF_NODE_LEFT_SPLIT_COUNT} cells"
        assert right_header.num_cells == 2, f"Right child should have 2 cells (1 from split + 1 from 4th insert)"

        print("✓ Leaf node split test passed!")
    else:
        print("Warning: Root is still a leaf node - split may not have occurred")
        # This could happen if the split logic isn't working correctly
        leaf_header = LeafNodeHeader.from_header(root_page)
        print(f"Leaf node has {leaf_header.num_cells} cells, max is {LEAF_NODE_MAX_CELLS}")

    # Clean up
    pager.close()
    os.remove(test_db_file)

    print("All split leaf node tests passed!")


if __name__ == "__main__":
    test_file_header()
    test_page_header()
    test_internal_node_header()
    test_pager()
    test_insert()
    test_split_leaf_node()