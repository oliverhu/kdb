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
import sys
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
        for i in range(num_keys):  # Read exactly num_keys children (the +1 child is in right_child_page_num)
            children.append(Integer.deserialize(header[children_offset + i * 4:children_offset + (i + 1) * 4]))
        result = InternalNodeHeader(is_root, parent_page_num, num_keys, right_child_page_num, keys, children)
        return result

    def to_header(self):
        header_bytes = Integer.serialize(self.node_type.value) + Integer.serialize(1 if self.is_root else 0) + Integer.serialize(self.parent_page_num) + Integer.serialize(self.num_keys) + Integer.serialize(self.right_child_page_num) + b"".join(Integer.serialize(key) for key in self.keys) + b"".join(Integer.serialize(child) for child in self.children)
        return header_bytes

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
        # Initialize root page as leaf if empty
        page = self.pager.get_page(root_page_num)
        if all(b == 0 for b in page):
            header = LeafNodeHeader(is_root=True, parent_page_num=0, num_cells=0, allocation_pointer=self.pager.page_size, cell_pointers=[])
            header_bytes = header.to_header()
            page[:len(header_bytes)] = header_bytes
            self.pager.write_page(root_page_num, bytes(page))
            # Ensure num_pages is correct so get_free_page never returns root
            if root_page_num >= self.pager.num_pages:
                self.pager.num_pages = root_page_num + 1

    def __str__(self):
        """Returns a string representation of the B-tree structure"""
        def print_node(page_num: int, level: int = 0) -> str:
            page = self.pager.get_page(page_num)
            node_type = NodeType(Integer.deserialize(page[0:4]))
            result = "  " * level

            if node_type == NodeType.LEAF:
                header = LeafNodeHeader.from_header(page)
                result += f"Leaf Node (page {page_num}): {header.num_cells} cells\n"
                for ptr in header.cell_pointers:
                    cell_data = page[ptr:]
                    key = deserialize_key(cell_data)
                    result += "  " * (level + 1) + f"Key: {key}\n"
            else:  # INTERNAL
                header = InternalNodeHeader.from_header(page)
                result += f"Internal Node (page {page_num}): {header.num_keys} keys\n"
                # Print children recursively
                for i, child in enumerate(header.children):
                    if i > 0:
                        result += "  " * (level + 1) + f"Key: {header.keys[i-1]}\n"
                    result += print_node(child, level + 1)
                # Print the right child if it exists
                if header.right_child_page_num != 0:
                    if len(header.children) > 0:
                        result += "  " * (level + 1) + f"Key: {header.keys[-1]}\n"
                    result += print_node(header.right_child_page_num, level + 1)

            return result

        return f"B-tree (root: page {self.root_page_num}):\n" + print_node(self.root_page_num)

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
           (node_type == NodeType.LEAF and int.from_bytes(page[16:20], sys.byteorder) < 128):
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
            # An internal node with n keys has n children in children[] and 1 child in right_child_page_num
            if len(header.children) == 0:
                # Only one child (in right_child_page_num)
                return self.find(key, header.right_child_page_num)
            else:
                # Multiple children - find the correct one based on keys
                left_child_page_num = header.children[0]
                # Get the largest key in the left child
                left_child_page = self.pager.get_page(left_child_page_num)
                left_child_header = LeafNodeHeader.from_header(left_child_page)
                if left_child_header.num_cells == 0:
                    # If left child is empty, go right
                    return self.find(key, header.right_child_page_num)
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
                    return self.find(key, header.right_child_page_num)

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


    def insert(self, cell: Cell):
        """
        Insert a cell into the correct page in the B-tree.
        The cell is added to the page, updating the page header accordingly.
        Returns a tuple of (position, length) where the cell was stored.
        """
        # Find the correct page to insert into
        cell_key = deserialize_key(cell)
        page_num = self.find(cell_key)

        # Get the current page
        page = bytearray(self.pager.get_page(page_num))

        # Parse the page header
        header = LeafNodeHeader.from_header(page)

        num_cells = header.num_cells
        if num_cells < LEAF_NODE_MAX_CELLS:
            # Insert into the leaf node
            result = self.insert_cell_into_leaf_node(cell, page_num)
            header_after = LeafNodeHeader.from_header(self.pager.get_page(page_num))
            return result
        else:
            # Split the leaf node
            self.split_leaf_node(page_num)
            # Re-fetch the page after split
            page_num = self.find(cell_key)
            result = self.insert_cell_into_leaf_node(cell, page_num)
            header_after = LeafNodeHeader.from_header(self.pager.get_page(page_num))
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
    def _rebuild_children(self, header):
        # Helper to rebuild children and right_child_page_num from all children
        all_children = header.children + [header.right_child_page_num]
        all_children = [c for c in all_children if c != 0]

        # For an internal node with n keys, we need n+1 children total
        # children should have n elements, right_child_page_num should be the (n+1)th element
        if len(all_children) >= header.num_keys + 1:
            header.children = all_children[:header.num_keys]
            header.right_child_page_num = all_children[header.num_keys]
        else:
            # Not enough children, this is an error state
            print(f"[ERROR] _rebuild_children: not enough children for {header.num_keys} keys, all_children={all_children}")
            # This should not happen in normal operation. If it does, it indicates a bug in the split logic.
            # For now, we'll set the node to have no children and let the caller handle it.
            header.children = []
            header.right_child_page_num = 0
            header.num_keys = 0

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
            children=[left_node_page_num]  # Only left child in children list
        )
        header_bytes = root_header.to_header()
        root_page[:len(header_bytes)] = header_bytes
        self.pager.write_page(root_page_num, bytes(root_page))

        # Update parent pointers of all children (including right child)
        for child_page_num in root_header.children:
            child_page = bytearray(self.pager.get_page(child_page_num))
            if get_node_type(child_page) == NodeType.LEAF:
                child_header = LeafNodeHeader.from_header(child_page)
                child_header.parent_page_num = root_page_num
                child_header.is_root = False
                child_page[:len(child_header.to_header())] = child_header.to_header()
            else:
                child_header = InternalNodeHeader.from_header(child_page)
                child_header.parent_page_num = root_page_num
                child_header.is_root = False
                child_page[:len(child_header.to_header())] = child_header.to_header()
            self.pager.write_page(child_page_num, bytes(child_page))
            self.pager.pages[child_page_num] = child_page
        return root_page_num

    def split_internal_node(self, page_num: int, new_child_page_num: int, new_child_key: int):
        old_page = self.pager.get_page(page_num)
        old_header = InternalNodeHeader.from_header(old_page)

        # Build the full list of children
        full_children = old_header.children + [old_header.right_child_page_num]
        # Insert the new child in the correct position
        insert_pos = 0
        while insert_pos < len(old_header.keys) and new_child_key > old_header.keys[insert_pos]:
            insert_pos += 1
        old_header.keys.insert(insert_pos, new_child_key)
        full_children.insert(insert_pos + 1, new_child_page_num)
        old_header.num_keys = len(old_header.keys)

        # Split evenly
        mid = old_header.num_keys // 2
        separator_key = old_header.keys[mid]
        left_keys = old_header.keys[:mid]
        right_keys = old_header.keys[mid+1:]
        left_children = full_children[:mid+1]
        right_children = full_children[mid+1:]

        # Ensure correct number of children for each side
        if len(left_children) != len(left_keys) + 1:
            raise Exception(f"Internal node split error: left_children count {len(left_children)} does not match left_keys+1 {len(left_keys)+1}. full_children={full_children}, mid={mid}, old_header.keys={old_header.keys}")
        if len(right_children) != len(right_keys) + 1:
            raise Exception(f"Internal node split error: right_children count {len(right_children)} does not match right_keys+1 {len(right_keys)+1}. full_children={full_children}, mid={mid}, old_header.keys={old_header.keys}")

        # Assign children and right_child for left node
        old_header.keys = left_keys
        old_header.children = left_children[:-1]
        old_header.num_keys = len(left_keys)
        old_header.right_child_page_num = left_children[-1]
        old_header_bytes = old_header.to_header()
        old_page[:len(old_header_bytes)] = old_header_bytes
        self.pager.write_page(page_num, bytes(old_page))

        # Assign children and right_child for right node
        new_page_num = self.pager.get_free_page()
        new_page = bytearray(self.pager.page_size)
        new_header = InternalNodeHeader(
            is_root=False,
            parent_page_num=old_header.parent_page_num,
            num_keys=len(right_keys),
            right_child_page_num=right_children[-1],
            keys=right_keys,
            children=right_children[:-1]
        )
        new_header_bytes = new_header.to_header()
        new_page[:len(new_header_bytes)] = new_header_bytes
        self.pager.write_page(new_page_num, bytes(new_page))

        for child_page_num in right_children:
            child_page = bytearray(self.pager.get_page(child_page_num))
            if get_node_type(child_page) == NodeType.LEAF:
                child_header = LeafNodeHeader.from_header(child_page)
                child_header.parent_page_num = new_page_num
                child_page[:len(child_header.to_header())] = child_header.to_header()
            else:
                child_header = InternalNodeHeader.from_header(child_page)
                child_header.parent_page_num = new_page_num
                child_page[:len(child_header.to_header())] = child_header.to_header()
            self.pager.write_page(child_page_num, bytes(child_page))
            self.pager.pages[child_page_num] = child_page

        if old_header.is_root:
            new_root_page_num = self.pager.get_free_page()
            new_root_page = bytearray(self.pager.page_size)
            new_root_header = InternalNodeHeader(
                is_root=True,
                parent_page_num=0,
                num_keys=1,
                right_child_page_num=new_page_num,
                keys=[separator_key],
                children=[page_num]
            )
            new_root_header_bytes = new_root_header.to_header()
            new_root_page[:len(new_root_header_bytes)] = new_root_header_bytes
            self.pager.write_page(new_root_page_num, bytes(new_root_page))
            old_header.parent_page_num = new_root_page_num
            old_header.is_root = False
            new_header.parent_page_num = new_root_page_num
            old_header_bytes = old_header.to_header()
            old_page[:len(old_header_bytes)] = old_header_bytes
            self.pager.write_page(page_num, bytes(old_page))
            new_header_bytes = new_header.to_header()
            new_page[:len(new_header_bytes)] = new_header_bytes
            self.pager.write_page(new_page_num, bytes(new_page))
            self.root_page_num = new_root_page_num
        else:
            return self.split_internal_node(old_header.parent_page_num, new_page_num, separator_key)

    def split_leaf_node(self, page_num: int):
        old_page_num = page_num
        old_header = LeafNodeHeader.from_header(self.pager.get_page(page_num))
        old_page = self.pager.get_page(old_page_num)
        # Make a copy of the cell pointers to use for splitting
        cell_pointers_copy = list(old_header.cell_pointers)
        # Sort cell_pointers by key before splitting
        cell_ptrs_with_keys = [(ptr, deserialize_key(old_page[ptr:])) for ptr in cell_pointers_copy]
        cell_ptrs_with_keys.sort(key=lambda x: x[1])
        sorted_cell_pointers = [ptr for ptr, key in cell_ptrs_with_keys]
        sorted_keys = [key for ptr, key in cell_ptrs_with_keys]
        # Allocate new page for the split BEFORE moving cells
        new_page_num = self.pager.get_free_page()
        new_page = bytearray(self.pager.page_size)
        new_header = LeafNodeHeader(is_root=False, parent_page_num=old_header.parent_page_num, num_cells=0, allocation_pointer=self.pager.page_size, cell_pointers=[])
        new_header_bytes = new_header.to_header()
        new_page[:len(new_header_bytes)] = new_header_bytes
        self.pager.write_page(new_page_num, bytes(new_page))
        # Move cells to new page BEFORE updating the old page header or writing the page
        pointers_to_move = sorted_cell_pointers[LEAF_NODE_LEFT_SPLIT_COUNT:]
        keys_to_move = [deserialize_key(old_page[ptr:]) for ptr in pointers_to_move]
        for ptr in pointers_to_move:
            cell_data = old_page[ptr:]
            size = cell_size(cell_data)
            cell = old_page[ptr:ptr+size]
            self.insert_cell_into_leaf_node(cell, new_page_num)
        # Update old page header with remaining cells
        old_header.cell_pointers = sorted_cell_pointers[:LEAF_NODE_LEFT_SPLIT_COUNT]
        old_header.num_cells = LEAF_NODE_LEFT_SPLIT_COUNT
        if old_header.cell_pointers:
            old_header.allocation_pointer = min(old_header.cell_pointers)
        else:
            old_header.allocation_pointer = self.pager.page_size
        left_keys_after = [deserialize_key(old_page[ptr:]) for ptr in old_header.cell_pointers]
        old_header_bytes = old_header.to_header()
        old_page[:len(old_header_bytes)] = old_header_bytes
        self.pager.write_page(old_page_num, bytes(old_page))
        # Re-fetch the new page and header after all insertions
        new_page = self.pager.get_page(new_page_num)
        new_header = LeafNodeHeader.from_header(new_page)
        new_keys = [deserialize_key(new_page[ptr:]) for ptr in new_header.cell_pointers]
        # If parent is internal, print its keys and children
        if old_header.parent_page_num != 0:
            parent_page = self.pager.get_page(old_header.parent_page_num)
            if get_node_type(parent_page) == NodeType.INTERNAL:
                parent_header = InternalNodeHeader.from_header(parent_page)

        # Implement comprehensive split logic for both root and non-root cases
        if old_header.is_root:
            # Create a new root internal node
            new_root_page_num = self.pager.get_free_page()
            new_root_page = bytearray(self.pager.page_size)
            # The separator key is the max key in the left (old) leaf
            separator_key = max(left_keys_after) if left_keys_after else 0
            new_root_header = InternalNodeHeader(
                is_root=True,
                parent_page_num=0,
                num_keys=1,
                right_child_page_num=new_page_num,
                keys=[separator_key],
                children=[old_page_num]
            )
            new_root_header_bytes = new_root_header.to_header()
            new_root_page[:len(new_root_header_bytes)] = new_root_header_bytes
            self.pager.write_page(new_root_page_num, bytes(new_root_page))
            # Update old and new leaf headers to point to new root
            old_header.parent_page_num = new_root_page_num
            old_header.is_root = False
            old_header_bytes = old_header.to_header()
            old_page[:len(old_header_bytes)] = old_header_bytes
            self.pager.write_page(old_page_num, bytes(old_page))
            new_header.parent_page_num = new_root_page_num
            new_header_bytes = new_header.to_header()
            new_page[:len(new_header_bytes)] = new_header_bytes
            self.pager.write_page(new_page_num, bytes(new_page))
            self.root_page_num = new_root_page_num
        else:
            # Insert new child and separator key into parent internal node
            parent_page_num = old_header.parent_page_num
            parent_page = self.pager.get_page(parent_page_num)
            parent_header = InternalNodeHeader.from_header(parent_page)
            # The separator key is the max key in the left (old) leaf
            separator_key = max(left_keys_after) if left_keys_after else 0
            # Insert new child and key into parent
            if parent_header.num_keys < INTERNAL_NODE_MAX_KEYS:
                # Insert into parent directly
                insert_pos = 0
                while insert_pos < len(parent_header.keys) and separator_key > parent_header.keys[insert_pos]:
                    insert_pos += 1
                parent_header.keys.insert(insert_pos, separator_key)
                parent_header.num_keys += 1
                # Insert new child into children/right_child
                full_children = parent_header.children + [parent_header.right_child_page_num]
                full_children.insert(insert_pos + 1, new_page_num)
                parent_header.children = full_children[:-1]
                parent_header.right_child_page_num = full_children[-1]
                parent_header_bytes = parent_header.to_header()
                parent_page[:len(parent_header_bytes)] = parent_header_bytes
                self.pager.write_page(parent_page_num, bytes(parent_page))
            else:
                # Parent is full, split parent recursively
                new_root_page_num = self.split_internal_node(parent_page_num, new_page_num, separator_key)
                if new_root_page_num is not None:
                    self.root_page_num = new_root_page_num

    def insert_cell_into_leaf_node(self, cell: Cell, page_num: int):
        page = bytearray(self.pager.get_page(page_num))
        header = LeafNodeHeader.from_header(page)

        cell_offset = header.allocation_pointer - len(cell)
        if cell_offset < 0:
            raise Exception("Cell offset is negative. Not enough space in page.")

        page[cell_offset:cell_offset + len(cell)] = cell

        header.num_cells += 1
        header.cell_pointers.append(cell_offset)
        header.allocation_pointer = cell_offset
        header_bytes = header.to_header()
        page[:len(header_bytes)] = header_bytes
        self.pager.write_page(page_num, bytes(page))
        self.pager.pages[page_num] = page

        # Return the position and length
        return cell_offset, len(cell)

    def insert_into_leaf_node(self, cell: Cell, page_num: int):
        return self.insert_cell_into_leaf_node(cell, page_num)


