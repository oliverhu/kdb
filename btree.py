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
from typing import Any, List

from record import Record, deserialize, deserialize_key, serialize, cell_size
from schema.basic_schema import BasicSchema, Column
from schema.datatypes import Integer, Text

Cell = bytearray

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

    def update_cell(self, key: int, new_cell: Cell):
        """
        Update a cell in the B-tree by replacing it with new cell data.
        """
        leaf_page_num = self.find(key)
        leaf_page = bytearray(self.pager.get_page(leaf_page_num))
        leaf_header = LeafNodeHeader.from_header(leaf_page)
        
        for i, ptr in enumerate(leaf_header.cell_pointers):
            cell_data = leaf_page[ptr:]
            cell_key = deserialize_key(cell_data)
            if cell_key == key:
                # Get the current cell size
                current_cell_size = cell_size(cell_data)
                new_cell_size = len(new_cell)
                
                # Check if the new cell fits in the same space
                if new_cell_size <= current_cell_size:
                    # Simple replacement
                    leaf_page[ptr:ptr + new_cell_size] = new_cell
                    # Clear any remaining space if new cell is smaller
                    if new_cell_size < current_cell_size:
                        leaf_page[ptr + new_cell_size:ptr + current_cell_size] = b'\x00' * (current_cell_size - new_cell_size)
                else:
                    # New cell is larger - handle by removing old cell and inserting new one
                    # Remove the old cell
                    self._remove_cell_from_leaf(leaf_page_num, i)
                    
                    # Insert the new cell
                    self.insert_cell_into_leaf_node(new_cell, leaf_page_num)
                    return True
                
                # Write the updated page
                self.pager.write_page(leaf_page_num, bytes(leaf_page))
                self.pager.pages[leaf_page_num] = leaf_page
                return True
        
        raise ValueError(f"Key {key} not found in B-tree")

    def delete(self, key: int):
        """
        Delete a key from the B-tree.
        Algorithm:
        1. Find the key (leaf page and cell position)
        2. If key doesn't exist, terminate
        3. Delete the key (reduce num_cells, move remaining cells)
        4. Update parent keys if deleted key was max key
        5. Check for restructuring needs (merge/redistribute with siblings)
        6. Handle root deletion if tree height needs to reduce
        """
        # Step 1: Find the leaf page containing the key
        leaf_page_num = self.find(key)
        leaf_page = bytearray(self.pager.get_page(leaf_page_num))
        leaf_header = LeafNodeHeader.from_header(leaf_page)
        
        # Step 2: Find the cell with the target key
        cell_index = None
        target_cell_ptr = None
        for i, ptr in enumerate(leaf_header.cell_pointers):
            cell_data = leaf_page[ptr:]
            cell_key = deserialize_key(cell_data)
            if cell_key == key:
                cell_index = i
                target_cell_ptr = ptr
                break
        
        # If key doesn't exist, terminate
        if cell_index is None:
            return False  # Key not found
        
        # Step 3: Remove the cell from the leaf node
        was_max_key = self._is_max_key_in_node(leaf_page, leaf_header, key)
        self._remove_cell_from_leaf(leaf_page_num, cell_index)
        
        # Step 4: Update parent keys if the deleted key was the max key
        if was_max_key:
            self._update_parent_keys_after_deletion(leaf_page_num, key)
        
        # Step 5: Check if restructuring is needed
        leaf_page = bytearray(self.pager.get_page(leaf_page_num))
        leaf_header = LeafNodeHeader.from_header(leaf_page)
        
        # Define minimum threshold for restructuring
        min_keys_threshold = LEAF_NODE_MAX_CELLS // 2
        
        if leaf_header.num_cells < min_keys_threshold and not leaf_header.is_root:
            self._handle_underflow(leaf_page_num)
        
        # Step 6: Handle root deletion if needed
        if leaf_header.is_root and leaf_header.num_cells == 0:
            # Root is empty, but we keep it as an empty leaf
            pass
        
        return True  # Successfully deleted

    def _is_max_key_in_node(self, page: bytearray, header: LeafNodeHeader, key: int) -> bool:
        """Check if the given key is the maximum key in the node"""
        max_key = None
        for ptr in header.cell_pointers:
            cell_data = page[ptr:]
            cell_key = deserialize_key(cell_data)
            if max_key is None or cell_key > max_key:
                max_key = cell_key
        return max_key == key

    def _remove_cell_from_leaf(self, page_num: int, cell_index: int):
        """Remove a cell from a leaf node by index"""
        page = bytearray(self.pager.get_page(page_num))
        header = LeafNodeHeader.from_header(page)
        
        # Remove the cell pointer at the specified index
        if 0 <= cell_index < len(header.cell_pointers):
            header.cell_pointers.pop(cell_index)
            header.num_cells -= 1
            
            # Update the header in the page
            header_bytes = header.to_header()
            page[:len(header_bytes)] = header_bytes
            self.pager.write_page(page_num, bytes(page))
            self.pager.pages[page_num] = page

    def _update_parent_keys_after_deletion(self, leaf_page_num: int, deleted_key: int):
        """Update parent keys when the max key in a child node is deleted"""
        leaf_page = self.pager.get_page(leaf_page_num)
        leaf_header = LeafNodeHeader.from_header(leaf_page)
        
        if leaf_header.parent_page_num == 0:
            return  # No parent to update
        
        # Find the new max key in the leaf node
        new_max_key = None
        if leaf_header.num_cells > 0:
            for ptr in leaf_header.cell_pointers:
                cell_data = leaf_page[ptr:]
                cell_key = deserialize_key(cell_data)
                if new_max_key is None or cell_key > new_max_key:
                    new_max_key = cell_key
        
        # Update parent internal node
        self._update_internal_node_key(leaf_header.parent_page_num, leaf_page_num, deleted_key, new_max_key)

    def _update_internal_node_key(self, internal_page_num: int, child_page_num: int, old_key: int, new_key: int):
        """Update a key in an internal node"""
        internal_page = bytearray(self.pager.get_page(internal_page_num))
        internal_header = InternalNodeHeader.from_header(internal_page)
        
        # Find and update the key
        key_updated = False
        for i, key in enumerate(internal_header.keys):
            if key == old_key:
                if new_key is not None:
                    internal_header.keys[i] = new_key
                else:
                    # If new_key is None, we need to remove this key
                    internal_header.keys.pop(i)
                    internal_header.num_keys -= 1
                key_updated = True
                break
        
        if key_updated:
            # Write the updated header back
            header_bytes = internal_header.to_header()
            internal_page[:len(header_bytes)] = header_bytes
            self.pager.write_page(internal_page_num, bytes(internal_page))
            self.pager.pages[internal_page_num] = internal_page
            
            # If this was the max key in the internal node and we changed it,
            # we may need to update the parent as well
            if old_key == max(internal_header.keys + [old_key]) and internal_header.parent_page_num != 0:
                self._update_internal_node_key(internal_header.parent_page_num, internal_page_num, old_key, new_key)

    def _handle_underflow(self, page_num: int):
        """Handle underflow in a node by merging or redistributing with siblings"""
        page = self.pager.get_page(page_num)
        node_type = get_node_type(page)
        
        if node_type == NodeType.LEAF:
            self._handle_leaf_underflow(page_num)
        else:
            self._handle_internal_underflow(page_num)

    def _handle_leaf_underflow(self, leaf_page_num: int):
        """Handle underflow in a leaf node"""
        leaf_page = self.pager.get_page(leaf_page_num)
        leaf_header = LeafNodeHeader.from_header(leaf_page)
        
        if leaf_header.parent_page_num == 0:
            return  # Root node, no siblings to merge with
        
        # Get parent and find siblings
        parent_page = self.pager.get_page(leaf_header.parent_page_num)
        parent_header = InternalNodeHeader.from_header(parent_page)
        
        # Find this node's position in parent's children
        node_position = -1
        all_children = parent_header.children + [parent_header.right_child_page_num]
        for i, child in enumerate(all_children):
            if child == leaf_page_num:
                node_position = i
                break
        
        if node_position == -1:
            return  # Couldn't find node in parent
        
        # Get left and right siblings
        left_sibling_num = all_children[node_position - 1] if node_position > 0 else None
        right_sibling_num = all_children[node_position + 1] if node_position < len(all_children) - 1 else None
        
        # Try to redistribute with left sibling first
        if left_sibling_num is not None:
            left_sibling_page = self.pager.get_page(left_sibling_num)
            left_sibling_header = LeafNodeHeader.from_header(left_sibling_page)
            
            total_cells = leaf_header.num_cells + left_sibling_header.num_cells
            if total_cells >= LEAF_NODE_MAX_CELLS:
                # Can redistribute
                self._redistribute_leaf_nodes(left_sibling_num, leaf_page_num)
                return
        
        # Try to redistribute with right sibling
        if right_sibling_num is not None:
            right_sibling_page = self.pager.get_page(right_sibling_num)
            right_sibling_header = LeafNodeHeader.from_header(right_sibling_page)
            
            total_cells = leaf_header.num_cells + right_sibling_header.num_cells
            if total_cells >= LEAF_NODE_MAX_CELLS:
                # Can redistribute
                self._redistribute_leaf_nodes(leaf_page_num, right_sibling_num)
                return
        
        # If we can't redistribute, merge with a sibling
        if left_sibling_num is not None:
            self._merge_leaf_nodes(left_sibling_num, leaf_page_num)
        elif right_sibling_num is not None:
            self._merge_leaf_nodes(leaf_page_num, right_sibling_num)

    def _redistribute_leaf_nodes(self, left_page_num: int, right_page_num: int):
        """Redistribute cells between two leaf nodes"""
        left_page = bytearray(self.pager.get_page(left_page_num))
        right_page = bytearray(self.pager.get_page(right_page_num))
        left_header = LeafNodeHeader.from_header(left_page)
        right_header = LeafNodeHeader.from_header(right_page)
        
        # Collect all cells from both nodes
        all_cells = []
        for ptr in left_header.cell_pointers:
            cell_data = left_page[ptr:]
            size = cell_size(cell_data)
            all_cells.append((left_page[ptr:ptr+size], deserialize_key(cell_data)))
        
        for ptr in right_header.cell_pointers:
            cell_data = right_page[ptr:]
            size = cell_size(cell_data)
            all_cells.append((right_page[ptr:ptr+size], deserialize_key(cell_data)))
        
        # Sort by key
        all_cells.sort(key=lambda x: x[1])
        
        # Redistribute evenly
        total_cells = len(all_cells)
        left_count = total_cells // 2
        right_count = total_cells - left_count
        
        # Clear both nodes
        left_header.cell_pointers.clear()
        left_header.num_cells = 0
        left_header.allocation_pointer = self.pager.page_size
        
        right_header.cell_pointers.clear()
        right_header.num_cells = 0
        right_header.allocation_pointer = self.pager.page_size
        
        # Write headers
        left_header_bytes = left_header.to_header()
        left_page[:len(left_header_bytes)] = left_header_bytes
        self.pager.write_page(left_page_num, bytes(left_page))
        
        right_header_bytes = right_header.to_header()
        right_page[:len(right_header_bytes)] = right_header_bytes
        self.pager.write_page(right_page_num, bytes(right_page))
        
        # Re-insert cells
        for i, (cell, key) in enumerate(all_cells):
            if i < left_count:
                self.insert_cell_into_leaf_node(cell, left_page_num)
            else:
                self.insert_cell_into_leaf_node(cell, right_page_num)

    def _merge_leaf_nodes(self, left_page_num: int, right_page_num: int):
        """Merge two leaf nodes into the left node"""
        left_page = bytearray(self.pager.get_page(left_page_num))
        right_page = self.pager.get_page(right_page_num)
        left_header = LeafNodeHeader.from_header(left_page)
        right_header = LeafNodeHeader.from_header(right_page)
        
        # Move all cells from right to left
        for ptr in right_header.cell_pointers:
            cell_data = right_page[ptr:]
            size = cell_size(cell_data)
            cell = right_page[ptr:ptr+size]
            self.insert_cell_into_leaf_node(cell, left_page_num)
        
        # Remove the right node from parent
        if left_header.parent_page_num != 0:
            self._remove_child_from_internal_node(left_header.parent_page_num, right_page_num)
        
        # Mark right page as free (implementation depends on pager)
        # For now, we'll just leave it as is

    def _handle_internal_underflow(self, internal_page_num: int):
        """Handle underflow in an internal node"""
        # Similar logic to leaf underflow but for internal nodes
        # This is more complex due to key management
        internal_page = self.pager.get_page(internal_page_num)
        internal_header = InternalNodeHeader.from_header(internal_page)
        
        if internal_header.parent_page_num == 0:
            # This is the root - check if it should be deleted
            if internal_header.num_keys == 0 and internal_header.right_child_page_num != 0:
                # Root has only one child, make that child the new root
                self._promote_child_to_root(internal_header.right_child_page_num)
            return
        
        # Handle non-root internal node underflow
        # Implementation would be similar to leaf nodes but more complex
        # For simplicity in this implementation, we'll leave it as a placeholder

    def _remove_child_from_internal_node(self, internal_page_num: int, child_page_num: int):
        """Remove a child pointer from an internal node"""
        internal_page = bytearray(self.pager.get_page(internal_page_num))
        internal_header = InternalNodeHeader.from_header(internal_page)
        
        # Find and remove the child
        all_children = internal_header.children + [internal_header.right_child_page_num]
        if child_page_num in all_children:
            child_index = all_children.index(child_page_num)
            
            if child_index < len(internal_header.children):
                # Child is in the children array
                internal_header.children.pop(child_index)
                if child_index < len(internal_header.keys):
                    internal_header.keys.pop(child_index)
                    internal_header.num_keys -= 1
            else:
                # Child is the right child
                if len(internal_header.children) > 0:
                    internal_header.right_child_page_num = internal_header.children.pop()
                    if len(internal_header.keys) > 0:
                        internal_header.keys.pop()
                        internal_header.num_keys -= 1
                else:
                    internal_header.right_child_page_num = 0
            
            # Write updated header
            header_bytes = internal_header.to_header()
            internal_page[:len(header_bytes)] = header_bytes
            self.pager.write_page(internal_page_num, bytes(internal_page))
            self.pager.pages[internal_page_num] = internal_page
            
            # Check if this internal node now needs restructuring
            min_keys_threshold = INTERNAL_NODE_MAX_KEYS // 2
            if internal_header.num_keys < min_keys_threshold and not internal_header.is_root:
                self._handle_underflow(internal_page_num)

    def _promote_child_to_root(self, child_page_num: int):
        """Promote a child to become the new root"""
        # Move the child page content to the root page
        child_page = self.pager.get_page(child_page_num)
        root_page = bytearray(self.pager.page_size)
        root_page[:] = child_page[:]
        
        # Update the header to mark it as root
        node_type = get_node_type(child_page)
        if node_type == NodeType.LEAF:
            header = LeafNodeHeader.from_header(root_page)
            header.is_root = True
            header.parent_page_num = 0
            header_bytes = header.to_header()
            root_page[:len(header_bytes)] = header_bytes
        else:
            header = InternalNodeHeader.from_header(root_page)
            header.is_root = True
            header.parent_page_num = 0
            header_bytes = header.to_header()
            root_page[:len(header_bytes)] = header_bytes
        
        # Write the new root
        self.pager.write_page(self.root_page_num, bytes(root_page))

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


