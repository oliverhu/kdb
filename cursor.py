import sys
from typing import Optional
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
        header = LeafNodeHeader.from_header(bytes(page))

        self.cell_num += 1
        if self.cell_num >= header.num_cells:
            self.navigate_to_next_leaf_node()
            self.cell_num = 0

    def get_cell(self):
        page = self.pager.get_page(self.page_num)
        header = LeafNodeHeader.from_header(bytes(page))

        # Check if we're at the end of the current page
        if self.cell_num >= header.num_cells:
            return b''  # Return empty bytes if no more cells

        cell_offset = header.cell_pointers[self.cell_num]
        return page[cell_offset:]

    # private methods
    def navigate_to_first_leaf_node(self, page_num: Optional[int] = None):
        if page_num is None:
            page_num = self.page_num
        page = self.pager.get_page(page_num)
        while get_node_type(bytes(page)) != NodeType.LEAF:
            header = InternalNodeHeader.from_header(bytes(page))
            if len(header.children) == 0:
                # If no children in children array, use right_child_page_num
                if header.right_child_page_num == 0:
                    # Empty tree, set end_of_table
                    self.end_of_table = True
                    return
                page_num = header.right_child_page_num
            else:
                page_num = header.children[0]
            page = self.pager.get_page(page_num)
        self.page_num = page_num  # Set to the leftmost leaf node after traversal

    def navigate_to_next_leaf_node(self):
        page = self.pager.get_page(self.page_num)
        node_type = get_node_type(bytes(page))
        if node_type == NodeType.LEAF:
            header = LeafNodeHeader.from_header(bytes(page))
            parent_page_num = header.parent_page_num
        else:
            header = InternalNodeHeader.from_header(bytes(page))
            parent_page_num = header.parent_page_num
        if header.is_root:
            self.end_of_table = True
            return None

        current_page_num = self.page_num
        while True:
            parent_header = InternalNodeHeader.from_header(bytes(self.pager.get_page(parent_page_num)))

            # Check if current page is the right child
            if current_page_num == parent_header.right_child_page_num:
                if parent_header.is_root:
                    self.end_of_table = True
                    return None
                current_page_num = parent_page_num
                parent_page_num = parent_header.parent_page_num
                continue

            # Check if current page is one of the children in the children array
            found_in_children = False
            for i, child_page_num in enumerate(parent_header.children):
                if child_page_num == current_page_num:
                    found_in_children = True
                    # If not the last child, go to the next sibling
                    if i + 1 < len(parent_header.children):
                        next_sibling = parent_header.children[i + 1]
                        if next_sibling != 0:
                            self.page_num = next_sibling
                            self.navigate_to_first_leaf_node(self.page_num)
                            return
                        else:
                            # Try to find the next valid sibling
                            for j in range(i + 2, len(parent_header.children)):
                                if parent_header.children[j] != 0:
                                    self.page_num = parent_header.children[j]
                                    self.navigate_to_first_leaf_node(self.page_num)
                                    return
                            # No valid sibling found, try right_child
                            if parent_header.right_child_page_num != 0:
                                self.page_num = parent_header.right_child_page_num
                                self.navigate_to_first_leaf_node(self.page_num)
                                return
                            else:
                                self.end_of_table = True
                                return None
                    else:
                        # If last child in children array, go to the right_child_page_num
                        if parent_header.right_child_page_num != 0:
                            self.page_num = parent_header.right_child_page_num
                            self.navigate_to_first_leaf_node(self.page_num)
                            return
                        else:
                            self.end_of_table = True
                            return None
                    break

            if not found_in_children:
                # Current page is not found in children array, this shouldn't happen
                print(f"[ERROR] navigate_to_next_leaf_node: current_page_num {current_page_num} not found in parent {parent_page_num}")
                self.end_of_table = True
                return None

