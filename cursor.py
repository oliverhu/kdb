from pager import Pager
from btree import BTree, NodeType, cell_from_page, get_node_type

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

class Cursor:
    def __init__(self, pager: Pager, tree: BTree):
        self.pager = pager
        self.tree = tree
        self.page_num = tree.root_page_num
        self.end_of_table = False
        self.cell_num = 0


    def next(self):
        pass

    def advance(self):
        self.cell_num += 1
        if self.cell_num >= self.num_cells:
            self.end_of_table = True

    def get_cell(self):
        page = self.pager.get_page(self.page_num)
        return cell_from_page(page, self.cell_num)

    def get_row_offset(self):
        return self.row_offset

    def set_row_offset(self, row_offset: int):
        self.row_offset = row_offset

    # private methods
    def navigate_to_first_leaf_node(self):
        node_type = get_node_type(self.pager.read_page(self.page_num))
        while node_type != NodeType.LEAF:
            self.page_num = self.tree.get_right_child_page_num(self.page_num)
            node_type = get_node_type(self.pager.read_page(self.page_num))

    def navigate_to_next_leaf_node(self):
        self.page_num = self.tree.get_right_child_page_num(self.page_num)