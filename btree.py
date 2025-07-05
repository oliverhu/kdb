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
from pager import PageHeader, Pager
from typing import List

from record import deserialize_key


class NodeType(Enum):
    INTERNAL = auto()
    LEAF = auto()


class Node:
    def __init__(self, page_num: int):
        self.page_num = page_num  # each node maps to a page in the file
        self.node_type = None
        self.is_root = False
        self.parent_page_num = None
        self.number_of_keys = 0
        self.right_child_page_num = None
        self.keys = []
        self.children = []
        self.cells = []
        self.free_list_head_page_num = None
        self.total_free_list_space = 0


class LeafNode(Node):
    def __init__(self, page_num: int):
        super().__init__(page_num)
        self.node_type = NodeType.LEAF

class InternalNode(Node):
    def __init__(self, page_num: int):
        super().__init__(page_num)
        self.node_type = NodeType.INTERNAL


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
"""
class BTree:
    def __init__(self, pager: Pager, root_page_num: int):
        self.pager = pager
        self.root_page_num = root_page_num

    # public APIs
    def find(self, key: int, page_num: int = None):
        # recursively find the page that contains the key
        if page_num is None:
            page_num = self.root_page_num
        cell = self.pager.get_page(page_num)
        header = PageHeader.from_header(cell)
        if header.node_type == NodeType.LEAF:
            return cell
        else:
            # For internal nodes, we need to find the appropriate child
            # This is a simplified implementation - in a real B-tree,
            # we would compare the key with the keys in the internal node
            # to determine which child to traverse
            return self.find(key, header.right_child_page_num)

    def insert(self, cell: bytes):
        key = deserialize_key(cell)
        # TODO: Implement B-tree insertion logic
        pass

    def delete(self, key: int):
        pass

    # private APIs
