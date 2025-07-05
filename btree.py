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
from pager import Pager
from typing import List


class NodeType(Enum):
    INTERNAL = auto()
    LEAF = auto()


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
        pass

    def insert(self, cell: bytes):
        pass

    def delete(self, key: int):
        pass

    # private APIs

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

class BTree:
    def __init__(self, page_size: int):
        self.page_size = page_size
        self.root_page_num = 0
        self.root_page = None

    def create(self, file_name: str):
        pass

    def open(self, file_name: str):
        pass

    def insert(self, key: int, value: bytes):
        pass

    def delete(self, key: int):
        pass

    def search(self, key: int):
        pass


def cell_from_page(page: bytes, cell_num: int):
    pass