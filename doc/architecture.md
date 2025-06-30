Database File
┌─────────────────────────────────────────────────────────────┐
│                    Database File                            │
├─────────────────────────────────────────────────────────────┤
│ File Header (100 bytes)                                     │
│ ├─ Version: "kdb0"                                          │
│ ├─ Next Free Page: <page_num>                               │
│ ├─ Has Free List: <boolean>                                 │
│ └─ Padding: <76 bytes>                                      │
├─────────────────────────────────────────────────────────────┤
│ Page 0: Catalog Table B-tree                                │
│ ├─ Catalog records (table_name → root_page_num mapping)     │
│ └─ Schema: (pkey, name, root_pagenum, sql_text)             │
├─────────────────────────────────────────────────────────────┤
│ Page 1: User Table A B-tree                                 │
│ ├─ Table A records                                          │
│ └─ Schema: (id, name, email, ...)                           │
├─────────────────────────────────────────────────────────────┤
│ Page 2: User Table B B-tree                                 │
│ ├─ Table B records                                          │
│ └─ Schema: (id, title, price, ...)                          │
├─────────────────────────────────────────────────────────────┤
│ ... more pages ...                                          │
└─────────────────────────────────────────────────────────────┘

Table
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

