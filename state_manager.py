# Manage the state of the database

import os
from sqlite3 import Cursor
from pager import Pager, PAGE_SIZE
from record import Record, serialize
from schema.basic_schema import BasicSchema
from schema.basic_schema import Column, Integer, Text
from btree import BTree
from catalog.system_table import CatalogTable

class StateManager:
    """
    Manage the state of the database in memory.
    """
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.pager = Pager(file_path)
        # Use CatalogTable instead of file_header
        self.catalog = CatalogTable(self.pager, page_num=0)
        self.trees = {}  # table_name -> BTree

        # Load existing schemas and table pages from catalog
        self.schemas = {}
        self.table_pages = {}
        self._load_catalog_data()

    def _load_catalog_data(self):
        """Load all table information from the catalog"""
        tables = self.catalog.get_all_tables()
        for table_record in tables:
            table_name = table_record.values["table_name"]
            root_page_num = table_record.values["root_page_num"]
            # Load schema
            schema = self.catalog.get_schema(table_name)
            if schema:
                self.schemas[table_name] = schema
                self.table_pages[table_name] = root_page_num
                # Create BTree for existing table
                tree = BTree(self.pager, root_page_num)
                self.trees[table_name] = tree

    def register_table(self, table_name: str, schema: BasicSchema):
        # Create a new BTree for this table
        tree = BTree.new_tree(self.pager)
        page_num = tree.root_page_num
        # Store in memory
        self.schemas[table_name] = schema
        self.table_pages[table_name] = page_num
        self.trees[table_name] = tree
        # Store in catalog table
        self.catalog.add_table(table_name, page_num, schema)

    def get_table_schema(self, table_name: str):
        # First try to get from memory
        if table_name in self.schemas:
            return self.schemas[table_name]

        # If not in memory, try to get from catalog
        schema = self.catalog.get_schema(table_name)
        if schema:
            self.schemas[table_name] = schema
            return schema

        return None

    def get_table_cursor(self, table_name: str):
        if table_name not in self.trees:
            # Try to load from catalog
            root_page_num = self.catalog.get_root_page_num(table_name)
            if root_page_num is not None:
                tree = BTree(self.pager, root_page_num)
                self.trees[table_name] = tree
            else:
                raise ValueError(f"Table '{table_name}' not found")

        return Cursor(self.pager, self.trees[table_name])

    def get_table_root_page(self, table_name: str):
        """Get the root page number for a table"""
        # First try to get from memory
        if table_name in self.table_pages:
            return self.table_pages[table_name]

        # If not in memory, try to get from catalog
        root_page_num = self.catalog.get_root_page_num(table_name)
        if root_page_num is not None:
            self.table_pages[table_name] = root_page_num
            return root_page_num

        return None

    def insert(self, table_name: str, record: Record):
        """Insert a record into the specified table"""
        if table_name not in self.schemas:
            # Try to load from catalog
            schema = self.catalog.get_schema(table_name)
            if not schema:
                raise ValueError(f"Table '{table_name}' not found")
            self.schemas[table_name] = schema
        if table_name not in self.trees:
            # Try to load from catalog
            root_page_num = self.catalog.get_root_page_num(table_name)
            if root_page_num is not None:
                tree = BTree(self.pager, root_page_num)
                self.trees[table_name] = tree
            else:
                raise ValueError(f"Table '{table_name}' not found")
        tree = self.trees[table_name]
        cell = serialize(record)
        tree.insert(cell)

    def list_tables(self):
        """List all tables in the database"""
        return list(self.schemas.keys())