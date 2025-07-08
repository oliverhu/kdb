# Manage the state of the database

import os
from sqlite3 import Cursor
from pager import Pager, PAGE_SIZE
from record import Record, serialize
from schema.basic_schema import BasicSchema
from schema.basic_schema import Column, Integer, Text
from btree import BTree

class StateManager:
    """
    Manage the state of the database in memory.
    """
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.pager = Pager(file_path)
        # Load schemas from file header
        self.schemas = self.pager.file_header.schemas.copy()
        # Load table page mappings from file header
        self.table_pages = self.pager.file_header.table_pages.copy()
        self.trees = {}  # NEW: table_name -> BTree

    def register_table(self, table_name: str, schema: BasicSchema):
        # Create a new BTree for this table
        tree = BTree.new_tree(self.pager)
        page_num = tree.root_page_num
        self.schemas[table_name] = schema
        self.table_pages[table_name] = page_num
        self.trees[table_name] = tree  # NEW: store the BTree
        self.pager.file_header.add_schema(table_name, schema, page_num)
        self.pager.file_header.write_schemas_header(self.pager.file_ptr)
        print(f"Registered schema for table {table_name}: {schema} on page {page_num}")

    def get_table_schema(self, table_name: str):
        return self.schemas[table_name]

    def get_table_cursor(self, table_name: str):
        return Cursor(self.pager, self.trees[table_name])

    def save_schemas(self):
        """Save all schemas to the file header"""
        self.pager.file_header.schemas = self.schemas.copy()
        self.pager.file_header.table_pages = self.table_pages.copy()
        self.pager.file_header.write_schemas_header(self.pager.file_ptr)

    def insert(self, table_name: str, record: Record):
        """Insert a record into the specified table"""
        if table_name not in self.schemas:
            raise ValueError(f"Table '{table_name}' not found")
        if table_name not in self.trees:
            # If not loaded, create BTree from root page
            tree = BTree(self.pager, self.table_pages[table_name])
            self.trees[table_name] = tree
        tree = self.trees[table_name]
        cell = serialize(record)
        tree.insert(cell)
        print(f"Inserted record into table '{table_name}'")


def test_schema_header():
    # Test the schema header functionality
    test_db_file = "test_schema.db"
    if os.path.exists(test_db_file):
        os.remove(test_db_file)

    # Create new pager
    pager = Pager(test_db_file)

    # Create test schemas
    users_schema = BasicSchema("users", [
        Column("id", Integer(), True),
        Column("username", Text(), False),
        Column("email", Text(), False)
    ])

    products_schema = BasicSchema("products", [
        Column("product_id", Integer(), True),
        Column("name", Text(), False),
        Column("price", Integer(), False)
    ])

    # Add schemas to file header with page numbers
    pager.file_header.add_schema("users", users_schema, 0)
    pager.file_header.add_schema("products", products_schema, 1)

    # Write schemas to file
    pager.file_header.write_schemas_header(pager.file_ptr)

    print(f"Added schemas: {list(pager.file_header.schemas.keys())}")

    # Close and reopen to test persistence
    pager.close()

    # Reopen and read schemas
    pager2 = Pager(test_db_file)

    # Verify schemas were loaded correctly
    assert "users" in pager2.file_header.schemas, "Users schema not found"
    assert "products" in pager2.file_header.schemas, "Products schema not found"

    # Verify page numbers were loaded correctly
    assert "users" in pager2.file_header.table_pages, "Users page number not found"
    assert "products" in pager2.file_header.table_pages, "Products page number not found"
    assert pager2.file_header.table_pages["users"] == 0, "Users page number mismatch"
    assert pager2.file_header.table_pages["products"] == 1, "Products page number mismatch"

    # Verify schema contents
    loaded_users = pager2.file_header.schemas["users"]
    assert loaded_users.name == "users", "Users schema name mismatch"
    assert len(loaded_users.columns) == 3, "Users schema column count mismatch"
    assert loaded_users.columns[0].name == "id", "Users schema first column name mismatch"
    assert loaded_users.columns[0].is_primary_key == True, "Users schema primary key mismatch"

    loaded_products = pager2.file_header.schemas["products"]
    assert loaded_products.name == "products", "Products schema name mismatch"
    assert len(loaded_products.columns) == 3, "Products schema column count mismatch"
    assert loaded_products.columns[0].name == "product_id", "Products schema first column name mismatch"

    print("Users schema:", loaded_users)
    print("Products schema:", loaded_products)

    # Test StateManager integration
    state_manager = StateManager(test_db_file)

    # Verify schemas are loaded in StateManager
    assert "users" in state_manager.schemas, "Users schema not loaded in StateManager"
    assert "products" in state_manager.schemas, "Products schema not loaded in StateManager"

    # Verify page numbers are loaded in StateManager
    assert "users" in state_manager.table_pages, "Users page number not loaded in StateManager"
    assert "products" in state_manager.table_pages, "Products page number not loaded in StateManager"
    assert state_manager.table_pages["users"] == 0, "Users page number mismatch in StateManager"
    assert state_manager.table_pages["products"] == 1, "Products page number mismatch in StateManager"

    # Test adding a new schema through StateManager
    orders_schema = BasicSchema("orders", [
        Column("order_id", Integer(), True),
        Column("user_id", Integer(), False),
        Column("total", Integer(), False)
    ])

    state_manager.register_table("orders", orders_schema)

    # Verify the new schema was saved
    assert "orders" in state_manager.schemas, "Orders schema not added to StateManager"
    assert "orders" in state_manager.table_pages, "Orders page number not added to StateManager"
    # The orders table should get page number 0 since it's the first data page after headers
    assert state_manager.table_pages["orders"] == 0, "Orders page number should be 0"

    # Clean up
    pager2.close()
    os.remove(test_db_file)

    print("All schema header tests passed!")


def test_insert_functionality():
    """Test the insert functionality of StateManager"""
    test_db_file = "test_insert.db"
    if os.path.exists(test_db_file):
        os.remove(test_db_file)

    # Create StateManager
    state_manager = StateManager(test_db_file)

    # Create a test schema
    users_schema = BasicSchema("users", [
        Column("id", Integer(), True),
        Column("username", Text(), False),
        Column("email", Text(), False)
    ])

    # Register the table
    state_manager.register_table("users", users_schema)

    # Create test records
    record1 = Record(values={"id": 1, "username": "john_doe", "email": "john@example.com"}, schema=users_schema)
    record2 = Record(values={"id": 2, "username": "jane_smith", "email": "jane@example.com"}, schema=users_schema)

    # Insert records
    state_manager.insert("users", record1)
    state_manager.insert("users", record2)

    # Test inserting into non-existent table
    try:
        state_manager.insert("nonexistent", record1)
        assert False, "Should have raised ValueError for non-existent table"
    except ValueError as e:
        assert "not found" in str(e), f"Unexpected error message: {e}"

    # Verify the page number was assigned correctly
    assert state_manager.table_pages["users"] == 0, "Users table should be on page 0"

    # Clean up
    state_manager.pager.close()
    os.remove(test_db_file)

    print("All insert functionality tests passed!")


if __name__ == "__main__":
    test_schema_header()
    test_insert_functionality()