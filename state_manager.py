# Manage the state of the database

import os
from sqlite3 import Cursor
from pager import Pager
from schema.basic_schema import BasicSchema
from schema.basic_schema import Column, Integer, Text

class StateManager:
    """
    Manage the state of the database in memory.
    """
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.pager = Pager(file_path)
        # Load schemas from file header
        self.schemas = self.pager.file_header.schemas.copy()

    def register_table(self, table_name: str, schema: BasicSchema):
        self.schemas[table_name] = schema
        # Update file header
        self.pager.file_header.add_schema(table_name, schema)
        self.pager.file_header.write_schemas_header(self.pager.file_ptr)
        print(f"Registered schema for table {table_name}: {schema}")

    def get_table_schema(self, table_name: str):
        return self.schemas[table_name]

    def get_table_cursor(self, table_name: str):
        return Cursor(self.pager, self.schemas[table_name])

    def save_schemas(self):
        """Save all schemas to the file header"""
        self.pager.file_header.schemas = self.schemas.copy()
        self.pager.file_header.write_schemas_header(self.pager.file_ptr)



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

    # Add schemas to file header
    pager.file_header.add_schema("users", users_schema)
    pager.file_header.add_schema("products", products_schema)

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

    # Test adding a new schema through StateManager
    orders_schema = BasicSchema("orders", [
        Column("order_id", Integer(), True),
        Column("user_id", Integer(), False),
        Column("total", Integer(), False)
    ])

    state_manager.register_table("orders", orders_schema)

    # Verify the new schema was saved
    assert "orders" in state_manager.schemas, "Orders schema not added to StateManager"

    # Clean up
    pager2.close()
    os.remove(test_db_file)

    print("All schema header tests passed!")

if __name__ == "__main__":
    test_schema_header()