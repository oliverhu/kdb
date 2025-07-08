import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
from state_manager import StateManager
from schema.basic_schema import BasicSchema, Column
from schema.datatypes import Integer, Text
from record import Record

def test_catalog_based_system():
    """Test the new catalog-based system"""
    test_db_file = "test_catalog.db"
    if os.path.exists(test_db_file):
        os.remove(test_db_file)

    # Create StateManager (which uses CatalogTable internally)
    state_manager = StateManager(test_db_file)

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

    # Register tables through StateManager
    state_manager.register_table("users", users_schema)
    state_manager.register_table("products", products_schema)

    # Close and reopen to test persistence
    state_manager.pager.close()

    # Reopen and test loading
    state_manager2 = StateManager(test_db_file)

    # Verify schemas were loaded correctly
    assert "users" in state_manager2.schemas, "Users schema not found"
    assert "products" in state_manager2.schemas, "Products schema not found"

    # Verify page numbers were loaded correctly
    assert "users" in state_manager2.table_pages, "Users page number not found"
    assert "products" in state_manager2.table_pages, "Products page number not found"

    # Verify schema contents
    loaded_users = state_manager2.get_table_schema("users")
    assert loaded_users.name == "users", "Users schema name mismatch"
    assert len(loaded_users.columns) == 3, "Users schema column count mismatch"
    assert loaded_users.columns[0].name == "id", "Users schema first column name mismatch"
    assert loaded_users.columns[0].is_primary_key == True, "Users schema primary key mismatch"

    loaded_products = state_manager2.get_table_schema("products")
    assert loaded_products.name == "products", "Products schema name mismatch"
    assert len(loaded_products.columns) == 3, "Products schema column count mismatch"
    assert loaded_products.columns[0].name == "product_id", "Products schema first column name mismatch"

    # Test adding a new schema through StateManager
    orders_schema = BasicSchema("orders", [
        Column("order_id", Integer(), True),
        Column("user_id", Integer(), False),
        Column("total", Integer(), False)
    ])

    state_manager2.register_table("orders", orders_schema)

    # Verify the new schema was saved
    assert "orders" in state_manager2.schemas, "Orders schema not added to StateManager"
    assert "orders" in state_manager2.table_pages, "Orders page number not added to StateManager"

    # Test catalog table directly
    catalog = state_manager2.catalog
    orders_record = catalog.get_table("orders")
    assert orders_record is not None, "Orders table not found in catalog"
    assert orders_record.values["table_name"] == "orders", "Orders table name mismatch in catalog"

    # Clean up
    state_manager2.pager.close()
    os.remove(test_db_file)

def test_insert_functionality():
    """Test the insert functionality of StateManager with catalog"""
    test_db_file = "test_insert_catalog.db"
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
    with pytest.raises(ValueError):
        state_manager.insert("nonexistent", record1)

    # Verify the table was registered in catalog
    users_record = state_manager.catalog.get_table("users")
    assert users_record is not None, "Users table not found in catalog"
    assert users_record.values["table_name"] == "users", "Users table name mismatch in catalog"

    # Clean up
    state_manager.pager.close()
    os.remove(test_db_file)