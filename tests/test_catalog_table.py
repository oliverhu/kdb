#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from catalog.system_table import CatalogTable
from pager import Pager
from record import Record, serialize, deserialize
from schema.basic_schema import BasicSchema, Column
from schema.datatypes import Integer, Text


def test_catalog_table_initialization():
    """Test CatalogTable initialization"""
    print("Testing CatalogTable initialization...")

    # Create test database
    test_db_file = "test_catalog_init.db"
    if os.path.exists(test_db_file):
        os.remove(test_db_file)

    pager = Pager(test_db_file)
    catalog = CatalogTable(pager, page_num=0)

    # Verify schema
    assert catalog.schema.name == "catalog", "Schema table name should be 'catalog'"
    assert len(catalog.schema.columns) == 4, "Should have 4 columns"

    # Verify column names and types
    column_names = [col.name for col in catalog.schema.columns]
    assert "id" in column_names, "Should have 'id' column"
    assert "table_name" in column_names, "Should have 'table_name' column"
    assert "root_page_num" in column_names, "Should have 'root_page_num' column"

    # Verify page number
    assert catalog.page_num == 0, "Page number should be 0"

    # Clean up
    pager.close()
    os.remove(test_db_file)

    print("✓ CatalogTable initialization test passed!")


def test_add_table():
    """Test adding a table to the catalog"""
    print("Testing add_table functionality...")

    # Create test database
    test_db_file = "test_catalog_add.db"
    if os.path.exists(test_db_file):
        os.remove(test_db_file)

    pager = Pager(test_db_file)
    catalog = CatalogTable(pager, page_num=0)

    # Add a table
    table_name = "users"
    root_page_num = 1
    catalog.add_table(table_name, root_page_num)

    # Verify the table was added by checking the B-tree
    tree = catalog.tree
    page = pager.get_page(tree.root_page_num)

    # The page should now contain the catalog entry
    # We can verify this by checking if the page is not empty
    assert any(b != 0 for b in page), "Page should not be empty after adding table"

    # Clean up
    pager.close()
    os.remove(test_db_file)

    print("✓ Add table test passed!")


def test_get_table():
    """Test retrieving a table from the catalog"""
    print("Testing get_table functionality...")

    # Create test database
    test_db_file = "test_catalog_get.db"
    if os.path.exists(test_db_file):
        os.remove(test_db_file)

    pager = Pager(test_db_file)
    catalog = CatalogTable(pager, page_num=0)

    # Add multiple tables
    tables = [
        ("users", 1),
        ("posts", 2),
        ("comments", 3)
    ]

    for table_name, root_page_num in tables:
        catalog.add_table(table_name, root_page_num)

    # Debug: Print B-tree structure
    print(f"B-tree structure after inserting {len(tables)} tables:")
    print(catalog.tree)

    # Retrieve tables
    for table_name, expected_root_page_num in tables:
        result = catalog.get_table(table_name)
        assert result is not None, f"Should find table '{table_name}'"
        assert result.values["table_name"] == table_name, f"Table name should match '{table_name}'"
        assert result.values["root_page_num"] == expected_root_page_num, f"Root page number should be {expected_root_page_num}"

    # Test retrieving non-existent table
    result = catalog.get_table("non_existent")
    assert result is None, "Should return None for non-existent table"

    # Clean up
    pager.close()
    os.remove(test_db_file)

    print("✓ Get table test passed!")


def test_catalog_table_with_multiple_tables():
    """Test catalog with multiple tables and edge cases"""
    print("Testing catalog with multiple tables...")

    # Create test database
    test_db_file = "test_catalog_multiple.db"
    if os.path.exists(test_db_file):
        os.remove(test_db_file)

    pager = Pager(test_db_file)
    catalog = CatalogTable(pager, page_num=0)

    # Add many tables to test B-tree splits
    tables = []
    for i in range(10):
        table_name = f"table_{i}"
        root_page_num = i + 1
        tables.append((table_name, root_page_num))
        catalog.add_table(table_name, root_page_num)

    # Debug: Print B-tree structure
    print(f"B-tree structure after inserting {len(tables)} tables:")
    print(catalog.tree)

    # Verify all tables can be retrieved
    for table_name, expected_root_page_num in tables:
        result = catalog.get_table(table_name)
        assert result is not None, f"Should find table '{table_name}'"
        assert result.values["table_name"] == table_name, f"Table name should match '{table_name}'"
        assert result.values["root_page_num"] == expected_root_page_num, f"Root page number should be {expected_root_page_num}"

    # Test case sensitivity (should be case-sensitive)
    result = catalog.get_table("TABLE_0")
    assert result is None, "Should be case-sensitive"

    # Test empty string
    result = catalog.get_table("")
    assert result is None, "Should return None for empty string"

    # Clean up
    pager.close()
    os.remove(test_db_file)

    print("✓ Multiple tables test passed!")


def test_catalog_table_edge_cases():
    """Test edge cases for catalog table"""
    print("Testing catalog edge cases...")

    # Create test database
    test_db_file = "test_catalog_edge.db"
    if os.path.exists(test_db_file):
        os.remove(test_db_file)

    pager = Pager(test_db_file)
    catalog = CatalogTable(pager, page_num=0)

    # Test adding table with empty name
    try:
        catalog.add_table("", 1)
        # This might work depending on implementation, but let's verify it's handled
        result = catalog.get_table("")
        # If it was added, we should be able to retrieve it
        if result is not None:
            assert result.values["table_name"] == "", "Should handle empty table name"
    except Exception as e:
        # It's also acceptable for this to raise an exception
        print(f"Empty table name handling: {e}")

    # Test adding table with special characters
    special_table_name = "table_with_special_chars_!@#$%^&*()"
    catalog.add_table(special_table_name, 999)
    result = catalog.get_table(special_table_name)
    assert result is not None, "Should handle special characters in table name"
    assert result.values["table_name"] == special_table_name, "Should preserve special characters"

    # Test adding table with very long name
    long_table_name = "a" * 1000  # 1000 character table name
    catalog.add_table(long_table_name, 1000)
    result = catalog.get_table(long_table_name)
    assert result is not None, "Should handle very long table names"
    assert result.values["table_name"] == long_table_name, "Should preserve long table name"

    # Clean up
    pager.close()
    os.remove(test_db_file)

    print("✓ Edge cases test passed!")


def test_catalog_table_schema_consistency():
    """Test that catalog table schema is consistent"""
    print("Testing catalog schema consistency...")

    # Create test database
    test_db_file = "test_catalog_schema.db"
    if os.path.exists(test_db_file):
        os.remove(test_db_file)

    pager = Pager(test_db_file)
    catalog = CatalogTable(pager, page_num=0)

    # Verify schema structure
    schema = catalog.schema

    # Check column count
    assert len(schema.columns) == 4, "Should have exactly 4 columns"

    # Check column details
    id_col = next(col for col in schema.columns if col.name == "id")
    table_name_col = next(col for col in schema.columns if col.name == "table_name")
    root_page_col = next(col for col in schema.columns if col.name == "root_page_num")

    # Verify column types
    assert isinstance(id_col.datatype, Integer), "id column should be Integer type"
    assert isinstance(table_name_col.datatype, Text), "table_name column should be Text type"
    assert isinstance(root_page_col.datatype, Integer), "root_page_num column should be Integer type"

    # Verify primary key
    assert id_col.is_primary_key, "id column should be primary key"
    assert not table_name_col.is_primary_key, "table_name column should not be primary key"
    assert not root_page_col.is_primary_key, "root_page_num column should not be primary key"

    # Clean up
    pager.close()
    os.remove(test_db_file)

    print("✓ Schema consistency test passed!")


if __name__ == "__main__":
    print("Running CatalogTable tests...")
    print("="*50)

    try:
        test_catalog_table_initialization()
        test_add_table()
        test_get_table()
        test_catalog_table_with_multiple_tables()
        test_catalog_table_edge_cases()
        test_catalog_table_schema_consistency()
        print("\n" + "="*50)
        print("All CatalogTable tests passed! ✓")
        print("="*50)
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)