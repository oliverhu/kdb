#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from record import deserialize_key, serialize, deserialize, Record
from schema.basic_schema import BasicSchema, Column
from schema.datatypes import Integer, Text


def debug_serialization():
    """Debug function to understand the serialization format"""
    print("Debug: Understanding serialization format")
    print("="*50)

    schema = BasicSchema("users", [Column("id", Integer(), True), Column("name", Text(), False)])
    record = Record(values={"id": 42, "name": "John"}, schema=schema)
    serialized = serialize(record)

    print(f"Original record: {record.values}")
    print(f"Serialized bytes: {serialized}")
    print(f"Serialized length: {len(serialized)}")

    # Let's manually parse the serialized data
    ptr = 0

    # Read key_size (first 4 bytes)
    key_size = Integer.deserialize(serialized[ptr:ptr+4])
    ptr += 4
    print(f"Key size: {key_size}")

    # Read data_size (next 4 bytes)
    data_size = Integer.deserialize(serialized[ptr:ptr+4])
    ptr += 4
    print(f"Data size: {data_size}")

    # Read key (next key_size bytes)
    key = Integer.deserialize(serialized[ptr:ptr+key_size])
    ptr += key_size
    print(f"Key: {key}")

    print(f"Remaining bytes: {serialized[ptr:]}")
    print("="*50)


def test_deserialize_key():
    """Test the deserialize_key function with various key values"""

    # Debug first
    debug_serialization()

    # Test case 1: Simple integer key
    print("Test 1: Simple integer key")
    schema = BasicSchema("users", [Column("id", Integer(), True), Column("name", Text(), False)])
    record = Record(values={"id": 42, "name": "John"}, schema=schema)
    serialized = serialize(record)
    key = deserialize_key(serialized)
    assert key == 42, f"Expected key 42, got {key}"
    print(f"✓ Key correctly deserialized: {key}")

    # Test case 2: Zero key
    print("\nTest 2: Zero key")
    record = Record(values={"id": 0, "name": "Zero"}, schema=schema)
    serialized = serialize(record)
    key = deserialize_key(serialized)
    assert key == 0, f"Expected key 0, got {key}"
    print(f"✓ Key correctly deserialized: {key}")

    # Test case 3: Large integer key
    print("\nTest 3: Large integer key")
    record = Record(values={"id": 999999, "name": "Large"}, schema=schema)
    serialized = serialize(record)
    key = deserialize_key(serialized)
    assert key == 999999, f"Expected key 999999, got {key}"
    print(f"✓ Key correctly deserialized: {key}")

    # Test case 4: Negative integer key
    print("\nTest 4: Negative integer key")
    record = Record(values={"id": -123, "name": "Negative"}, schema=schema)
    serialized = serialize(record)
    key = deserialize_key(serialized)
    assert key == -123, f"Expected key -123, got {key}"
    print(f"✓ Key correctly deserialized: {key}")

    # Test case 5: Round trip test - serialize then deserialize
    print("\nTest 5: Round trip test")
    original_key = 12345
    record = Record(values={"id": original_key, "name": "RoundTrip"}, schema=schema)
    serialized = serialize(record)
    deserialized_key = deserialize_key(serialized)
    assert deserialized_key == original_key, f"Round trip failed: {original_key} != {deserialized_key}"
    print(f"✓ Round trip successful: {original_key} -> {deserialized_key}")


def test_record_serialization_round_trip():
    """Test complete record serialization and deserialization"""
    print("\n" + "="*50)
    print("Testing complete record serialization/deserialization")
    print("="*50)

    schema = BasicSchema("users", [
        Column("id", Integer(), True),
        Column("name", Text(), False),
        Column("age", Integer(), False)
    ])

    original_values = {"id": 100, "name": "Alice", "age": 25}
    record = Record(values=original_values, schema=schema)

    # Serialize
    serialized = serialize(record)
    print(f"Original record: {record.values}")
    print(f"Serialized size: {len(serialized)} bytes")

    # Deserialize
    deserialized_record = deserialize(serialized, schema)
    print(f"Deserialized record: {deserialized_record.values}")

    # Verify all values match
    for key, value in original_values.items():
        assert deserialized_record.values[key] == value, f"Mismatch for {key}: {value} != {deserialized_record.values[key]}"

    print("✓ Complete record round trip successful!")