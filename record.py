
from schema.basic_schema import BasicSchema, Column
from schema.datatypes import Integer, Text


class Record:
    """
    values -> {column_name: value}
    schema -> [column: name, datatype, is_primary_key]
    """
    def __init__(self, values: dict = None, schema: BasicSchema = None):
        self.values = values
        self.schema = schema

    def get(self, colume_name: str):
        return self.values[colume_name]

def serialize(record: Record) -> bytearray:

    serialized_value = bytearray()
    for column in record.schema.columns:
        value = record.values[column.name]
        datatype = column.datatype
        serialized_value += datatype.serialize(value)
        serialized_value += b"\x00"
        print("serialized_value", serialized_value)
    return serialized_value

def deserialize(serialized_value: bytearray, schema: BasicSchema) -> Record:
    values = {}
    column_values = serialized_value.split(b"\x00")
    for column_value, column in zip(column_values, schema.columns):
        datatype = column.datatype
        values[column.name] = datatype.deserialize(column_value)
    return Record(values, schema)


# quick test
schema = BasicSchema("users", [Column("id", Integer(), True), Column("name", Text(), False)])
record = Record(values={"id": 1, "name": "John"}, schema=schema)
serialized_value = serialize(record)
print(serialized_value)
deserialized_record = deserialize(serialized_value, schema)
print(deserialized_record.values)