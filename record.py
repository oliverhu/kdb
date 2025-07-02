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

    def to_bytes(self) -> bytearray:
        return serialize(self)

    @staticmethod
    def from_bytes(bytes: bytearray, schema: BasicSchema) -> "Record":
        return deserialize(bytes, schema)

    def __str__(self):
        return f"Record(values={self.values}, schema={self.schema})"


"""
Binary format of the record:
1. key -> integer
2. data -> {column_name: value}
    2.1 data_header -> size of each column
    2.2 data_body -> actual data
"""
def serialize(record: Record) -> bytearray:
    key = b""
    data = b""
    data_header = b""
    for column in record.schema.columns:
        if column.is_primary_key:
            key = column.datatype.serialize(record.values[column.name])
        value = record.values[column.name]
        datatype = column.datatype
        v_binary = datatype.serialize(value)
        data_header += Integer.serialize(len(v_binary))
        data += v_binary

    key_size = Integer.serialize(len(key))
    data_size = Integer.serialize(len(data_header) + len(data))
    # print("serialized", key_size, data_size, key, data_header, data)
    return key_size + data_size + key + data_header + data

def deserialize(serialized_value: bytearray, schema: BasicSchema) -> Record:
    # print("deserializing", schema)
    values = {}
    ptr = 0
    key_size = Integer.deserialize(serialized_value[ptr:4])
    ptr += 4
    data_size = Integer.deserialize(serialized_value[ptr:ptr + 4])
    ptr += 4
    key = Integer.deserialize(serialized_value[ptr:ptr + key_size])
    ptr += key_size
    data_header = serialized_value[ptr:ptr + Integer.fixed_length * len(schema.columns)]
    ptr += Integer.fixed_length * len(schema.columns)
    data = serialized_value[ptr:ptr + data_size]
    ptr = 0
    for i, column in enumerate(schema.columns):
        offset = data_header[i * Integer.fixed_length:i * Integer.fixed_length + Integer.fixed_length]
        size = Integer.deserialize(offset)
        key_name = column.name.name if hasattr(column.name, 'name') else str(column.name)
        values[key_name] = column.datatype.deserialize(data[ptr:ptr + size])
        ptr += size
    # print("deserialized", values)
    return Record(values, schema)


if __name__ == "__main__":
    schema = BasicSchema("users", [Column("id", Integer(), True), Column("name", Text(), False)])
    record = Record(values={"id": 3, "name": "John"}, schema=schema)
    # print(record)
    serialized = serialize(record)
    # print("serialized", serialized)
    deserialized = deserialize(serialized, schema)
    # print("deserialized", deserialized.values)
    print(deserialized.values)