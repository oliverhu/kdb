from typing import List
from schema.datatypes import Datatype, Integer, Text


class Column:
    def __init__(self, name: str, datatype: Datatype, is_primary_key: bool = False):
        self.name = name
        self.datatype = datatype
        self.is_primary_key = is_primary_key

    def __str__(self):
        return f"Column[{self.name}, {self.datatype}, is_primary: {self.is_primary_key}]"

    def serialize(self):
        """Serialize a Column to bytes"""
        name_bytes = self.name.encode("utf-8")
        name_length = Integer.serialize(len(name_bytes))
        datatype_name = self.datatype.__class__.__name__.encode("utf-8")
        datatype_length = Integer.serialize(len(datatype_name))
        is_primary = Integer.serialize(1 if self.is_primary_key else 0)

        return name_length + name_bytes + datatype_length + datatype_name + is_primary

    @classmethod
    def deserialize(cls, data: bytes, offset: int = 0):
        """Deserialize a Column from bytes, returns (Column, new_offset)"""
        # Read name
        name_length = Integer.deserialize(data[offset:offset+4])
        offset += 4
        name = data[offset:offset+name_length].decode("utf-8")
        offset += name_length

        # Read datatype
        datatype_length = Integer.deserialize(data[offset:offset+4])
        offset += 4
        datatype_name = data[offset:offset+datatype_length].decode("utf-8")
        offset += datatype_length

        # Create datatype instance
        if datatype_name == "Integer":
            datatype = Integer()
        elif datatype_name == "Text":
            datatype = Text()
        else:
            raise ValueError(f"Unknown datatype: {datatype_name}")

        # Read is_primary_key
        is_primary_key = bool(Integer.deserialize(data[offset:offset+4]))
        offset += 4

        return cls(name, datatype, is_primary_key), offset


class BasicSchema:
    def __init__(self, name: str, columns: List[Column]):
        self.name = name
        self.columns = columns

    def get_primary_key(self):
        for column in self.columns:
            if column.is_primary_key:
                return column
        return None

    def __str__(self):
        body = " ".join([str(col) for col in self.columns])
        return f"Schema({str(self.name)}, {str(body)})"

    def serialize(self):
        """Serialize a BasicSchema to bytes"""
        name_bytes = self.name.encode("utf-8")
        name_length = Integer.serialize(len(name_bytes))
        num_columns = Integer.serialize(len(self.columns))

        # Serialize each column
        columns_data = b""
        for column in self.columns:
            columns_data += column.serialize()

        return name_length + name_bytes + num_columns + columns_data

    @classmethod
    def deserialize(cls, data: bytes, offset: int = 0):
        """Deserialize a BasicSchema from bytes, returns (BasicSchema, new_offset)"""
        # Read name
        name_length = Integer.deserialize(data[offset:offset+4])
        offset += 4
        name = data[offset:offset+name_length].decode("utf-8")
        offset += name_length

        # Read number of columns
        num_columns = Integer.deserialize(data[offset:offset+4])
        offset += 4

        # Read each column
        columns = []
        for _ in range(num_columns):
            column, offset = Column.deserialize(data, offset)
            columns.append(column)

        return cls(name, columns), offset
