from typing import List
from schema.datatypes import Datatype


class Column:
    def __init__(self, name: str, datatype: Datatype, is_primary_key: bool = False):
        self.name = name
        self.datatype = datatype
        self.is_primary_key = is_primary_key

    def __str__(self):
        return f"Column[{self.name}, {self.datatype}, is_primary: {self.is_primary_key}]"

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
