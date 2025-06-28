from typing import List
from schema.datatypes import Datatype


class Column:
    def __init__(self, name: str, datatype: Datatype, is_primary_key: bool = False):
        self.name = name
        self.datatype = datatype
        self.is_primary_key = is_primary_key


class BasicSchema:
    def __init__(self, name: str, columns: List[Column]):
        self.name = name
        self.columns = columns