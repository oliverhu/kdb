# Manage the state of the database

from pager import Pager
from schema.basic_schema import BasicSchema


class StateManager:
    """
    Manage the state of the database in memory.
    """
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.pager = Pager(file_path)
        self.schemas = {}

    def register_table(self, table_name: str, schema: BasicSchema):
        self.schemas[table_name] = schema
        print(f"Registered schema for table {table_name}: {schema}")

    def get_table(self, table_name: str):
        return self.schemas[table_name]