from cursor import Cursor
from btree import BTree
from pager import Pager
from record import Record, serialize, deserialize, deserialize_key
from schema.basic_schema import BasicSchema, Column
from schema.datatypes import Integer, Text


class CatalogTable:
    def __init__(self, pager: Pager, page_num: int = 0):
        self.schema = BasicSchema(
            "catalog",
            [
                Column("id", Integer(), True),
                Column("table_name", Text(), False),
                Column("root_page_num", Integer(), False),
                Column("schema_data", Text(), False),  # Store serialized schema
            ]
        )
        self.page_num = page_num
        self.tree = BTree(pager, page_num)
        self._next_id = 1

    def add_table(self, table_name: str, root_page_num: int, schema: BasicSchema = None):
        # Serialize schema if provided
        schema_data = ""
        if schema:
            schema_data = schema.serialize().hex()  # Convert bytes to hex string for storage
        # Create a record for the catalog entry
        record = Record(
            values={
                "id": self._next_id,
                "table_name": table_name,
                "root_page_num": root_page_num,
                "schema_data": schema_data
            },
            schema=self.schema
        )
        # Serialize and insert the record
        cell = serialize(record)
        self.tree.insert(cell)
        self._next_id += 1

    def get_table(self, table_name: str):
        # Use a fresh cursor to traverse all records
        cursor = Cursor(self.tree.pager, self.tree)
        cursor.navigate_to_first_leaf_node()
        while not cursor.end_of_table:
            try:
                cell = cursor.get_cell()
                record = deserialize(cell, self.schema)
                if record.values["table_name"] == table_name:
                    return record
                cursor.advance()
            except Exception:
                cursor.advance()
                continue
        return None

    def get_all_tables(self):
        """Get all table records from the catalog"""
        tables = []
        cursor = Cursor(self.tree.pager, self.tree)
        cursor.navigate_to_first_leaf_node()
        while not cursor.end_of_table:
            try:
                cell = cursor.get_cell()
                record = deserialize(cell, self.schema)
                tables.append(record)
                cursor.advance()
            except Exception:
                cursor.advance()
                continue
        return tables

    def get_schema(self, table_name: str):
        """Get the schema for a specific table"""
        record = self.get_table(table_name)
        if record and record.values.get("schema_data"):
            try:
                # Convert hex string back to bytes and deserialize
                schema_bytes = bytes.fromhex(record.values["schema_data"])
                schema, _ = BasicSchema.deserialize(schema_bytes)
                return schema
            except Exception:
                return None
        return None

    def get_root_page_num(self, table_name: str):
        """Get the root page number for a specific table"""
        record = self.get_table(table_name)
        if record:
            return record.values.get("root_page_num")
        return None