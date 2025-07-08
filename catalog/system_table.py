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
            ]
        )
        self.page_num = page_num
        self.tree = BTree(pager, page_num)
        self.cursor = Cursor(pager, self.tree)
        self._next_id = 1

    def add_table(self, table_name: str, root_page_num: int):
        print(f"[DEBUG] CatalogTable.add_table: id={self._next_id}, table_name={table_name}, root_page_num={root_page_num}")
        # Create a record for the catalog entry
        record = Record(
            values={
                "id": self._next_id,
                "table_name": table_name,
                "root_page_num": root_page_num
            },
            schema=self.schema
        )

        # Serialize and insert the record
        cell = serialize(record)
        print(f"[DEBUG] Serialized cell: {cell}")
        print(f"[DEBUG] Deserialized key from cell: {deserialize_key(cell)}")
        self.tree.insert(cell)
        self._next_id += 1

    def get_table(self, table_name: str):
        # Use cursor to traverse all records
        self.cursor.navigate_to_first_leaf_node()

        while not self.cursor.end_of_table:
            try:
                cell = self.cursor.get_cell()
                record = deserialize(cell, self.schema)
                print(f"[DEBUG] get_table: visited record id={record.values['id']}, table_name={record.values['table_name']}")

                if record.values["table_name"] == table_name:
                    print(f"[DEBUG] get_table: found match for table_name={table_name}")
                    return record

                self.cursor.advance()
            except Exception as e:
                print(f"Error deserializing catalog record: {e}")
                self.cursor.advance()
                continue

        print(f"[DEBUG] get_table: did not find table_name={table_name}")
        return None