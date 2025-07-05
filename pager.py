# Pager is a module that provides a pager for the database.
# A record is a Python object that is deserialized from a cell. A cell is a serialized record.
import os

from record import Record, serialize, deserialize
from schema.basic_schema import BasicSchema, Column
from schema.datatypes import Integer, Text

PAGE_SIZE = 4096
TABLE_MAX_PAGES = 100


class DatabaseFileHeader:
    def __init__(self, version: str, next_free_page: int, has_free_list: bool, schemas: dict = None, table_pages: dict = None):
        self.version = version
        self.next_free_page = next_free_page
        self.has_free_list = has_free_list
        self.schemas = schemas or {}  # table_name -> BasicSchema
        self.table_pages = table_pages or {}  # table_name -> page_number

    def from_header(header: bytes):
        version = header[:6].decode("utf-8")
        next_free_page = Integer.deserialize(header[6:10])
        has_free_list = bool(header[10])
        return DatabaseFileHeader(version, next_free_page, has_free_list)

    def to_header(self):
        return self.version.encode("utf-8") + Integer.serialize(self.next_free_page) + Integer.serialize(1 if self.has_free_list else 0)

    def write_schemas_header(self, file_ptr):
        """Write the 4K schemas header after the main file header"""
        # Create a 4K buffer
        schemas_buffer = bytearray(4096)

        # Write number of schemas
        num_schemas = Integer.serialize(len(self.schemas))
        schemas_buffer[:4] = num_schemas

        offset = 4
        for table_name, schema in self.schemas.items():
            # Write table name
            table_name_bytes = table_name.encode("utf-8")
            table_name_length = Integer.serialize(len(table_name_bytes))
            schemas_buffer[offset:offset+4] = table_name_length
            offset += 4
            schemas_buffer[offset:offset+len(table_name_bytes)] = table_name_bytes
            offset += len(table_name_bytes)

            # Write page number
            page_num = self.table_pages.get(table_name, 0)
            page_num_bytes = Integer.serialize(page_num)
            schemas_buffer[offset:offset+4] = page_num_bytes
            offset += 4

            # Write schema
            schema_bytes = schema.serialize()
            schema_length = Integer.serialize(len(schema_bytes))
            schemas_buffer[offset:offset+4] = schema_length
            offset += 4
            schemas_buffer[offset:offset+len(schema_bytes)] = schema_bytes
            offset += len(schema_bytes)

        # Write to file
        file_ptr.seek(100)  # After the main file header
        file_ptr.write(schemas_buffer)
        file_ptr.flush()

    def read_schemas_header(self, file_ptr):
        """Read the 4K schemas header and populate self.schemas"""
        file_ptr.seek(100)  # After the main file header
        schemas_buffer = file_ptr.read(4096)

        # Read number of schemas
        num_schemas = Integer.deserialize(schemas_buffer[:4])

        offset = 4
        for _ in range(num_schemas):
            # Read table name
            table_name_length = Integer.deserialize(schemas_buffer[offset:offset+4])
            offset += 4
            table_name = schemas_buffer[offset:offset+table_name_length].decode("utf-8")
            offset += table_name_length

            # Read page number
            page_num = Integer.deserialize(schemas_buffer[offset:offset+4])
            offset += 4
            self.table_pages[table_name] = page_num

            # Read schema
            schema_length = Integer.deserialize(schemas_buffer[offset:offset+4])
            offset += 4
            schema_data = schemas_buffer[offset:offset+schema_length]
            schema, _ = BasicSchema.deserialize(schema_data)
            offset += schema_length

            self.schemas[table_name] = schema

    def add_schema(self, table_name: str, schema: BasicSchema, page_num: int = None):
        """Add a schema to the header with optional page number"""
        self.schemas[table_name] = schema
        if page_num is not None:
            self.table_pages[table_name] = page_num

    def get_schema(self, table_name: str):
        """Get a schema by table name"""
        return self.schemas.get(table_name)


class PageHeader:
    def __init__(self, node_type: str, is_root: bool, parent_page_num: int, num_cells: int, allocation_pointer: int, cell_pointers: list[int]):
        self.node_type = node_type
        self.is_root = is_root
        self.num_cells = num_cells
        self.allocation_pointer = allocation_pointer
        self.parent_page_num = parent_page_num
        self.cell_pointers = cell_pointers

    def from_header(header: bytes):
        node_type = header[:1].decode("utf-8")
        is_root = bool(Integer.deserialize(header[1:5]))
        parent_page_num = Integer.deserialize(header[5:9])
        num_cells = Integer.deserialize(header[9:13])
        allocation_pointer = Integer.deserialize(header[13:17])
        cell_pointers = []
        for i in range(num_cells):
            cell_pointers.append(Integer.deserialize(header[17 + i * 4:21 + i * 4]))
        return PageHeader(node_type, is_root, parent_page_num, num_cells, allocation_pointer, cell_pointers)

    def to_header(self):
        return self.node_type.encode("utf-8") + Integer.serialize(1 if self.is_root else 0) + Integer.serialize(self.parent_page_num) + Integer.serialize(self.num_cells) + Integer.serialize(self.allocation_pointer) + b"".join(Integer.serialize(cell_pointer) for cell_pointer in self.cell_pointers)


class Pager:
    def __init__(self, file_name):
        self.file_name = file_name
        if not os.path.exists(file_name):
            self.file_ptr = open(file_name, "wb+")
            self.file_length = 0
            self.num_pages = 0
            self.init_file_header()
        else:
            self.file_ptr = open(file_name, "rb+")
            # initialize from the file
            self.file_length = os.path.getsize(file_name)
            # Calculate number of data pages (excluding header space)
            header_space = 100 + 4096  # file header + schemas header
            if self.file_length > header_space:
                self.num_pages = (self.file_length - header_space) // PAGE_SIZE
            else:
                self.num_pages = 0

        self.pages = [None] * TABLE_MAX_PAGES

        self.file_header = self.read_file_header()
        self.init_pages()

    def init_pages(self):
        for i in range(self.num_pages):
            self.get_page(i)

    @property
    def page_size(self):
        return PAGE_SIZE

    def get_page(self, page_num):
        if page_num >= TABLE_MAX_PAGES:
            return bytearray(self.page_size)
        if self.pages[page_num] is None:
            if page_num < self.num_pages:
                self.file_ptr.seek(4196 + page_num * PAGE_SIZE)  # 100 + 4096 for schemas header
                self.pages[page_num] = self.file_ptr.read(PAGE_SIZE)
            else:
                self.pages[page_num] = bytearray(PAGE_SIZE)
        return self.pages[page_num]

    def get_free_page(self):
        self.num_pages += 1
        return self.num_pages - 1

    def write_page(self, page_num, data):
        self.pages[page_num] = data
        self.flush_page(page_num)
        return self.pages[page_num]

    def flush_page(self, page_num):
        if self.pages[page_num] is None:
            print(f"Tried to flush page {page_num} but it is None")
            return
        self.file_ptr.seek(4196 + page_num * PAGE_SIZE)  # 100 + 4096 for schemas header
        self.file_ptr.write(self.pages[page_num])
        self.file_ptr.flush() # write to disk

    def close(self):
        self.file_ptr.close()

    def init_file_header(self):
        self.file_ptr.seek(0)
        file_header = DatabaseFileHeader(version="kdb000", next_free_page=self.num_pages, has_free_list=False)
        file_header_bytes = file_header.to_header()
        self.file_ptr.write(file_header_bytes)

        # Write empty schemas header
        file_header.write_schemas_header(self.file_ptr)
        self.file_ptr.flush()

    def read_file_header(self):
        self.file_ptr.seek(0)
        file_header_bytes = self.file_ptr.read(100)
        file_header = DatabaseFileHeader.from_header(file_header_bytes)

        # Read schemas header
        file_header.read_schemas_header(self.file_ptr)
        return file_header

    def set_free_page_header(self, page_num: int):
        self.file_ptr.seek(100)
        file_header = DatabaseFileHeader(version="kdb000", next_free_page=page_num, has_free_list=False)
        file_header = file_header.to_header()
        self.file_ptr.write(file_header)
        self.file_ptr.flush()

    def read_page(self, page_num):
        return self.get_page(page_num)

    def insert(self, record: Record, page_num: int):
        """
        Insert a record into the specified page.
        The record is serialized and added to the page, updating the page header accordingly.
        Returns a tuple of (position, length) where the record was stored.
        """
        # Get the current page
        page = bytearray(self.get_page(page_num))

        # Parse the page header
        header = PageHeader.from_header(page)

        # Serialize the record
        record_bytes = serialize(record)
        record_length = len(record_bytes)

        # Calculate position for the new record (from end of page)
        record_pos = self.page_size - record_length

        # If this is not the first record, adjust position to avoid overlap
        if header.num_cells > 0:
            # Find the minimum cell pointer to ensure we don't overlap
            min_cell_pointer = min(header.cell_pointers)
            record_pos = min_cell_pointer - record_length

        # Update the page header
        header.num_cells += 1
        header.cell_pointers.append(record_pos)
        header.allocation_pointer = record_pos

        # Write the updated header back to the page
        header_bytes = header.to_header()
        page[:len(header_bytes)] = header_bytes

        # Write the record data to the page
        page[record_pos:record_pos + record_length] = record_bytes

        # Write the updated page back to disk
        self.write_page(page_num, bytes(page))

        return (record_pos, record_length)

class Table:
    def __init__(self, pager: Pager):
        self.pager = pager
        self.num_rows = 0

    def db_open(self, file_name):
        self.pager = Pager(file_name)
        self.num_rows = 0

    def db_close(self):
        self.pager.file_ptr.close()

    def db_insert(self, row):
        self.pager.pages.append(row.serialize())
        self.num_rows += 1

    def db_select(self, id):
        for row in self.pager.pages:
            if row.id == id:
                return row
        return None

def test_page_header():
    # Test serialization/deserialization of page header
    original_header = PageHeader(
        node_type="L",  # Leaf node
        is_root=True,
        parent_page_num=0,
        num_cells=3,
        allocation_pointer=100,
        cell_pointers=[200, 300, 400]
    )
    serialized = original_header.to_header()
    deserialized = PageHeader.from_header(serialized)

    assert deserialized.node_type == original_header.node_type, "Node type mismatch"
    assert deserialized.is_root == original_header.is_root, "Is root mismatch"
    assert deserialized.parent_page_num == original_header.parent_page_num, "Parent page num mismatch"
    assert deserialized.num_cells == original_header.num_cells, "Num cells mismatch"
    assert deserialized.allocation_pointer == original_header.allocation_pointer, "Allocation pointer mismatch"
    assert deserialized.cell_pointers == original_header.cell_pointers, "Cell pointers mismatch"

    # Test with different values
    header2 = PageHeader(
        node_type="I",  # Internal node
        is_root=False,
        parent_page_num=5,
        num_cells=0,
        allocation_pointer=50,
        cell_pointers=[]
    )
    serialized2 = header2.to_header()
    deserialized2 = PageHeader.from_header(serialized2)

    assert deserialized2.node_type == header2.node_type
    assert deserialized2.is_root == header2.is_root
    assert deserialized2.parent_page_num == header2.parent_page_num
    assert deserialized2.num_cells == header2.num_cells
    assert deserialized2.allocation_pointer == header2.allocation_pointer
    assert deserialized2.cell_pointers == header2.cell_pointers

    print("All page header tests passed!")


def test_file_header():
    # Test serialization/deserialization of file header
    original_header = DatabaseFileHeader(version="kdb000", next_free_page=10, has_free_list=False)
    serialized = original_header.to_header()
    deserialized = DatabaseFileHeader.from_header(serialized)

    assert deserialized.version == original_header.version, "Version mismatch"
    assert deserialized.next_free_page == original_header.next_free_page, "Next free page mismatch"
    assert deserialized.has_free_list == original_header.has_free_list, "Has free list mismatch"

    # Test with different values
    header2 = DatabaseFileHeader(version="kdb000", next_free_page=0, has_free_list=True)
    serialized2 = header2.to_header()
    deserialized2 = DatabaseFileHeader.from_header(serialized2)

    assert deserialized2.version == header2.version
    assert deserialized2.next_free_page == header2.next_free_page
    assert deserialized2.has_free_list == header2.has_free_list

    print("All file header tests passed!")


def test_pager():
    # Test creating a new database file
    test_db_file = "test.db"
    if os.path.exists(test_db_file):
        os.remove(test_db_file)

    # Create new pager and verify file header
    pager = Pager(test_db_file)
    assert pager.file_header.version == "kdb000"
    assert pager.file_header.next_free_page == 0
    assert pager.file_header.has_free_list == False

    # Create a simple schema for testing
    schema = BasicSchema("test_table", [
        Column("id", Integer(), True),
        Column("data", Text(), False)
    ])

    # Create test records/cells
    record1 = Record(values={"id": 1, "data": "test data 1"}, schema=schema)
    record2 = Record(values={"id": 2, "data": "test data 2"}, schema=schema)

    # Insert records using the new insert function
    pos1, len1 = pager.insert(record1, 1)
    pos2, len2 = pager.insert(record2, 1)

    print(f"Inserted records at positions: {pos1} (length: {len1}), {pos2} (length: {len2})")

    # Read page and verify records can be read back
    read_page = pager.read_page(1)
    read_header = PageHeader.from_header(read_page)

    print(f"Page header: num_cells={read_header.num_cells}, cell_pointers={read_header.cell_pointers}")

    # Verify records can be read back
    read_record1 = deserialize(read_page[read_header.cell_pointers[0]:read_header.cell_pointers[0]+len1], schema)
    read_record2 = deserialize(read_page[read_header.cell_pointers[1]:read_header.cell_pointers[1]+len2], schema)

    assert read_record1.values["id"] == record1.values["id"]
    assert read_record1.values["data"] == record1.values["data"]
    assert read_record2.values["id"] == record2.values["id"]
    assert read_record2.values["data"] == record2.values["data"]

    print(f"Record 1 verified: {read_record1.values}")
    print(f"Record 2 verified: {read_record2.values}")

    # Test reading non-existent page returns empty page
    empty_page = pager.read_page(999)
    assert len(empty_page) == pager.page_size
    assert all(b == 0 for b in empty_page)

    # Clean up
    pager.close()
    os.remove(test_db_file)

    print("All pager tests passed!")


def test_insert():
    # Test the insert function
    test_db_file = "test_insert.db"
    if os.path.exists(test_db_file):
        os.remove(test_db_file)

    # Create new pager
    pager = Pager(test_db_file)

    # Create a simple schema for testing
    schema = BasicSchema("test_table", [
        Column("id", Integer(), True),
        Column("data", Text(), False)
    ])

    # Create test records
    record1 = Record(values={"id": 1, "data": "first record"}, schema=schema)
    record2 = Record(values={"id": 2, "data": "second record"}, schema=schema)
    record3 = Record(values={"id": 3, "data": "third record"}, schema=schema)

    # Insert records into page 1
    pos1, len1 = pager.insert(record1, 1)
    pos2, len2 = pager.insert(record2, 1)
    pos3, len3 = pager.insert(record3, 1)

    print(f"Inserted records at positions: {pos1} (length: {len1}), {pos2} (length: {len2}), {pos3} (length: {len3})")

    # Read the page back and verify
    page = pager.read_page(1)
    header = PageHeader.from_header(page)

    print(f"Page header: num_cells={header.num_cells}, cell_pointers={header.cell_pointers}")

    # Verify all records can be read back
    for i, (expected_record, expected_len) in enumerate([(record1, len1), (record2, len2), (record3, len3)]):
        cell_pos = header.cell_pointers[i]
        read_record = deserialize(page[cell_pos:cell_pos + expected_len], schema)

        assert read_record.values["id"] == expected_record.values["id"], f"Record {i} id mismatch"
        assert read_record.values["data"] == expected_record.values["data"], f"Record {i} data mismatch"
        print(f"Record {i} verified: {read_record.values}")

    # Clean up
    pager.close()
    os.remove(test_db_file)

    print("All insert tests passed!")



if __name__ == "__main__":
    test_file_header()
    test_page_header()
    test_pager()
    test_insert()

