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
        self.recycled_pages = []  # the pages that are not used (e.g. deleted entries)
        self.init_pages()

    def init_pages(self):
        for i in range(self.num_pages):
            self.get_page(i)

    @property
    def page_size(self):
        return PAGE_SIZE

    def get_page(self, page_num) -> bytearray:
        if page_num >= TABLE_MAX_PAGES:
            return bytearray(self.page_size)
        if self.pages[page_num] is None:
            if page_num < self.num_pages:
                self.file_ptr.seek(4196 + page_num * PAGE_SIZE)  # 100 + 4096 for schemas header
                self.pages[page_num] = bytearray(self.file_ptr.read(PAGE_SIZE))
            else:
                self.pages[page_num] = bytearray(PAGE_SIZE)
        return self.pages[page_num]

    def get_free_page(self):
        # Always allocate a new page to prevent page reuse and data corruption
        self.num_pages += 1
        return self.num_pages - 1

    def write_page(self, page_num, data):
        self.pages[page_num] = bytearray(data)
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


