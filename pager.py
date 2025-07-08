# Pager is a module that provides a pager for the database.
# A record is a Python object that is deserialized from a cell. A cell is a serialized record.
import os


from record import Record, serialize, deserialize
from schema.basic_schema import BasicSchema, Column
from schema.datatypes import Integer, Text

PAGE_SIZE = 4096
TABLE_MAX_PAGES = 100


class DatabaseFileHeader:
    def __init__(self, version: str, next_free_page: int, has_free_list: bool):
        self.version = version
        self.next_free_page = next_free_page
        self.has_free_list = has_free_list

    def from_header(header: bytes):
        version = header[:6].decode("utf-8")
        next_free_page = Integer.deserialize(header[6:10])
        has_free_list = bool(header[10])
        return DatabaseFileHeader(version, next_free_page, has_free_list)

    def to_header(self):
        return self.version.encode("utf-8") + Integer.serialize(self.next_free_page) + Integer.serialize(1 if self.has_free_list else 0)


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
            header_space = 100  # file header only
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
                self.file_ptr.seek(100 + page_num * PAGE_SIZE)  # 100 for file header
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
        self.file_ptr.seek(100 + page_num * PAGE_SIZE)  # 100 for file header
        self.file_ptr.write(self.pages[page_num])
        self.file_ptr.flush() # write to disk

    def close(self):
        self.file_ptr.close()

    def init_file_header(self):
        self.file_ptr.seek(0)
        file_header = DatabaseFileHeader(version="kdb000", next_free_page=self.num_pages, has_free_list=False)
        file_header_bytes = file_header.to_header()
        self.file_ptr.write(file_header_bytes)

    def read_file_header(self):
        self.file_ptr.seek(0)
        file_header_bytes = self.file_ptr.read(100)
        file_header = DatabaseFileHeader.from_header(file_header_bytes)
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


