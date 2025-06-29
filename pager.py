# Pager is a module that provides a pager for the database.
import os

PAGE_SIZE = 4096
TABLE_MAX_PAGES = 100


class Pager:
    def __init__(self, file_name):
        self.file_name = file_name
        self.file_ptr = open(file_name, "rb+")
        self.pages = [None] * TABLE_MAX_PAGES

        # initialize from the file
        self.file_length = os.path.getsize(file_name)
        self.num_pages = self.file_length // PAGE_SIZE

    def init_pages(self):
        for i in range(self.num_pages):
            self.get_page(i)

    def get_page(self, page_num):
        if self.pages[page_num] is None:
            if page_num < self.num_pages:
                self.file_ptr.seek(page_num * PAGE_SIZE)
                self.pages[page_num] = self.file_ptr.read(PAGE_SIZE)
            else:
                self.pages[page_num] = bytearray(PAGE_SIZE)
        return self.pages[page_num]

    def get_free_page(self):
        num_pages += 1
        return num_pages - 1

    def write_page(self, page_num, data):
        self.pages[page_num] = data
        self.flush_page(page_num)
        return self.pages[page_num]

    def flush_page(self, page_num):
        if self.pages[page_num] is None:
            print(f"Tried to flush page {page_num} but it is None")
            return
        self.file_ptr.seek(page_num * PAGE_SIZE)
        self.file_ptr.write(self.pages[page_num])
        self.file_ptr.flush() # write to disk

    def close(self):
        self.file_ptr.close()

class Table:
    def __init__(self, pager: Pager):
        self.pager = pager
        self.num_rows = 0

    def db_open(self, file_name):
        self.pager = Pager(file_name)
        self.num_rows = 0

    def db_close(self):
        self.pager.file_handle.close()

    def db_insert(self, row):
        self.pager.pages.append(row.serialize())
        self.num_rows += 1

    def db_select(self, id):
        for row in self.pager.pages:
            if row.id == id:
                return row
        return None
