# Pager is a module that provides a pager for the database.
PAGE_SIZE = 4096
TABLE_MAX_PAGES = 100

class Row:
    def __init__(self, id, username, email):
        self.id = id
        self.username = username
        self.email = email

    def serialize(self):
        return f"{self.id},{self.username},{self.email}"

class Pager:
    def __init__(self, file_name):
        self.file_name = file_name
        self.file_handle = open(file_name, "wr")
        self.pages = []

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

