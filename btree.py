class BTree:
    def __init__(self, page_size: int):
        self.page_size = page_size
        self.root_page_num = 0
        self.root_page = None

    def create(self, file_name: str):
        pass

    def open(self, file_name: str):
        pass

    def insert(self, key: int, value: bytes):
        pass

    def delete(self, key: int):
        pass

    def search(self, key: int):
        pass


def cell_from_page(page: bytes, cell_num: int):
    pass