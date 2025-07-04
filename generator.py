from abc import ABC, abstractmethod
from typing import Any
from record import Record


class Generator(ABC):
    @abstractmethod
    def get_value(self, record: Record) -> Any:
        pass

class ColumnNameGenerator(Generator):
    def __init__(self, column_name: str):
        self.column_name = column_name

    def get_value(self, record: Record) -> Any:
        return record.values[self.column_name]

class LiteralGenerator(Generator):
    def __init__(self, value: Any):
        self.value = value

    def get_value(self, record: Record) -> Any:
        return self.value