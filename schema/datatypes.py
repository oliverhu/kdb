from abc import ABCMeta
import sys
from typing import Any


class Datatype:
    __metaclass__ = ABCMeta
    typename = "NoType"
    is_fixed_size = False
    fixed_length = 0

    @staticmethod
    def serialize(value: Any) -> bytes:
        pass

    @staticmethod
    def deserialize(value: bytes) -> Any:
        pass

class Integer(Datatype):
    typename = "Integer"
    is_fixed_size = True
    fixed_length = 4

    @staticmethod
    def serialize(value: int) -> bytes:
        return value.to_bytes(4, sys.byteorder)

    @staticmethod
    def deserialize(value: bytes) -> Any:
        return int.from_bytes(value, sys.byteorder)

class Text(Datatype):
    typename = "Text"
    is_fixed_size = False

    @staticmethod
    def serialize(value: str) -> bytes:
        return value.encode("utf-8")

    @staticmethod
    def deserialize(value: bytes) -> Any:
        return value.decode("utf-8")

class Boolean(Datatype):
    typename = "Boolean"
    is_fixed_size = True
    fixed_length = 1

    @staticmethod
    def serialize(value: Any) -> bytes:
        return bytes([1 if value else 0])

    @staticmethod
    def deserialize(value: bytes) -> Any:
        return value[0] == 1