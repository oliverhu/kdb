#!/usr/bin/env python3
"""
KDB - A simple SQL database implementation with REPL interface
"""

from dataclasses import dataclass
from enum import Enum, auto
from typing import List, Optional, Dict, Any
from collections import defaultdict
import os
from pager import Pager
from schema.basic_schema import BasicSchema
from symbols import *
from lark import Lark, Transformer, ast_utils

from grammar import GRAMMAR
from virtual_machine import VirtualMachine
from visitor import Visitor




def db_open(file_name):
    return Table(Pager(file_name=file_name))


def db_close(table):
    table.db_close()


class Table:
    def __init__(self, pager: Pager):
        self.pager = pager
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

    def execute(self, command):
        if command == "exit":
            self.db_close()
            return
        if command == "insert":
            self.db_insert("ok")
        elif command == "select":
            self.db_select(id)

class Frontend:
    def __init__(self):
        self.parser = Lark(GRAMMAR, parser="earley", start="program", debug=True)


def repl(db_file: str):
    frontend = Frontend()
    vm = VirtualMachine(db_file)
    commands = [
        "create table users (id integer primary key, username text, email text)",
        "insert into users (id, username, email) values (1, 'John Doe', 'john.doe@example.com')",
        "insert into users (id, username, email) values (2, 'Jane Musk', 'jane.musk@example.com')",
        "select username, email from users where id = 1",
    ]
    for command in commands:
        print(frontend.parser.parse(command).pretty())
        parse_tree = frontend.parser.parse(command)
        transformer = ToAst()
        tree = transformer.transform(parse_tree)
        vm.run(tree)
    # while True:
    #     command = input("> ")
    #     if command == "exit":
    #         break
    #     print(frontend.parser.parse(command).pretty())
    #     parse_tree = frontend.parser.parse(command)
    #     transformer = ToAst()
    #     tree = transformer.transform(parse_tree)
    #     vm.run(tree)


def main():
    repl("playground/pg.db")

if __name__ == "__main__":
    main()
