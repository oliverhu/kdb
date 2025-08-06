#!/usr/bin/env python3
"""
KDB - A simple SQL database implementation with REPL interface
"""

from lark import Lark
from symbols import *

from grammar import GRAMMAR
from virtual_machine import VirtualMachine


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
        "select id, username, email from users where id = 2",
        "update users set email = 'jane.musk@gmail.com' where id = 2",
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
