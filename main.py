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
        "select username, email from users",
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

        # table.execute(command)


class SymbolicDataType(Enum):
    Integer = auto()
    Text = auto()
    Real = auto()
    Blob = auto()
    Boolean = auto()


class ToAst(Transformer):
    def program(self, args):
        return Program(args)

    def select_stmt(self, args):
        select_clause = args[0]
        from_clause = args[1] if len(args) > 1 else None
        return SelectStmt(select_clause, from_clause)

    def select_clause(self, args):
        return SelectClause(args)

    def from_clause(self, args):
        fc = FromClause(args)
        fc.source = FromSource(fc.source)
        return fc

    def selectable(self, args):
        return Selectable(args[0])

    def expr(self, args):
        return Expr(args[0])

    def literal(self, args):
        val = args[0]
        if hasattr(val, 'value'):
            val = val.value

        # Convert string numbers to int
        if isinstance(val, str) and val.isdigit():
            return Literal(int(val))
        # Remove quotes from strings
        elif isinstance(val, str) and val.startswith("'") and val.endswith("'"):
            return Literal(val[1:-1])
        return Literal(val)

    def condition(self, args):
        return Condition(args)

    def or_clause(self, args):
        if len(args) == 1:
            return OrClause(args[0], None)
        else:
            return OrClause(args[0], args[1])

    def and_clause(self, args):
        if len(args) == 1:
            return AndClause(args[0], None)
        else:
            return AndClause(args[0], args[1])

    def not_clause(self, args):
        return NotClause(args)

    def comparison(self, args):
        if len(args) == 1:
            return Comparison(args[0], None, None)
        else:
            return Comparison(args[0], args[1], args[2])

    def predicate(self, args):
        return Predicate(args)

    def term(self, args):
        if len(args) == 1:
            return Term(args[0], None, None)
        else:
            return Term(args[0], args[1], args[2])

    def factor(self, args):
        if len(args) == 1:
            return Factor(args[0], None, None)
        else:
            return Factor(args[0], args[1], args[2])

    def unary_op(self, args):
        return UnaryOp(args)

    def binary_op(self, args):
        return BinaryOp(args[0], args[1], args[2])

    def primary(self, args):
        return Primary(args)

    def identifier(self, args):
        return Identifier(args[0])

    def column_name(self, args):
        return ColumnName(args[0])

    def where_clause(self, args):
        return WhereClause(args[0])

    def group_by_clause(self, args):
        return GroupByClause(args)

    def having_clause(self, args):
        return HavingClause(args[0])

    def order_by_clause(self, args):
        return OrderByClause(args)

    def limit_clause(self, args):
        if len(args) == 1:
            return LimitClause(args[0])
        else:
            return LimitClause(args[0], args[1])

    def source(self, args):
        return Source(args[0])

    def single_source(self, args):
        if len(args) == 1:
            return SingleSource(args[0])
        else:
            return SingleSource(args[0], args[1])

    def joining(self, args):
        return Joining(args[0])

    def conditioned_join(self, args):
        if len(args) == 4:
            return ConditionedJoin(args[0], args[1], args[3], None)
        else:
            return ConditionedJoin(args[0], args[2], args[4], args[1])

    def unconditioned_join(self, args):
        return UnconditionedJoin(args[0], args[1])

    def ordered_column(self, args):
        if len(args) == 1:
            return OrderedColumn(args[0])
        else:
            return OrderedColumn(args[0], args[1])

    def create_stmt(self, args):
        return CreateStmt(args[0], args[1])

    def column_def(self, args):
        primary_key = len(args) > 2 and args[2] == "primary_key"
        not_null = len(args) > 3 and args[3] == "not_null"
        # Extract the column name string from the ColumnName object
        column_name = args[0].name if hasattr(args[0], 'name') else str(args[0])
        return ColumnDef(column_name, args[1], primary_key, not_null)

    def drop_stmt(self, args):
        return DropStmt(args[0])

    def insert_stmt(self, args):
        return InsertStmt(args[0], args[1], args[2])

    def delete_stmt(self, args):
        if len(args) == 1:
            return DeleteStmt(args[0])
        else:
            return DeleteStmt(args[0], args[1])

    def update_stmt(self, args):
        if len(args) == 3:
            return UpdateStmt(args[0], args[1], args[2])
        else:
            return UpdateStmt(args[0], args[1], args[2], args[3])

    def truncate_stmt(self, args):
        return TruncateStmt(args[0])

    def func_call(self, args):
        return FuncCall(args[0], args[1])

    def func_arg_list(self, args):
        return args

    def nested(self, args):
        return Nested(args[0])

    def column_def_list(self, args):
        return args

    def column_name(self, args):
        assert len(args) == 1
        val = args[0]
        return ColumnName(val)

    def primary(self, args):
        assert len(args) == 1
        return args[0]

    def datatype(self, args):
        """
        Convert datatype text to arg
        """
        datatype = args[0].lower()
        if datatype == "integer":
            return SymbolicDataType.Integer
        elif datatype == "real":
            return SymbolicDataType.Real
        elif datatype == "text":
            return SymbolicDataType.Text
        elif datatype == "blob":
            return SymbolicDataType.Blob
        else:
            raise ValueError(f"Unrecognized datatype [{datatype}]")

    def table_name(self, args):
        return args[0]

    def table_alias(self, args):
        return args[0]

    def datatype(self, args):
        return args[0]

    def primary_key(self, args):
        return "primary_key"

    def not_null(self, args):
        return "not_null"

    def asc(self, args):
        return "asc"

    def desc(self, args):
        return "desc"

    def inner(self, args):
        return "inner"

    def left_outer(self, args):
        return "left_outer"

    def right_outer(self, args):
        return "right_outer"

    def full_outer(self, args):
        return "full_outer"

    def cross(self, args):
        return "cross"

    def column_name_list(self, args):
        return args

    def value_list(self, args):
        return args




def main():
    repl("playground/pg.db")

if __name__ == "__main__":
    main()
