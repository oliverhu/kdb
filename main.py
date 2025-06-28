#!/usr/bin/env python3
"""
KDB - A simple SQL database implementation with REPL interface
"""

from dataclasses import dataclass
from typing import List, Optional, Dict, Any
from collections import defaultdict
import os

from lark import Lark, Transformer, ast_utils

from grammar import GRAMMAR
from visitor import Visitor
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
    table = db_open(db_file)
    frontend = Frontend()
    vm = VirtualMachine(table)
    command = "select 1 from users"
    print(frontend.parser.parse(command).pretty())
    parse_tree = frontend.parser.parse(command)
    transformer = ToAst()
    tree = transformer.transform(parse_tree)
    print(tree)
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



class Symbol(ast_utils.Ast):
    def accept(self, visitor: Visitor):
        print(f"-> Accepting {self} to {visitor}")
        return visitor.visit(self)


@dataclass
class SelectClause(Symbol):
    selectables: List[Any]

@dataclass
class FromClause(Symbol):
    source: Any


@dataclass
class SelectStmt(Symbol):
    select_clause: SelectClause
    from_clause: FromClause

@dataclass
class Selectable(Symbol):
    value: Any

@dataclass
class Program(Symbol):
    statements: list

@dataclass
class Literal(Symbol):
    value: Any


@dataclass
class Expr(Symbol):
    value: Any

@dataclass
class Condition(Symbol):
    value: Any

@dataclass
class OrClause(Symbol):
    left: Any
    right: Any

@dataclass
class AndClause(Symbol):
    left: Any
    right: Any

@dataclass
class NotClause(Symbol):
    operand: Any

@dataclass
class Comparison(Symbol):
    left: Any
    operator: str
    right: Any

@dataclass
class Predicate(Symbol):
    value: Any

@dataclass
class Term(Symbol):
    left: Any
    operator: str
    right: Any

@dataclass
class Factor(Symbol):
    left: Any
    operator: str
    right: Any

@dataclass
class UnaryOp(Symbol):
    operator: str
    operand: Any

@dataclass
class BinaryOp(Symbol):
    operator: str
    left: Any
    right: Any

@dataclass
class Primary(Symbol):
    value: Any

@dataclass
class Identifier(Symbol):
    name: str

@dataclass
class ColumnName(Symbol):
    name: str

@dataclass
class WhereClause(Symbol):
    condition: Any

@dataclass
class GroupByClause(Symbol):
    columns: List[Any]

@dataclass
class HavingClause(Symbol):
    condition: Any

@dataclass
class OrderByClause(Symbol):
    columns: List[Any]

@dataclass
class LimitClause(Symbol):
    limit: Any
    offset: Optional[Any] = None

@dataclass
class Source(Symbol):
    value: Any

@dataclass
class SingleSource(Symbol):
    table_name: str
    alias: Optional[str] = None

@dataclass
class Joining(Symbol):
    value: Any

@dataclass
class ConditionedJoin(Symbol):
    source: Any
    single_source: Any
    condition: Any
    join_modifier: Optional[str] = None

@dataclass
class UnconditionedJoin(Symbol):
    source: Any
    single_source: Any

@dataclass
class OrderedColumn(Symbol):
    column: Any
    direction: Optional[str] = None

@dataclass
class CreateStmt(Symbol):
    table_name: str
    column_defs: List[Any]

@dataclass
class ColumnDef(Symbol):
    column_name: str
    datatype: str
    primary_key: bool = False
    not_null: bool = False

@dataclass
class DropStmt(Symbol):
    table_name: str

@dataclass
class InsertStmt(Symbol):
    table_name: str
    columns: List[str]
    values: List[Any]

@dataclass
class DeleteStmt(Symbol):
    table_name: str
    where_clause: Optional[Any] = None

@dataclass
class UpdateStmt(Symbol):
    table_name: str
    column: str
    value: Any
    where_clause: Optional[Any] = None

@dataclass
class TruncateStmt(Symbol):
    table_name: str

@dataclass
class FuncCall(Symbol):
    func_name: str
    args: List[Any]

@dataclass
class Nested(Symbol):
    value: Any

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
        return FromClause(args)

    def selectable(self, args):
        return Selectable(args[0])

    def expr(self, args):
        return Expr(args[0])

    def literal(self, args):
        return Literal(args)

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

    def create_stmnt(self, args):
        return CreateStmt(args[0], args[1])

    def column_def(self, args):
        primary_key = len(args) > 2 and args[2] == "primary_key"
        not_null = len(args) > 3 and args[3] == "not_null"
        return ColumnDef(args[0], args[1], primary_key, not_null)

    def drop_stmnt(self, args):
        return DropStmt(args[0])

    def insert_stmnt(self, args):
        return InsertStmt(args[0], args[1], args[2])

    def delete_stmnt(self, args):
        if len(args) == 1:
            return DeleteStmt(args[0])
        else:
            return DeleteStmt(args[0], args[1])

    def update_stmnt(self, args):
        if len(args) == 3:
            return UpdateStmt(args[0], args[1], args[2])
        else:
            return UpdateStmt(args[0], args[1], args[2], args[3])

    def truncate_stmnt(self, args):
        return TruncateStmt(args[0])

    def func_call(self, args):
        return FuncCall(args[0], args[1])

    def func_arg_list(self, args):
        return args

    def nested(self, args):
        return Nested(args[0])

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


class VirtualMachine(Visitor):
    def __init__(self, table: Table):
        self.table = table
        self.stack = []
        self.heap = {}
        self.pc = 0
        self.running = True

    def run(self,program):
        self.execute(program)

    def execute(self, stmt: Symbol):
        stmt.accept(self)

    def visit_select_stmt(self, stmt: SelectStmt):
        pass

    def visit_from_clause(self, stmt: FromClause):
        pass

    def visit_select_clause(self, stmt: SelectClause):
        pass

    def visit_selectable(self, stmt: Selectable):
        pass

    def visit_program(self, stmt: Program):
        print(f"-> Visiting program {stmt}")
        print(stmt.statements)
        for stmt in stmt.statements:
            self.execute(stmt)

    def visit_literal(self, stmt: Literal):
        pass

    def visit_expr(self, stmt: Expr):
        pass

    def visit_condition(self, stmt: Condition):
        pass

    def visit_or_clause(self, stmt: OrClause):
        pass

    def visit_and_clause(self, stmt: AndClause):
        pass

    def visit_not_clause(self, stmt: NotClause):
        pass

    def visit_comparison(self, stmt: Comparison):
        pass

    def visit_predicate(self, stmt: Predicate):
        pass

    def visit_term(self, stmt: Term):
        pass

    def visit_factor(self, stmt: Factor):
        pass

    def visit_unary_op(self, stmt: UnaryOp):
        pass

    def visit_binary_op(self, stmt: BinaryOp):
        pass

    def visit_primary(self, stmt: Primary):
        pass

    def visit_identifier(self, stmt: Identifier):
        pass

    def visit_column_name(self, stmt: ColumnName):
        pass

    def visit_where_clause(self, stmt: WhereClause):
        pass

    def visit_group_by_clause(self, stmt: GroupByClause):
        pass

    def visit_having_clause(self, stmt: HavingClause):
        pass

    def visit_order_by_clause(self, stmt: OrderByClause):
        pass

    def visit_limit_clause(self, stmt: LimitClause):
        pass

    def visit_source(self, stmt: Source):
        pass

    def visit_single_source(self, stmt: SingleSource):
        pass

    def visit_joining(self, stmt: Joining):
        pass

    def visit_conditioned_join(self, stmt: ConditionedJoin):
        pass

    def visit_unconditioned_join(self, stmt: UnconditionedJoin):
        pass

    def visit_ordered_column(self, stmt: OrderedColumn):
        pass

    def visit_create_stmnt(self, stmt: CreateStmt):
        pass

    def visit_column_def(self, stmt: ColumnDef):
        pass

    def visit_drop_stmnt(self, stmt: DropStmt):
        pass

    def visit_insert_stmnt(self, stmt: InsertStmt):
        pass

    def visit_delete_stmnt(self, stmt: DeleteStmt):
        pass

    def visit_update_stmnt(self, stmt: UpdateStmt):
        pass

    def visit_truncate_stmnt(self, stmt: TruncateStmt):
        pass

    def visit_func_call(self, stmt: FuncCall):
        pass

    def visit_nested(self, stmt: Nested):
        pass


def main():
    # Run some tests
    pager = Pager("playground/pg.db")
    pager.init_pages()
    page1 = pager.get_page(1)
    page0 = pager.get_page(0)
    print(page1.rstrip(b'\x00'))
    assert page0.rstrip(b'\x00') == b"Hello!"
    assert page1.rstrip(b'\x00') == b"World!"
    # print(Frontend().parser.parse("select 1").pretty())
    repl("playground/pg.db")

if __name__ == "__main__":
    main()
