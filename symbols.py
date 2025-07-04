from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, List, Optional
from lark import Transformer, ast_utils

from visitor import Visitor


class Symbol(ast_utils.Ast):
    def accept(self, visitor: Visitor):
        return visitor.visit(self)


@dataclass
class SelectClause(Symbol):
    selectables: List[Any]

@dataclass
class FromSource(Symbol):
    source: Any


@dataclass
class WhereClause(Symbol):
    condition: Any


@dataclass
class FromClause(Symbol):
    source: FromSource
    where_clause: WhereClause


class ArithmeticOp(Enum):
    ADD = auto()
    SUB = auto()
    MUL = auto()
    DIV = auto()


@dataclass
class BinaryOp(Symbol):
    operator: ArithmeticOp
    left: Any
    right: Any

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
class SingleSource(Symbol):
    table_name: str
    alias: Optional[str] = None


@dataclass
class Source(Symbol):
    single_source: List[SingleSource]


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
class ColumnDef(Symbol):
    column_name: str
    datatype: str
    primary_key: bool = False
    not_null: bool = False
@dataclass
class CreateStmt(Symbol):
    table_name: str
    column_defs: List[ColumnDef]

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
        fc = FromClause(args[0], args[1])
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